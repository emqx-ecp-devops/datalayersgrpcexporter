package otel2datalayers

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type MetricsSchema uint8

const (
	_ MetricsSchema = iota
	MetricsSchemaTelegrafPrometheusV1
	MetricsSchemaTelegrafPrometheusV2
	MetricsSchemaOtelV1
)

func (ms MetricsSchema) String() string {
	switch ms {
	case MetricsSchemaTelegrafPrometheusV1:
		return "telegraf-prometheus-v1"
	case MetricsSchemaTelegrafPrometheusV2:
		return "telegraf-prometheus-v2"
	case MetricsSchemaOtelV1:
		return "otel-v1"
	default:
		panic("invalid MetricsSchema")
	}
}

var MetricsSchemata = map[string]MetricsSchema{
	MetricsSchemaTelegrafPrometheusV1.String(): MetricsSchemaTelegrafPrometheusV1,
	MetricsSchemaTelegrafPrometheusV2.String(): MetricsSchemaTelegrafPrometheusV2,
	MetricsSchemaOtelV1.String():               MetricsSchemaOtelV1,
}

// use to store metrics data for temporary
type MetricsMultipleLines struct {
	Lines      []MetricsSingleLine
	Attributes map[string]string
}
type MetricsSingleLine struct {
	Key   string
	Value interface{}
	Type  int32
}

func WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	// TODO: add metrics processing, Splice SQL and store it in datalayers.

	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		newLines := MetricsMultipleLines{
			Lines:      []MetricsSingleLine{},
			Attributes: map[string]string{},
		}

		deduplicateMap := map[string]any{}

		rm := md.ResourceMetrics().At(i)

		attrs := rm.Resource().Attributes()
		attrs.Range(func(k string, v pcommon.Value) bool {
			if len(v.AsString()) > 0 {
				newLines.Attributes[k] = v.AsString()
			}
			return true
		})

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)

				if _, ok := deduplicateMap[m.Name()]; ok {
					continue
				} else {
					deduplicateMap[m.Name()] = nil
				}

				metricsSingleLine := MetricsSingleLine{
					Key:  m.Name(),
					Type: int32(m.Type()),
				}

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					metricsSingleLine.Value = m.Gauge().DataPoints().At(0).DoubleValue()
				case pmetric.MetricTypeSum:
					metricsSingleLine.Value = m.Sum().DataPoints().At(0).DoubleValue()
				case pmetric.MetricTypeHistogram:
					metricsSingleLine.Value = m.Histogram().DataPoints().At(0).Sum()
				case pmetric.MetricTypeSummary:
					metricsSingleLine.Value = m.Summary().DataPoints().At(0).Sum()
				case pmetric.MetricTypeExponentialHistogram:
					metricsSingleLine.Value = m.ExponentialHistogram().DataPoints().At(0).Sum()
				default:
					metricsSingleLine.Value = nil
				}

				newLines.Lines = append(newLines.Lines, metricsSingleLine)
			}
		}
		enqueueNewlines(newLines)
	}

	return nil
}

var metricQueue = make(chan MetricsMultipleLines, 1000)

func enqueueNewlines(metrics MetricsMultipleLines) {
	metricQueue <- metrics
}

func (w *DatalayerWritter) ProcessMetrics(ctx context.Context) {
	for {
		select {
		case metrics := <-metricQueue:
			fmt.Println("Received metrics: ", metrics)
			w.concatenateSql(metrics)
		case <-ctx.Done():
			return
		}
	}
}

func (w *DatalayerWritter) concatenateSql(metrics MetricsMultipleLines) {
	sql := "INSERT INTO %s (%s) VALUES (%s);"
	tableName := w.table

	columns := ""
	_ = fmt.Sprintf("Recieve new metrics: %v\n", metrics)

	values := ""

	for k, v := range metrics.Attributes {
		if k != "service.instance.id" {
			CompareObject.AddColumnsMap(k, 0)
			columns += fmt.Sprintf("'%s_0',", k)
		} else {
			columns += fmt.Sprintf("'%s',", "instance_id")
		}

		values += fmt.Sprintf("'%s',", v)
	}

	for _, metric := range metrics.Lines {
		CompareObject.AddColumnsMap(metric.Key, metric.Type)
		// 用 service.instance.id 做主键 ， 实际为 Job name 中 resource_type/instance/cluster_name~${host} 的 rcluster_name~${host}
		if metric.Key == "instance_id" {
			columns += fmt.Sprintf("'%s',", "instance_id")
		} else {
			columns += fmt.Sprintf("'%s_%d',", metric.Key, metric.Type)
		}

		// 用 service.name 字段分表， 实际为 Job name 中 resource_type/instance/cluster_name~${host} 的 resource_type
		if metric.Key == "service.name" {
			tableName = fmt.Sprintf("%s,", metric.Value)
		}

		if metric.Type <= int32(pmetric.MetricTypeSummary) {
			temp := ""
			if v, ok := metric.Value.(float64); ok {
				temp = strconv.FormatFloat(v, 'f', -1, 64)
			} else {
				temp = fmt.Sprintf("%d", metric.Value)
			}

			values += fmt.Sprintf("%s,", temp)
		} else {
			values += fmt.Sprintf("'%s',", metric.Value)
		}

	}
	if err := w.AlterTableWithColumnsMap(); err != nil {
		fmt.Println("Failed to update table: ", err)
		return
	}
	CompareObject.SwapColumnsMap()

	columns = strings.TrimSuffix(columns, ",")
	values = strings.TrimSuffix(values, ",")

	table := w.db + "." + tableName
	err := w.CheckTable(tableName)
	if err != nil {
		fmt.Printf("\nFailed to check table: %s\nsql: %s\n\n", err.Error(), sql)
		return
	}

	sql = fmt.Sprintf(sql, table, columns, values)
	fmt.Println("to execute the sql: ", sql)

	_, err = w.client.Execute(sql) // todo: maybe need to set the instance_name field
	if err != nil {
		fmt.Printf("\nFailed to insert metrics: %s\nsql: %s\n\n", err.Error(), sql)
		return
	}

}
