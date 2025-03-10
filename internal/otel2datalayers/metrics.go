package otel2datalayers

import (
	"context"
	"fmt"
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
	Key      string
	Value    interface{}
	Type     int32
	Metadata map[string]string
}

func WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
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
					Key:      m.Name(),
					Type:     int32(m.Type()),
					Metadata: map[string]string{},
				}

				m.Metadata().Range(func(k string, v pcommon.Value) bool {
					metricsSingleLine.Metadata[k] = v.AsString()
					return true
				})

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

func addquote(v string) string {
	return fmt.Sprintf("`%s`", v)
}

func addSingleQuote(v string) string {
	return fmt.Sprintf("'%s'", v)
}

func (w *DatalayerWritter) concatenateSql(metrics MetricsMultipleLines) {
	dbName := ""
	partitions := []string{}
	fields := []string{}

	partitionFieldValues := []string{}
	for k, v := range metrics.Attributes {
		// 用 service.name 字段分表， 实际为 Job name 中 resource_type/instance/cluster_name~${host} 的 resource_type
		if k == "service.name" {
			dbName = "metrics_" + v
		}
		partitions = append(partitions, addquote(k))
		partitionFieldValues = append(partitionFieldValues, addSingleQuote(v))
	}
	if dbName == "" {
		return // todo: 处理没有 service.name 的情况
	}

	for _, metric := range metrics.Lines {
		tableName := metric.Key
		valueType := metric.Type
		value := metric.Value
		meta := metric.Metadata

		metaValues := []string{}
		fields = []string{}
		for k, v := range meta {
			fields = append(fields, addquote(k))
			metaValues = append(metaValues, addSingleQuote(v))
		}

		err := w.CheckDBAndTable(dbName, addquote(tableName), partitions, fields, valueType)
		if err != nil {
			fmt.Printf("\nFailed to check table: %s", err.Error())
			return
		}

		sql := "INSERT INTO %s.`%s` (%s) VALUES (%s)"
		columns := strings.Join(append(partitions, fields...), ",")
		values := strings.Join(append(partitionFieldValues, metaValues...), ",")

		columns = columns + ", val"
		values = values + ", " + fmt.Sprintf("%v", value)

		sql = fmt.Sprintf(sql, dbName, tableName, columns, values)
		fmt.Println("to execute the sql: ", sql)

		_, err = w.client.Execute(sql) // todo: maybe need to set the instance_name field
		if err != nil {
			fmt.Printf("\nFailed to insert metrics: %s\nsql: %s\n\n", err.Error(), sql)
			return
		}

	}
}
