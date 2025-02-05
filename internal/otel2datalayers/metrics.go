package otel2datalayers

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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
type MetricsMultipleLines []MetricsSingleLine
type MetricsSingleLine struct {
	Key      string
	Value    interface{}
	Type     int32
	Metadata map[string]any
}

func WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	// TODO: add metrics processing, Splice SQL and store it in datalayers.
	newLines := MetricsMultipleLines{}
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ilm := rm.ScopeMetrics().At(j)
			for k := 0; k < ilm.Metrics().Len(); k++ {
				m := ilm.Metrics().At(k)

				metricsSingleLine := MetricsSingleLine{
					Key:      m.Name(),
					Type:     int32(m.Type()),
					Metadata: m.Metadata().AsRaw(),
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

				newLines = append(newLines, metricsSingleLine)
			}
		}
	}

	enqueueNewlines(newLines)
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

	table := w.db + "." + w.table
	columns := ""
	_ = fmt.Sprintf("Recieve new metrics: %v\n", metrics)

	values := ""

	for _, metric := range metrics {
		CompareObject.AddColumnsMap(metric.Key, metric.Type)
		columns += fmt.Sprintf("`%s_%d`,", metric.Key, metric.Type)
		if metric.Type <= int32(pmetric.MetricTypeSummary) {
			temp := ""
			if v, ok := metric.Value.(float64); ok {
				temp = strconv.FormatFloat(v, 'f', -1, 64)
			} else {
				temp = fmt.Sprintf("%d", metric.Value)
			}

			values += fmt.Sprintf("%s,", temp)
		} else {
			values += fmt.Sprintf("%s,", metric.Value)
		}

	}
	if err := w.AlterTableWithColumnsMap(); err != nil {
		fmt.Println("Failed to update table: ", err)
		return
	}
	CompareObject.SwapColumnsMap()

	if !strings.Contains(columns, "instantce_name") {
		columns += "`instantce_name`,"
		values += "`default`,"
	}

	columns = strings.TrimSuffix(columns, ",")
	values = strings.TrimSuffix(values, ",")

	sql = fmt.Sprintf(sql, table, columns, values)
	fmt.Println("to execute the sql: ", sql)

	_, err := w.client.Execute(sql) // todo: maybe need to set the instance_name field
	if err != nil {
		fmt.Println("Failed to insert metrics: ", err)
		return
	}

}
