package otel2datalayers

import (
	"context"
	"fmt"

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

func ProcessMetrics() {
	for {
		select {
		case metrics := <-metricQueue:
			// TODO: to write metrics to datalayers.
			fmt.Println("Received metrics: ", metrics)
		}
	}
}
