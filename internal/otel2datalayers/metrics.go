package otel2datalayers

import (
	"context"

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

type OtelMetricsToLineProtocol struct {
	iw InfluxWriter
	mw metricWriter
}

type metricWriter interface {
	enqueueMetric(ctx context.Context, resource pcommon.Resource, instrumentationScope pcommon.InstrumentationScope, metric pmetric.Metric, batch InfluxWriterBatch) error
}

func WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	// TODO: add metrics processing
	return nil
}

type basicDataPoint interface {
	Timestamp() pcommon.Timestamp
	StartTimestamp() pcommon.Timestamp
	Attributes() pcommon.Map
}
