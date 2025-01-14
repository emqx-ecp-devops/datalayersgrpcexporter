package otel2datalayers

import (
	"context"
	"time"
)

type InfluxMetricValueType uint8

const (
	InfluxMetricValueTypeUntyped InfluxMetricValueType = iota
	InfluxMetricValueTypeGauge
	InfluxMetricValueTypeSum
	InfluxMetricValueTypeHistogram
	InfluxMetricValueTypeSummary
)

type InfluxWriter interface {
	NewBatch() InfluxWriterBatch
}

type InfluxWriterBatch interface {
	EnqueuePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType InfluxMetricValueType) error
	WriteBatch(ctx context.Context) error
}

type NoopInfluxWriter struct{}

func (w *NoopInfluxWriter) NewBatch() InfluxWriterBatch {
	return w
}

func (w *NoopInfluxWriter) EnqueuePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType InfluxMetricValueType) error {
	return nil
}

func (w *NoopInfluxWriter) WriteBatch(ctx context.Context) error {
	return nil
}
