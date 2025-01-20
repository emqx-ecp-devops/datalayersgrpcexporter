// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datalayersgrpcexporter

import (
	"context"

	"github.com/emqx-ecp-devops/datalayersgrpcexporter/internal/metadata"
	"github.com/emqx-ecp-devops/datalayersgrpcexporter/internal/otel2datalayers"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// NewFactory creates a factory for InfluxDB exporter.
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		// exporter.WithTraces(createTraceExporter, metadata.TracesStability),
		exporter.WithMetrics(createMetricsExporter, metadata.MetricsStability),
		// exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:      "datalayers:6361",
		QueueSettings: exporterhelper.NewDefaultQueueSettings(),
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
		MetricsSchema: otel2datalayers.MetricsSchemaTelegrafPrometheusV1.String(),
		// Trace: Trace{
		// 	SpanDimensions: otel2influx.DefaultOtelTracesToLineProtocolConfig().GlobalTrace.SpanDimensions,
		// },
		// LogRecordDimensions: otel2influx.DefaultOtelLogsToLineProtocolConfig().LogRecordDimensions,
		// defaults per suggested:
		// https://docs.influxdata.com/influxdb/cloud-serverless/write-data/best-practices/optimize-writes/#batch-writes
		PayloadMaxLines: 10_000,
		PayloadMaxBytes: 10_000_000,
	}
}

// func createTraceExporter(
// 	ctx context.Context,
// 	set exporter.Settings,
// 	config component.Config,
// ) (exporter.Traces, error) {
// 	cfg := config.(*Config)

// 	logger := newZapInfluxLogger(set.Logger)

// 	writer, err := newInfluxHTTPWriter(logger, cfg, set.TelemetrySettings)
// 	if err != nil {
// 		return nil, err
// 	}

// 	expConfig := otel2influx.DefaultOtelTracesToLineProtocolConfig()
// 	expConfig.Logger = logger
// 	expConfig.Writer = writer
// 	trace := cfg.Trace
// 	expConfig.GlobalTrace = otel2influx.Trace{
// 		Table:          trace.Table,
// 		SpanDimensions: trace.SpanDimensions,
// 		SpanFields:     trace.SpanFields,
// 	}
// 	expConfig.CustomKeyScope = cfg.Trace.CustomKeyScope
// 	customs := cfg.Trace.Custom
// 	expConfig.CustomTraces = make(map[string]otel2influx.Trace, len(customs))
// 	for _, custom := range customs {
// 		customTrace := otel2influx.Trace{
// 			Table:          custom.Table,
// 			SpanDimensions: custom.SpanDimensions,
// 			SpanFields:     custom.SpanFields,
// 		}
// 		for _, key := range custom.Key {
// 			expConfig.CustomTraces[key] = customTrace
// 		}
// 	}
// 	exp, err := otel2influx.NewOtelTracesToLineProtocol(expConfig)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return exporterhelper.NewTracesExporter(
// 		ctx,
// 		set,
// 		cfg,
// 		exp.WriteTraces,
// 		exporterhelper.WithQueue(cfg.QueueSettings),
// 		exporterhelper.WithRetry(cfg.BackOffConfig),
// 		exporterhelper.WithStart(writer.Start),
// 	)
// }

func createMetricsExporter(ctx context.Context, set exporter.Settings, config component.Config) (exporter.Metrics, error) {
	cfg := config.(*Config)

	writer, err := newDatalayerWritter(cfg, set.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return exporterhelper.NewMetricsExporter(
		ctx,
		set,
		cfg,
		otel2datalayers.WriteMetrics,
		exporterhelper.WithQueue(cfg.QueueSettings),
		exporterhelper.WithRetry(cfg.BackOffConfig),
		exporterhelper.WithStart(writer.Start),
	)
}

// func createLogsExporter(ctx context.Context, set exporter.Settings, config component.Config) (exporter.Logs, error) {
// 	cfg := config.(*Config)

// 	logger := newZapInfluxLogger(set.Logger)

// 	writer, err := newInfluxHTTPWriter(logger, cfg, set.TelemetrySettings)
// 	if err != nil {
// 		return nil, err
// 	}

// 	expConfig := otel2influx.DefaultOtelLogsToLineProtocolConfig()
// 	expConfig.Logger = logger
// 	expConfig.Writer = writer
// 	expConfig.LogRecordDimensions = cfg.LogRecordDimensions
// 	exp, err := otel2influx.NewOtelLogsToLineProtocol(expConfig)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return exporterhelper.NewLogsExporter(
// 		ctx,
// 		set,
// 		cfg,
// 		exp.WriteLogs,
// 		exporterhelper.WithQueue(cfg.QueueSettings),
// 		exporterhelper.WithRetry(cfg.BackOffConfig),
// 		exporterhelper.WithStart(writer.Start),
// 	)
// }

func newDatalayerWritter(config *Config, telemetrySettings component.TelemetrySettings) (*otel2datalayers.DatalayerWritter, error) {
	return otel2datalayers.NewDatalayerWritter(
		config.Endpoint,
		config.Username,
		config.Password,
		config.TlsCertPath,
		config.DB,
		config.Table,
		config.Port,
		config.Columns,
		config.PayloadMaxLines,
		config.PayloadMaxBytes,
		telemetrySettings)
}
