// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datalayersgrpcexporter

import (
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"golang.org/x/exp/maps"
)

type Trace struct {
	Table string `mapstructure:"table"`
	// SpanDimensions are span attributes to be used as line protocol tags.
	// These are always included as tags:
	// - trace ID
	// - span ID
	// The default values are strongly recommended for use with Jaeger:
	// - service.name
	// - span.name
	// Other common attributes can be found here:
	// - https://opentelemetry.io/docs/specs/semconv/
	SpanDimensions []string `mapstructure:"span_dimensions"`
	// SpanFields are span attributes to be used as line protocol fields.
	// SpanFields can be empty.
	SpanFields     []string      `mapstructure:"span_fields"`
	CustomKeyScope string        `mapstructure:"custom_key_scope"`
	Custom         []CustomTrace `mapstructure:"custom"`
}

type CustomTrace struct {
	Key            []string `mapstructure:"key"`
	Table          string   `mapstructure:"table"`
	SpanDimensions []string `mapstructure:"span_dimensions"`
	SpanFields     []string `mapstructure:"span_fields"`
}

// Config defines configuration for the InfluxDB exporter.
type Config struct {
	// confighttp.ClientConfig   `mapstructure:",squash"`
	QueueSettings             exporterhelper.QueueSettings `mapstructure:"sending_queue"`
	configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	// // Org is the InfluxDB organization name of the destination bucket.
	// Org string `mapstructure:"org"`
	// // Bucket is the InfluxDB bucket name that telemetry will be written to.
	// Bucket string `mapstructure:"bucket"`
	// // Token is used to identify InfluxDB permissions within the organization.
	// Token configopaque.String `mapstructure:"token"`

	// Endpoint is the InfluxDB server URL.
	Host string `mapstructure:"host"`

	// Port is the InfluxDB server port.
	Port uint32 `mapstructure:"port"`

	// TlsCertPath is the path to the TLS certificate to use for HTTPS requests.
	TlsCertPath string `mapstructure:"tls_cert_path"`

	// PartitionNum is the number of partitions to use for partitioning.
	PartitionNum int `mapstructure:"partition_num"`
	// Username is used to optionally specify the basic auth username
	Username string `mapstructure:"username"`
	// Password is used to optionally specify the basic auth password
	Password string `mapstructure:"password"`

	Trace Trace `mapstructure:"trace"`
	// PayloadMaxLines is the maximum number of line protocol lines to POST in a single request.
	PayloadMaxLines int `mapstructure:"payload_max_lines"`
	// PayloadMaxBytes is the maximum number of line protocol bytes to POST in a single request.
	PayloadMaxBytes int `mapstructure:"payload_max_bytes"`

	// MetricsSchema indicates the metrics schema to emit to line protocol.
	// Options:
	// - telegraf-prometheus-v1
	// - telegraf-prometheus-v2
	MetricsSchema string `mapstructure:"metrics_schema"`

	// TTL is the TTL of datalayers's table. the uint is the number of hours.
	TTL int `mapstructure:"ttl"`
}

func (cfg *Config) Validate() error {
	globalSpanTags := make(map[string]struct{}, len(cfg.Trace.SpanDimensions))
	globalSpanFields := make(map[string]struct{}, len(cfg.Trace.SpanDimensions))
	duplicateTags := make(map[string]struct{})
	for _, k := range cfg.Trace.SpanDimensions {
		if _, found := globalSpanTags[k]; found {
			duplicateTags[k] = struct{}{}
		} else {
			globalSpanTags[k] = struct{}{}
		}
	}
	if len(duplicateTags) > 0 {
		return fmt.Errorf("duplicate span dimension(s) configured: %s",
			strings.Join(maps.Keys(duplicateTags), ","))
	}
	duplicateFields := make(map[string]struct{})
	for _, k := range cfg.Trace.SpanFields {
		if _, found := globalSpanFields[k]; found {
			duplicateTags[k] = struct{}{}
		} else {
			globalSpanFields[k] = struct{}{}
		}
	}
	if len(duplicateFields) > 0 {
		return fmt.Errorf("duplicate span fields(s) configured: %s",
			strings.Join(maps.Keys(duplicateFields), ","))
	}
	if len(cfg.Trace.Custom) > 0 {
		// validate custom_key_scope
		// valid values: name, kind, parent_span_id, status_code, context.trace_id, context.span_id, attributes.xxx
		keyScope := cfg.Trace.CustomKeyScope
		switch keyScope {
		case "name", "kind", "parent_span_id", "status_code", "context.trace_id", "context.span_id":
		default:
			if !strings.HasPrefix(keyScope, "attributes.") {
				return fmt.Errorf("invalid custom key scope %s, valid scope values are: name, kind, parent_span_id, status_code, context.trace_id, context.span_id, attributes.xxx", keyScope)
			}
		}

		// validate tags & fields
		customKeys := make(map[string]struct{})
		duplicateKeys := make(map[string]struct{})
		for _, custom := range cfg.Trace.Custom {
			keyArray := custom.Key
			for _, k := range keyArray {
				if _, found := customKeys[k]; found {
					duplicateKeys[k] = struct{}{}
				} else {
					customKeys[k] = struct{}{}
				}
			}
			customSpanTags := make(map[string]struct{}, len(cfg.Trace.SpanDimensions))
			customSpanFields := make(map[string]struct{})
			duplicateTags = make(map[string]struct{})
			for _, k := range custom.SpanDimensions {
				if _, found := customSpanTags[k]; found {
					duplicateTags[k] = struct{}{}
				} else {
					customSpanTags[k] = struct{}{}
				}
			}
			if len(duplicateTags) > 0 {
				return fmt.Errorf("duplicate custom span dimension(s) configured: %s",
					strings.Join(maps.Keys(duplicateTags), ","))
			}
			duplicateFields := make(map[string]struct{})
			for _, k := range custom.SpanFields {
				if _, found := customSpanFields[k]; found {
					duplicateFields[k] = struct{}{}
				} else {
					customSpanFields[k] = struct{}{}
				}
			}
			if len(duplicateFields) > 0 {
				return fmt.Errorf("duplicate custom span fields(s) configured: %s",
					strings.Join(maps.Keys(duplicateFields), ","))
			}
		}
		if len(duplicateKeys) > 0 {
			return fmt.Errorf("duplicate custom key configured: %s",
				strings.Join(maps.Keys(duplicateKeys), ","))
		}
	}

	return nil
}
