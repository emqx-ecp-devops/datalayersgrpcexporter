// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datalayersexporter

import (
	"context"

	"github.com/emqx-ecp-devops/datalayersexporter/internal/otel2datalayers"
	"go.opentelemetry.io/collector/component"
)

type datalayerWritter struct {
	clientConfig *otel2datalayers.ClientConfig
	client       *otel2datalayers.Client

	db      string
	table   string
	columns map[string]string

	telemetrySettings component.TelemetrySettings
	payloadMaxLines   int
	payloadMaxBytes   int

	logger otel2datalayers.Logger
}

func newDatalayerWritter(logger otel2datalayers.Logger, config *Config, telemetrySettings component.TelemetrySettings) (*datalayerWritter, error) {
	clientConfig := &otel2datalayers.ClientConfig{
		Host:     config.Endpoint,
		Username: config.Username,
		Password: config.Password,
		TlsCert:  &config.TlsCertPath,
	}

	c, err := otel2datalayers.MakeClient(clientConfig)
	if err != nil {
		return nil, err
	}

	return &datalayerWritter{
		clientConfig: clientConfig,
		client:       c,
		db:           config.DB,
		table:        config.Table,
		columns:      config.Columns,

		telemetrySettings: telemetrySettings,
		payloadMaxLines:   config.PayloadMaxLines,
		payloadMaxBytes:   config.PayloadMaxBytes,
		logger:            logger,
	}, nil
}

// Start implements component.StartFunc
func (w *datalayerWritter) Start(ctx context.Context, host component.Host) error {
	// TODO: to check the database and tables is existed? Create without existing.

	return nil
}
