// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datalayersexporter

import (
	"context"
	"fmt"
	"strings"

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

	// Creates a database.
	sql := fmt.Sprintf("create database if not exists %s;", w.db)
	_, err := w.client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to create database: ", err)
		return err
	}

	// Creates a table.
	// If w.colums is empty, will use the automatic schema later. It will use all metrics as fields.
	columsStr := ""
	if len(w.columns) > 0 {
		for k, v := range w.columns {
			columsStr += fmt.Sprintf("`%s` `%s` ,", k, v)
		}
	}

	sql = fmt.Sprintf(`
    create table if not exists %s.%s (
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        %s
        timestamp key(ts)
    );`, w.db, w.table, columsStr)
	_, err = w.client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to create table: ", err)
		return err
	}

	go otel2datalayers.ProcessMetrics()

	return nil
}

func (w *datalayerWritter) concatenateSql(metrics otel2datalayers.MetricsMultipleLines) {
	sql := "INSERT INTO %s (%s) VALUES (%s);"

	table := w.db + "." + w.table
	columns := ""
	for k, v := range w.columns {
		columns += fmt.Sprintf("`%s_%s`,", k, v)
	}
	columns = strings.TrimSuffix(columns, ",")

	values := ""
	for _, metric := range metrics {
		values += fmt.Sprintf("%v,", metric.Value)
	}
	values = strings.TrimSuffix(values, ",")

	sql = fmt.Sprintf(sql, table, columns, values)
	_, err := w.client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to insert metrics: ", err)
		return
	}

}
