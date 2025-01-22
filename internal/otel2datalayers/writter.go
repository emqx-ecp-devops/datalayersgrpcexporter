// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otel2datalayers

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
)

type DatalayerWritter struct {
	clientConfig *ClientConfig
	client       *Client

	db            string
	table         string
	columns       map[string]string
	partitionKeys []string
	partitionNum  int

	telemetrySettings component.TelemetrySettings
	payloadMaxLines   int
	payloadMaxBytes   int
}

func NewDatalayerWritter(host, username, password, tlsPath, db, table string, partitions []string, port uint32, columns map[string]string, payloadMaxLines, payloadMaxBytes int,
	telemetrySettings component.TelemetrySettings) (*DatalayerWritter, error) {
	clientConfig := &ClientConfig{
		Host:     host,
		Port:     port,
		Username: username,
		Password: password,
		TlsCert:  &tlsPath,
	}

	c, err := MakeClient(clientConfig)
	if err != nil {
		return nil, err
	}

	return &DatalayerWritter{
		clientConfig:      clientConfig,
		client:            c,
		db:                db,
		table:             table,
		columns:           columns,
		partitionKeys:     partitions,
		telemetrySettings: telemetrySettings,
		payloadMaxLines:   payloadMaxLines,
		payloadMaxBytes:   payloadMaxBytes,
	}, nil
}

// Start implements component.StartFunc
func (w *DatalayerWritter) Start(ctx context.Context, host component.Host) error {
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
			columsStr += fmt.Sprintf("`%s` %s ,", k, v)
		}
	}
	// partitionStr := strings.Join(w.partitionKeys, ", ")
	// if partitionStr == "" {
	// 	partitionStr = "ts"
	// }

	sqlCreateTable := `CREATE TABLE %s.%s (
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        %s
        timestamp key(ts)
    )

    ENGINE=TimeSeries;
	`
	sql = fmt.Sprintf(sqlCreateTable, w.db, w.table, columsStr)

	_, err = w.client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to create table: ", err)
		return err
	}

	go w.ProcessMetrics(ctx)

	return nil
}
