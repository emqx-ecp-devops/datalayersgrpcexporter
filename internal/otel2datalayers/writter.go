// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otel2datalayers

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type DatalayerWritter struct {
	clientConfig *ClientConfig
	client       *Client
	partitionNum int

	telemetrySettings component.TelemetrySettings
	payloadMaxLines   int
	payloadMaxBytes   int
	ttl               int
}

func NewDatalayerWritter(host, username, password, tlsPath string, partitionNum int, port uint32, payloadMaxLines, payloadMaxBytes int,
	telemetrySettings component.TelemetrySettings, ttl int) (*DatalayerWritter, error) {
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
		partitionNum:      partitionNum,
		telemetrySettings: telemetrySettings,
		payloadMaxLines:   payloadMaxLines,
		payloadMaxBytes:   payloadMaxBytes,
		ttl:               ttl,
	}, nil
}

// Start implements component.StartFunc
func (w *DatalayerWritter) Start(ctx context.Context, host component.Host) error {
	// // TODO: to check the database and tables is existed? Create without existing.
	// tableMap[w.table] = nil

	// // Creates a database.
	// sql := fmt.Sprintf("create database if not exists %s;", w.db)
	// _, err := w.client.Execute(sql)
	// if err != nil {
	// 	fmt.Println("Failed to create database: ", err)
	// 	return err
	// }

	// // Creates a table.
	// sqlCreateTable := `CREATE TABLE IF NOT EXISTS %s.%s (
	//       ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	//       instance_id STRING DEFAULT 'Unknown',
	//       timestamp key(ts)
	//   )
	//   PARTITION BY HASH(%s) PARTITIONS %d
	//   ENGINE=TimeSeries;
	// `
	// sql = fmt.Sprintf(sqlCreateTable, w.db, w.table, "instance_id", w.partitionNum)

	// _, err = w.client.Execute(sql)
	// if err != nil {
	// 	fmt.Println("Failed to create table: ", err)
	// 	return err
	// }

	//todo: 可以配置并发数
	go w.ProcessMetrics(ctx)

	return nil
}

var tableMap = map[string]map[string]map[string]any{} // key: db, value: tableName, value: fieldName

func (w *DatalayerWritter) CheckDBAndTable(db, tableName string, partitions, fields []string, valueType int32) error {
	if len(partitions) == 0 {
		return errors.New("PartitionKeys is empty")
	}

	if _, ok := tableMap[db]; !ok {
		// Creates a database.
		sqlCreateDB := "CREATE DATABASE IF NOT EXISTS %s"
		sql := fmt.Sprintf(sqlCreateDB, db)

		_, err := w.client.Execute(sql)
		if err != nil {
			fmt.Println("Failed to create databases: ", err)
			return err
		}

		tableMap[db] = map[string]map[string]any{}
	}

	dbTables := tableMap[db]
	if oldFieldsMap, ok := dbTables[tableName]; ok {
		if len(oldFieldsMap) != 0 {
			for _, partition := range partitions {
				if _, ok := oldFieldsMap[partition]; !ok {
					//todo: 新增字段, PartitionKey 暂时不支持动态修改

					oldFieldsMap[partition] = nil
					dbTables[tableName] = oldFieldsMap
					tableMap[db] = dbTables
				}
			}
			for _, field := range fields {
				if _, ok := oldFieldsMap[field]; !ok {
					//新增字段
					sqlAlterTable := "ALTER TABLE %s.%s ADD COLUMN %s STRING DEFAULT '';"
					sql := fmt.Sprintf(sqlAlterTable, db, tableName, field)

					_, err := w.client.Execute(sql)
					if err != nil {
						fmt.Println("Failed to alter table: ", err)
						return err
					}

					oldFieldsMap[field] = nil
					dbTables[tableName] = oldFieldsMap
					tableMap[db] = dbTables
				}
			}
		}
	} else {
		// Creates a table.
		sqlCreateTable := `CREATE TABLE IF NOT EXISTS %s.%s (
				ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
				val %s,
				%s
				timestamp key(ts)
				)
				PARTITION BY HASH(%s) PARTITIONS %d
				ENGINE=TimeSeries
				with (ttl='%dh')
				`
		newFields := map[string]any{}
		fieldSql := ""
		partitionKeys := ""
		for _, partition := range partitions {
			fieldSql += fmt.Sprintf("%s STRING DEFAULT '',", partition)
			partitionKeys += fmt.Sprintf("%s,", partition)
			newFields[partition] = nil
		}
		for _, field := range fields {
			fieldSql += fmt.Sprintf("%s STRING DEFAULT '',", field)
			newFields[field] = nil
		}
		partitionKeys = strings.TrimSuffix(partitionKeys, ",")

		sql := fmt.Sprintf(sqlCreateTable, db, tableName, tableTypeString(valueType), fieldSql, partitionKeys, w.partitionNum, w.ttl)

		_, err := w.client.Execute(sql)
		if err != nil {
			fmt.Println("Failed to create table: ", err)
			return err
		}

		dbTables[tableName] = newFields
		tableMap[db] = dbTables
	}

	return nil
}

// tableTypeString returns the string representation of the metric type
// todo: 需确认类型映射是否正确
func tableTypeString(t int32) string {
	switch t {
	case int32(pmetric.MetricTypeGauge):
		return "DOUBLE"
	case int32(pmetric.MetricTypeSum):
		return "DOUBLE"
	case int32(pmetric.MetricTypeHistogram):
		return "DOUBLE"
	case int32(pmetric.MetricTypeSummary):
		return "DOUBLE"
	case int32(pmetric.MetricTypeExponentialHistogram):
		return "DOUBLE"
	default:
		return "STRING"
	}
}
