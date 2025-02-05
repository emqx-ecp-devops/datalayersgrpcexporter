// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otel2datalayers

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

type DatalayerWritter struct {
	clientConfig *ClientConfig
	client       *Client

	db           string
	table        string
	partitionNum int

	telemetrySettings component.TelemetrySettings
	payloadMaxLines   int
	payloadMaxBytes   int
}

func NewDatalayerWritter(host, username, password, tlsPath, db, table string, partitionNum int, port uint32, payloadMaxLines, payloadMaxBytes int,
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
		partitionNum:      partitionNum,
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
	sqlCreateTable := `CREATE TABLE IF NOT EXISTS %s.%s (
        ts TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        instantce_name STRING,
        timestamp key(ts)
    )
    PARTITION BY HASH(%s) PARTITIONS %d
    ENGINE=TimeSeries;
	`
	sql = fmt.Sprintf(sqlCreateTable, w.db, w.table, "instantce_name", w.partitionNum)

	_, err = w.client.Execute(sql)
	if err != nil {
		fmt.Println("Failed to create table: ", err)
		return err
	}

	go w.ProcessMetrics(ctx)

	return nil
}

type compareMap struct {
	mu            sync.RWMutex
	columnsMap    map[string]int32
	oldColumnsMap map[string]int32
}

func (c compareMap) ResetColumnsMap() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.columnsMap = map[string]int32{}
}

func (c compareMap) ResetOldColumnsMap() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.oldColumnsMap = map[string]int32{}
}

// todo: other functions
func (c compareMap) AddColumnsMap(key string, value int32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.columnsMap[key] = value
}

func (c compareMap) SwapColumnsMap() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.oldColumnsMap = c.columnsMap
	c.columnsMap = map[string]int32{}
}

var CompareObject = compareMap{
	mu:            sync.RWMutex{},
	columnsMap:    map[string]int32{},
	oldColumnsMap: map[string]int32{},
}

func (w *DatalayerWritter) AlterTableWithColumnsMap() error {
	// todo: 根据 len(columnsMap) > oldColumnsLenght 时， 在 concatenateSql 中自动触发修改表
	// 这样配置文件中就不需要配置表字段了
	if len(CompareObject.columnsMap) > len(CompareObject.oldColumnsMap) {

		for k, v := range CompareObject.columnsMap {
			if _, ok := CompareObject.oldColumnsMap[k]; !ok {

				sqlAlterTable := `ALTER TABLE %s.%s ADD COLUMN %s_%d %s;`
				sql := fmt.Sprintf(sqlAlterTable, w.db, w.table, k, v, tableTypeString(v))

				_, err := w.client.Execute(sql)
				if err != nil && !strings.Contains(err.Error(), "has already exist") {
					fmt.Println("Failed to update table: ", err)
					return err
				}
			}
		}
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
		return "STRING"
	case int32(pmetric.MetricTypeSummary):
		return "STRING"
	case int32(pmetric.MetricTypeExponentialHistogram):
		return "DOUBLE"
	default:
		return "STRING"
	}
	return "UINT64"
}
