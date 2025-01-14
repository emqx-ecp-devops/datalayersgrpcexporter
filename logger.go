// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datalayersexporter

import (
	"github.com/emqx-ecp-devops/datalayersexporter/internal/otel2datalayers"
	"go.uber.org/zap"
)

type zapInfluxLogger struct {
	*zap.SugaredLogger
}

func newZapInfluxLogger(logger *zap.Logger) otel2datalayers.Logger {
	return &otel2datalayers.ErrorLogger{
		Logger: &zapInfluxLogger{
			logger.Sugar(),
		},
	}
}

func (l zapInfluxLogger) Debug(msg string, kv ...any) {
	l.SugaredLogger.Debugw(msg, kv...)
}
