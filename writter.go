// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package datalayersexporter

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"sync"

	"github.com/emqx-ecp-devops/datalayersexporter/internal/otel2datalayers"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configopaque"
)

type datalayerWritter struct {
	encoderPool sync.Pool
	httpClient  *http.Client

	httpClientSettings confighttp.ClientConfig
	telemetrySettings  component.TelemetrySettings
	writeURL           string
	payloadMaxLines    int
	payloadMaxBytes    int

	logger otel2datalayers.Logger
}

func newDatalayerWritter(logger otel2datalayers.Logger, config *Config, telemetrySettings component.TelemetrySettings) (*datalayerWritter, error) {
	writeURL, err := composeWriteURL(config)
	if err != nil {
		return nil, err
	}

	return &datalayerWritter{
		encoderPool: sync.Pool{
			New: nil,
		},
		httpClientSettings: config.ClientConfig,
		telemetrySettings:  telemetrySettings,
		writeURL:           writeURL,
		payloadMaxLines:    config.PayloadMaxLines,
		payloadMaxBytes:    config.PayloadMaxBytes,
		logger:             logger,
	}, nil
}

func composeWriteURL(config *Config) (string, error) {
	writeURL, err := url.Parse(config.ClientConfig.Endpoint)
	if err != nil {
		return "", err
	}
	if writeURL.Path == "" || writeURL.Path == "/" {
		writeURL, err = writeURL.Parse("write")
		if err != nil {
			return "", err
		}
	}
	queryValues := writeURL.Query()
	queryValues.Set("precision", "ns")
	queryValues.Set("db", config.DB)

	if config.Username != "" && config.Password != "" {
		basicAuth := base64.StdEncoding.EncodeToString(
			[]byte(config.Username + ":" + string(config.Password)))
		if config.ClientConfig.Headers == nil {
			config.ClientConfig.Headers = make(map[string]configopaque.String, 1)
		}
		config.ClientConfig.Headers["Authorization"] = configopaque.String("Basic " + basicAuth)
	}
	writeURL.RawQuery = queryValues.Encode()

	return writeURL.String(), nil
}

// Start implements component.StartFunc
func (w *datalayerWritter) Start(ctx context.Context, host component.Host) error {
	httpClient, err := w.httpClientSettings.ToClient(ctx, host, w.telemetrySettings)
	if err != nil {
		return err
	}
	w.httpClient = httpClient
	return nil
}
