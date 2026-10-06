// Copyright 2020 Trey Dockendorf
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0

package collector

import (
        "context"
        "regexp"
        "strconv"
        "strings"
        "time"

        "github.com/alecthomas/kingpin/v2"
        "github.com/go-kit/log"
        "github.com/go-kit/log/level"
        "github.com/prometheus/client_golang/prometheus"
        "github.com/treydock/tsm_exporter/config"
)

var (
        requestTimeout = kingpin.Flag(
                "collector.request.timeout",
                "Timeout for collecting request information",
        ).Default("20").Int()

        DsmadmcRequestExec = dsmadmcRequest
)

type RequestMetric struct {
        RequestNumber   string
        Message         string
        RemainingSeconds float64
}

type RequestCollector struct {
        Active            *prometheus.Desc
        RemainingSeconds  *prometheus.Desc
        RequestsOutstanding *prometheus.Desc

        target config.Target
        logger log.Logger
}

func init() {
        registerCollector("request", true, NewRequestExporter)
}

func NewRequestExporter(target *config.Target, logger log.Logger) Collector {
        return &RequestCollector{
                Active: prometheus.NewDesc(
                        prometheus.BuildFQName(namespace, "request", "active"),
                        "Whether a TSM request is currently outstanding",
                        []string{"request","message"},
                        nil,
                ),

                RemainingSeconds: prometheus.NewDesc(
                        prometheus.BuildFQName(namespace, "request", "remaining_seconds"),
                        "Number of seconds remaining before the outstanding TSM request deadline",
                        []string{"request"},
                        nil,
                ),
                RequestsOutstanding: prometheus.NewDesc(
                        prometheus.BuildFQName(namespace, "requests", "outstanding"),
                        "Number of outstanding TSM requests",
                        nil,
                        nil,
                ),

                target: *target,
                logger: logger,
        }
}

func (c *RequestCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- c.Active
        ch <- c.RemainingSeconds
        ch <- c.RequestsOutstanding
}

func (c *RequestCollector) Collect(ch chan<- prometheus.Metric) {
        level.Debug(c.logger).Log(
                "msg",
                "Collecting request metrics",
        )

        collectTime := time.Now()

        timeout := 0
        errorMetric := 0

        metrics, err := c.collect()

        if err == context.DeadlineExceeded {
                timeout = 1
        } else if err != nil {
                level.Error(c.logger).Log(
                        "msg",
                        err,
                )

                errorMetric = 1
        }

        ch <- prometheus.MustNewConstMetric(
                c.RequestsOutstanding,
                prometheus.GaugeValue,
                float64(len(metrics)),
        )

        for _, metric := range metrics {

                ch <- prometheus.MustNewConstMetric(
                        c.Active,
                        prometheus.GaugeValue,
                        1,
                        metric.RequestNumber,
                        metric.Message,
                )

                ch <- prometheus.MustNewConstMetric(
                        c.RemainingSeconds,
                        prometheus.GaugeValue,
                        metric.RemainingSeconds,
                        metric.RequestNumber,
                )
        }

        ch <- prometheus.MustNewConstMetric(
                collectError,
                prometheus.GaugeValue,
                float64(errorMetric),
                "request",
        )

        ch <- prometheus.MustNewConstMetric(
                collecTimeout,
                prometheus.GaugeValue,
                float64(timeout),
                "request",
        )

        ch <- prometheus.MustNewConstMetric(
                collectDuration,
                prometheus.GaugeValue,
                time.Since(collectTime).Seconds(),
                "request",
        )
}

func (c *RequestCollector) collect() ([]RequestMetric, error) {
        ctx, cancel := context.WithTimeout(
                context.Background(),
                time.Duration(*requestTimeout)*time.Second,
        )
        defer cancel()

        out, err := DsmadmcRequestExec(
                &c.target,
                ctx,
                c.logger,
        )

        if err != nil {
                return nil, err
        }

        return requestParse(out, c.logger), nil
}

func dsmadmcRequest(
        target *config.Target,
        ctx context.Context,
        logger log.Logger,
) (string, error) {

        out, err := dsmadmcCommand(
                target,
                "q request",
                ctx,
                logger,
        )

        // Spectrum Protect returns RC 11 when there are no
        // outstanding requests. This is a valid state, not
        // an exporter error.
        if strings.Contains(
                out,
                "ANR8346I QUERY REQUEST: No requests are outstanding.",
        ) {
                return out, nil
        }

        return out, err
}

func requestParse(
        out string,
        logger log.Logger,
) []RequestMetric {

        metrics := []RequestMetric{}

        lines := strings.Split(out, "\n")

        /*
                Example:

                ANR8342I Requests outstanding:
                ANR8381I 303: LTO volume TP0567L8 is required for use in library TS4500; CHECKIN LIBVOLUME required within 27 minutes.
        */

        requestRegex := regexp.MustCompile(
                `ANR\d{4}I\s+([0-9]+):\s+(.*?)(?:\s+within\s+([0-9]+)\s+minutes?\.)?$`,
        )

        for _, line := range lines {

                line = strings.TrimSpace(line)

                if line == "" {
                        continue
                }

                // No outstanding requests.
                if strings.Contains(
                        line,
                        "ANR8346I QUERY REQUEST: No requests are outstanding",
                ) {
                        continue
                }

                matches := requestRegex.FindStringSubmatch(line)

                if len(matches) == 0 {
                        continue
                }

                requestNumber := matches[1]
                message := strings.TrimSpace(matches[2])

                remainingSeconds := 0.0

                if matches[3] != "" {

                        minutes, err := strconv.ParseFloat(
                                matches[3],
                                64,
                        )

                        if err != nil {
                                level.Error(logger).Log(
                                        "msg",
                                        "Failed to parse request remaining minutes",
                                        "value",
                                        matches[3],
                                        "err",
                                        err,
                                )

                                continue
                        }

                        remainingSeconds = minutes * 60
                }

                metrics = append(
                        metrics,
                        RequestMetric{
                                RequestNumber:    requestNumber,
                                Message:          message,
                                RemainingSeconds: remainingSeconds,
                        },
                )
        }

        return metrics
}
