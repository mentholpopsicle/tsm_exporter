// Copyright 2020 Trey Dockendorf
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
        "context"
        "strings"
        "time"

        "github.com/alecthomas/kingpin/v2"
        "github.com/go-kit/log"
        "github.com/go-kit/log/level"
        "github.com/prometheus/client_golang/prometheus"
        "github.com/treydock/tsm_exporter/config"
)

var (
        processTimeout     = kingpin.Flag("collector.process.timeout", "Timeout for collecting running processes information").Default("20").Int()
        DsmadmcProcessExec = dsmadmcProcess
)

type ProcessMetric struct {
        ProcessNum       string
        Process          string
        StartTime        float64
        ElapsedSeconds   float64
        BytesProcessed   float64
        BytesPerSecond   float64
}

type ProcessCollector struct {
        StartTime      *prometheus.Desc
        Elapsed        *prometheus.Desc
        BytesProcessed *prometheus.Desc
        BytesPerSecond *prometheus.Desc
        target         *config.Target
        logger         log.Logger
}

func init() {
        registerCollector("process", true, NewProcessExporter)
}

func NewProcessExporter(target *config.Target, logger log.Logger) Collector {
        labels := []string{"process_num", "process"}

        return &ProcessCollector{
                StartTime: prometheus.NewDesc(
                        prometheus.BuildFQName(namespace, "process", "start_timestamp_seconds"),
                        "Start time of the TSM process",
                        labels,
                        nil,
                ),

                Elapsed: prometheus.NewDesc(
                        prometheus.BuildFQName(namespace, "process", "elapsed_seconds"),
                        "Elapsed time of the TSM process in seconds",
                        labels,
                        nil,
                ),

                BytesProcessed: prometheus.NewDesc(
                        prometheus.BuildFQName(namespace, "process", "bytes_processed"),
                        "Amount of data processed by the TSM process in bytes",
                        labels,
                        nil,
                ),

                BytesPerSecond: prometheus.NewDesc(
                        prometheus.BuildFQName(namespace, "process", "bytes_per_second"),
                        "Processing rate of the TSM process in bytes per second",
                        labels,
                        nil,
                ),

                target: target,
                logger: logger,
        }
}

func (c *ProcessCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- c.StartTime
        ch <- c.Elapsed
        ch <- c.BytesProcessed
        ch <- c.BytesPerSecond
}

func (c *ProcessCollector) Collect(ch chan<- prometheus.Metric) {
        level.Debug(c.logger).Log("msg", "Collecting metrics")

        collectTime := time.Now()
        timeout := 0
        errorMetric := 0

        metrics, err := c.collect()

        if err == context.DeadlineExceeded {
                timeout = 1
        } else if err != nil {
                level.Error(c.logger).Log("msg", err)
                errorMetric = 1
        }

        for _, m := range metrics {
                ch <- prometheus.MustNewConstMetric(
                        c.StartTime,
                        prometheus.GaugeValue,
                        m.StartTime,
                        m.ProcessNum,
                        m.Process,
                )

                ch <- prometheus.MustNewConstMetric(
                        c.Elapsed,
                        prometheus.GaugeValue,
                        m.ElapsedSeconds,
                        m.ProcessNum,
                        m.Process,
                )

                ch <- prometheus.MustNewConstMetric(
                        c.BytesProcessed,
                        prometheus.GaugeValue,
                        m.BytesProcessed,
                        m.ProcessNum,
                        m.Process,
                )

                ch <- prometheus.MustNewConstMetric(
                        c.BytesPerSecond,
                        prometheus.GaugeValue,
                        m.BytesPerSecond,
                        m.ProcessNum,
                        m.Process,
                )
        }

        ch <- prometheus.MustNewConstMetric(
                collectError,
                prometheus.GaugeValue,
                float64(errorMetric),
                "process",
        )

        ch <- prometheus.MustNewConstMetric(
                collecTimeout,
                prometheus.GaugeValue,
                float64(timeout),
                "process",
        )

        ch <- prometheus.MustNewConstMetric(
                collectDuration,
                prometheus.GaugeValue,
                time.Since(collectTime).Seconds(),
                "process",
        )
}

func (c *ProcessCollector) collect() (map[string]ProcessMetric, error) {
        ctx, cancel := context.WithTimeout(
                context.Background(),
                time.Duration(*processTimeout)*time.Second,
        )
        defer cancel()

        out, err := DsmadmcProcessExec(c.target, ctx, c.logger)
        if err != nil {
                return nil, err
        }

        metrics, err := processParse(out, c.target, c.logger)
        return metrics, err
}

func buildProcessQuery(target *config.Target) string {
        return "SELECT PROCESS_NUM, PROCESS, START_TIME, BYTES_PROCESSED, " +
                "TIMESTAMPDIFF(2, CHAR(CURRENT_TIMESTAMP - START_TIME)) " +
                "FROM PROCESSES"
}

func dsmadmcProcess(
        target *config.Target,
        ctx context.Context,
        logger log.Logger,
) (string, error) {
        out, err := dsmadmcQuery(
                target,
                buildProcessQuery(target),
                ctx,
                logger,
        )

        return out, err
}

func processParse(
        out string,
        target *config.Target,
        logger log.Logger,
) (map[string]ProcessMetric, error) {

        metrics := make(map[string]ProcessMetric)

        records, err := getRecords(out, logger)
        if err != nil {
                return nil, err
        }

        for _, record := range records {

                // PROCESS_NUM
                // PROCESS
                // START_TIME
                // BYTES_PROCESSED
                // ELAPSED_SECONDS
                if len(record) != 5 {
                        continue
                }

                var metric ProcessMetric

                processNum := strings.TrimSpace(record[0])
                process := strings.TrimSpace(record[1])

                // Parse start time
                startTime, err := parseTime(record[2], target)
                if err != nil {
                        level.Error(logger).Log(
                                "msg", "Failed to parse START_TIME",
                                "value", record[2],
                                "record", strings.Join(record, ","),
                                "err", err,
                        )
                        return nil, err
                }

                // Parse bytes processed
                bytesProcessed, err := parseFloat(record[3])
                if err != nil {
                        level.Error(logger).Log(
                                "msg", "Error parsing bytes processed",
                                "value", record[3],
                                "record", strings.Join(record, ","),
                                "err", err,
                        )
                        return nil, err
                }

                // Parse elapsed seconds
                elapsedSeconds, err := parseFloat(record[4])
                if err != nil {
                        level.Error(logger).Log(
                                "msg", "Error parsing elapsed seconds",
                                "value", record[4],
                                "record", strings.Join(record, ","),
                                "err", err,
                        )
                        return nil, err
                }

                // Calculate processing rate.
                var bytesPerSecond float64

                if elapsedSeconds > 0 {
                        bytesPerSecond = bytesProcessed / elapsedSeconds
                }

                metric.ProcessNum = processNum
                metric.Process = process
                metric.StartTime = float64(startTime.Unix())
                metric.ElapsedSeconds = elapsedSeconds
                metric.BytesProcessed = bytesProcessed
                metric.BytesPerSecond = bytesPerSecond

                // A process number should uniquely identify a running
                // process, so use it as the map key.
                metrics[processNum] = metric
        }

        return metrics, nil
}
