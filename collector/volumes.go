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
        "math"
        "regexp"
        "strings"
        "time"
	"fmt"

        "github.com/alecthomas/kingpin/v2"
        "github.com/go-kit/log"
        "github.com/go-kit/log/level"
        "github.com/prometheus/client_golang/prometheus"
        "github.com/treydock/tsm_exporter/config"
)

var (
        volumesTimeout          = kingpin.Flag("collector.volumes.timeout", "Timeout for collecting volumes information").Default("30").Int()
        volumesClassnameExclude = kingpin.Flag("collector.volumes.classname-exclude", "Regexp of classname of exclude").Default("").String()
        DsmadmcVolumesExec      = dsmadmcVolumes
        volumeStatuses          = []string{"EMPTY", "FILLING", "FULL"}
)

type VolumeMetric struct {
        name          string
        classname     string
        access        string
        utilized      float64
        capacity      float64
        stgpool       string
        status        string
        times_mounted float64
        write_pass    float64
        read_errors   float64
        write_errors  float64
        error_state   float64
}

type VolumesCollector struct {
        unavailable   *prometheus.Desc
        readonly      *prometheus.Desc
        utilized      *prometheus.Desc
        capacity      *prometheus.Desc
        stgpool       *prometheus.Desc
        status        *prometheus.Desc
        times_mounted *prometheus.Desc
        write_pass    *prometheus.Desc
        target        *config.Target
        logger        log.Logger
        read_errors   *prometheus.Desc
        write_errors  *prometheus.Desc
        error_state   *prometheus.Desc
}

func init() {
        registerCollector("volumes", true, NewVolumesExporter)
}

func NewVolumesExporter(target *config.Target, logger log.Logger) Collector {
        return &VolumesCollector{
                unavailable: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volumes", "unavailable"),
                        "Number of unavailable volumes", nil, nil),
                readonly: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volumes", "readonly"),
                        "Number of readonly volumes", nil, nil),
                utilized: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "utilized_ratio"),
                        "Volume utilized ratio, 0.0-1.0", []string{"volume", "classname"}, nil),
                capacity: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "estimated_capacity_bytes"),
                        "Volume estimated capacity", []string{"volume", "classname"}, nil),
                stgpool: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "storage_pool_info"),
                        "Volume storage pool information", []string{"volume", "classname", "stgpool"}, nil),
                status: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "status_info"),
                        "Volume status information", []string{"volume", "classname", "status"}, nil),
                times_mounted: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "times_mounted"),
                        "Volume times mounted", []string{"volume", "classname"}, nil),
                write_pass: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "write_pass"),
                        "Volume write pass", []string{"volume", "classname"}, nil),
                target: target,
                logger: logger,
                read_errors: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "read_errors"),
                        "Volume Read Errors",[]string{"volume", "classname"},nil),
                write_errors: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "write_errors"),
                        "Volume Write Errors",[]string{"volume", "classname"},nil),
                error_state: prometheus.NewDesc(prometheus.BuildFQName(namespace, "volume", "error_state"),
                        "Volume error state (1=YES, 0=NO)",[]string{"volume", "classname"},nil),
        }
}

func (c *VolumesCollector) Describe(ch chan<- *prometheus.Desc) {
        ch <- c.unavailable
        ch <- c.readonly
        ch <- c.utilized
        ch <- c.capacity
        ch <- c.stgpool
        ch <- c.status
        ch <- c.times_mounted
        ch <- c.write_pass
        ch <- c.read_errors
        ch <- c.write_errors
        ch <- c.error_state
}

func (c *VolumesCollector) Collect(ch chan<- prometheus.Metric) {
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

        var unavailable float64
        var readonly float64
        for _, m := range metrics {
                switch m.access {
                case "UNAVAILABLE":
                        unavailable++
                case "READONLY":
                        readonly++
                }
                if !math.IsNaN(m.utilized) {
                        ch <- prometheus.MustNewConstMetric(c.utilized, prometheus.GaugeValue, m.utilized, m.name, m.classname)
                }
                if !math.IsNaN(m.capacity) {
                        ch <- prometheus.MustNewConstMetric(c.capacity, prometheus.GaugeValue, m.capacity, m.name, m.classname)
                }
                ch <- prometheus.MustNewConstMetric(c.stgpool, prometheus.GaugeValue, 1, m.name, m.classname, m.stgpool)
                for _, s := range volumeStatuses {
                        var status float64
                        if s == m.status {
                                status = 1
                        }
                        ch <- prometheus.MustNewConstMetric(c.status, prometheus.GaugeValue, status, m.name, m.classname, s)
                }
                ch <- prometheus.MustNewConstMetric(c.times_mounted, prometheus.GaugeValue, m.times_mounted, m.name, m.classname)
                ch <- prometheus.MustNewConstMetric(c.write_pass, prometheus.GaugeValue, m.write_pass, m.name, m.classname)
                ch <- prometheus.MustNewConstMetric(c.read_errors, prometheus.GaugeValue, m.read_errors, m.name, m.classname)
                ch <- prometheus.MustNewConstMetric(c.write_errors, prometheus.GaugeValue, m.write_errors, m.name, m.classname)
                ch <- prometheus.MustNewConstMetric(c.error_state, prometheus.GaugeValue, m.error_state, m.name, m.classname)
        }
        if err == nil {
                ch <- prometheus.MustNewConstMetric(c.unavailable, prometheus.GaugeValue, unavailable)
                ch <- prometheus.MustNewConstMetric(c.readonly, prometheus.GaugeValue, readonly)
        }

        ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "volumes")
        ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "volumes")
        ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "volumes")
}

func (c *VolumesCollector) collect() ([]VolumeMetric, error) {
        ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*volumesTimeout)*time.Second)
        defer cancel()
        out, err := DsmadmcVolumesExec(c.target, ctx, c.logger)
        if err != nil {
                return nil, err
        }
        metrics, err := volumesParse(out, c.logger)
        return metrics, err
}

func dsmadmcVolumes(target *config.Target, ctx context.Context, logger log.Logger) (string, error) {
        query := "SELECT access,est_capacity_mb,pct_utilized,devclass_name,volume_name,stgpool_name,status,times_mounted,write_pass,read_errors,write_errors,error_state FROM volumes"

    	if len(target.StgpoolNames) > 0 {
	    quoted := make([]string, len(target.StgpoolNames))
	    for i, pool := range target.StgpoolNames {
        	quoted[i] = fmt.Sprintf("'%s'", pool)
	    }

            query += fmt.Sprintf(
	        " WHERE stgpool_name IN (%s)",
	        strings.Join(quoted, ","),
            )
	}
        out, err := dsmadmcQuery(target, query, ctx, logger)
        return out, err
}

func volumesParse(out string, logger log.Logger) ([]VolumeMetric, error) {
        classnameExcludePattern := regexp.MustCompile(*volumesClassnameExclude)
        var metrics []VolumeMetric
        records, err := getRecords(out, logger)
        if err != nil {
                return nil, err
        }
        for _, record := range records {
                if len(record) != 12 {
                        continue
                }
                var metric VolumeMetric
                metric.name = record[4]
                metric.classname = record[3]
                if *volumesClassnameExclude != "" && classnameExcludePattern.MatchString(metric.classname) {
                        level.Debug(logger).Log("msg", "Skipping volume due to classname exclude", "volume", metric.name, "classname", metric.classname)
                        continue
                }
                metric.access = record[0]
                metric.stgpool = record[5]
                metric.status = record[6]
                capacity, err := parseFloat(record[1])
                if err != nil {
                        level.Error(logger).Log("msg", "Error parsing est_capacity_mb", "value", record[1], "record", strings.Join(record, ","), "err", err)
                        return nil, err
                }
                metric.capacity = capacity * 1024 * 1024
                utilized, err := parseFloat(record[2])
                if err != nil {
                        level.Error(logger).Log("msg", "Error parsing pct_utilized value", "value", record[2], "record", strings.Join(record, ","), "err", err)
                        return nil, err
                }
                metric.utilized = utilized / 100
                times_mounted, err := parseFloat(record[7])
                if err != nil {
                        level.Error(logger).Log("msg", "Error parsing times_mounted", "value", record[7], "record", strings.Join(record, ","), "err", err)
                        return nil, err
                }
                metric.times_mounted = times_mounted
                write_pass, err := parseFloat(record[8])
                if err != nil {
                        level.Error(logger).Log("msg", "Error parsing write_pass", "value", record[8], "record", strings.Join(record, ","), "err", err)
                        return nil, err
                }
                metric.write_pass = write_pass
                readErrors, err := parseFloat(record[9])
                if err != nil {
                    	level.Error(logger).Log("msg", "Error parsing read_errors", "value", record[9], "record", strings.Join(record, ","),"err", err)
                    return nil, err
                }
                metric.read_errors = readErrors
                writeErrors, err := parseFloat(record[10])
                if err != nil {
		    	level.Error(logger).Log("msg", "Error parsing write_errors", "value", record[10], "record", strings.Join(record, ","), "err", err)
                    return nil, err
                }
                metric.write_errors = writeErrors
                switch strings.ToUpper(record[11]) {
                case "YES":
                    metric.error_state = 1
                default:
                    metric.error_state = 0
                }
                metrics = append(metrics, metric)
        }
        return metrics, nil
}
