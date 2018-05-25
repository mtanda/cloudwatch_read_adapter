package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/prometheus/prompb"
	"golang.org/x/sync/errgroup"
)

const (
	PROMETHEUS_MAXIMUM_POINTS = 11000
)

type config struct {
	listenAddr  string
	configFile  string
	storagePath string
}

func runQuery(indexer *Indexer, archiver *Archiver, q *prompb.Query, lookbackDelta time.Duration, logger log.Logger) []*prompb.TimeSeries {
	result := make(resultMap)

	namespace := ""
	debugMode := false
	originalJobLabel := ""
	matchers := make([]*prompb.LabelMatcher, 0)
	for _, m := range q.Matchers {
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "job" {
			originalJobLabel = m.Value
			continue
		}
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "debug" {
			debugMode = true
			continue
		}
		if m.Type == prompb.LabelMatcher_EQ && m.Name == "Namespace" {
			namespace = m.Value
		}
		matchers = append(matchers, m)
	}
	q.Matchers = matchers
	if namespace == "" {
		level.Debug(logger).Log("msg", "namespace is required")
		return result.slice()
	}

	// workaround, align query range to Prometheus original query range
	lookbackDeltaMs := lookbackDelta.Nanoseconds() / 1000 / 1000
	if (q.EndTimestampMs - q.StartTimestampMs) > lookbackDeltaMs*2 {
		q.StartTimestampMs += lookbackDeltaMs
	}
	startTime := time.Unix(int64(q.StartTimestampMs/1000), int64(q.StartTimestampMs%1000*1000))
	endTime := time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000))
	now := time.Now().UTC()
	if endTime.After(now) {
		q.EndTimestampMs = now.Unix() * 1000
		endTime = time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000))
	}
	queryRangeSec := endTime.Unix() - startTime.Unix()

	// get archived result
	if q.StartTimestampMs < q.EndTimestampMs && archiver.isArchived(startTime, []string{namespace}) {
		if archiver.isExpired(startTime) && !indexer.isExpired(startTime, []string{namespace}) {
			expiredTime := time.Now().Add(-archiver.retention)
			if endTime.Before(expiredTime) {
				expiredTime = endTime
			}
			baq := *q
			baq.EndTimestampMs = expiredTime.Unix() * 1000
			q.StartTimestampMs = baq.EndTimestampMs + 1000
			level.Info(logger).Log("msg", "querying for CloudWatch with index before archived period", "query", fmt.Sprintf("%+v", baq))
			region, queries, err := getQueryWithIndex(&baq, indexer, calcMaximumStep(queryRangeSec))
			if err != nil {
				level.Error(logger).Log("err", err)
				return result.slice()
			}
			err = queryCloudWatch(region, queries, q, lookbackDelta, result)
			if err != nil {
				level.Error(logger).Log("err", err)
				return result.slice()
			}
		}
		if q.StartTimestampMs < q.EndTimestampMs {
			level.Info(logger).Log("msg", "querying for archive", "query", fmt.Sprintf("%+v", q))
			aq := *q
			if aq.EndTimestampMs > archiver.s.Timestamp[namespace]*1000+1000 {
				aq.EndTimestampMs = archiver.s.Timestamp[namespace]*1000 + 1000 // add 1 second
			}
			archivedResult, err := archiver.query(&aq, calcMaximumStep(queryRangeSec))
			if err != nil {
				level.Error(logger).Log("err", err)
				return result.slice()
			}
			if debugMode {
				level.Info(logger).Log("msg", "dump archive query result", "result", fmt.Sprintf("%+v", archivedResult))
			}
			result.append(archivedResult)
			q.StartTimestampMs = aq.EndTimestampMs
			level.Info(logger).Log("msg", fmt.Sprintf("Get %d time series from archive.", len(result)))
		}
	}

	// parse query
	if q.StartTimestampMs < q.EndTimestampMs {
		var region string
		var queries []*cloudwatch.GetMetricStatisticsInput
		var err error
		if indexer.isExpired(endTime, []string{namespace}) {
			level.Info(logger).Log("msg", "querying for CloudWatch without index", "query", fmt.Sprintf("%+v", q))
			region, queries, err = getQueryWithoutIndex(q, indexer, calcMaximumStep(queryRangeSec))
			if err != nil {
				level.Error(logger).Log("err", err)
				return result.slice()
			}
		} else {
			level.Info(logger).Log("msg", "querying for CloudWatch with index", "query", fmt.Sprintf("%+v", q))
			region, queries, err = getQueryWithIndex(q, indexer, calcMaximumStep(queryRangeSec))
			if err != nil {
				level.Error(logger).Log("err", err)
				return result.slice()
			}
		}
		err = queryCloudWatch(region, queries, q, lookbackDelta, result)
		if err != nil {
			level.Error(logger).Log("err", err)
			return result.slice()
		}
	}

	if originalJobLabel != "" {
		for _, ts := range result {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: "job", Value: originalJobLabel})
		}
	}

	level.Info(logger).Log("msg", fmt.Sprintf("Returned %d time series.", len(result)))

	return result.slice()
}

func GetDefaultRegion() (string, error) {
	var region string

	metadata := ec2metadata.New(session.New(), &aws.Config{
		MaxRetries: aws.Int(0),
	})
	if metadata.Available() {
		var err error
		region, err = metadata.Region()
		if err != nil {
			return "", err
		}
	} else {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
	}

	return region, nil
}

func main() {
	var cfg config

	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9415", "Address to listen on for web endpoints.")
	flag.StringVar(&cfg.configFile, "config.file", "./cloudwatch_read_adapter.yml", "Configuration file path.")
	flag.StringVar(&cfg.storagePath, "storage.tsdb.path", "./data", "Base path for metrics storage.")
	flag.Parse()

	logLevel := promlog.AllowedLevel{}
	logLevel.Set("info")
	logger := promlog.New(logLevel)

	readCfg, err := LoadConfig(cfg.configFile)
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	if len(readCfg.Targets) == 0 {
		level.Info(logger).Log("msg", "no targets")
		os.Exit(0)
	}

	// set default region
	region, err := GetDefaultRegion()
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	if len(readCfg.Targets[0].Index.Region) == 0 {
		readCfg.Targets[0].Index.Region = append(readCfg.Targets[0].Index.Region, region)
	}
	if len(readCfg.Targets[0].Archive.Region) == 0 {
		readCfg.Targets[0].Archive.Region = append(readCfg.Targets[0].Archive.Region, region)
	}

	for _, n := range readCfg.Targets[0].Archive.Namespace {
		found := false
		for _, nn := range readCfg.Targets[0].Index.Namespace {
			if n == nn {
				found = true
			}
		}
		if !found {
			err := "archive target namespace should be indexed"
			level.Error(logger).Log("err", err)
			panic(err)
		}
	}

	pctx, cancel := context.WithCancel(context.Background())
	eg, ctx := errgroup.WithContext(pctx)
	indexer, err := NewIndexer(readCfg.Targets[0].Index, cfg.storagePath, log.With(logger, "component", "indexer"))
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	indexer.start(eg, ctx)
	archiver, err := NewArchiver(readCfg.Targets[0].Archive, cfg.storagePath, indexer, log.With(logger, "component", "archiver"))
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	archiver.start(eg, ctx)

	srv := &http.Server{Addr: cfg.listenAddr}
	http.Handle("/metrics", prometheus.Handler())
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(req.Queries) != 1 {
			http.Error(w, "Can only handle one query.", http.StatusBadRequest)
			return
		}

		resp := prompb.ReadResponse{
			Results: []*prompb.QueryResult{
				{Timeseries: runQuery(indexer, archiver, req.Queries[0], readCfg.LookbackDelta, logger)},
			},
		}
		data, err := proto.Marshal(&resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		if _, err := w.Write(snappy.Encode(nil, data)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	})

	term := make(chan os.Signal, 1)
	signal.Notify(term, os.Interrupt, syscall.SIGTERM)
	defer func() {
		signal.Stop(term)
		cancel()
	}()
	go func() {
		select {
		case <-term:
			level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
			cancel()
			if err := eg.Wait(); err != nil {
				level.Error(logger).Log("err", err)
			}

			ctxHttp, _ := context.WithTimeout(context.Background(), 60*time.Second)
			if err := srv.Shutdown(ctxHttp); err != nil {
				level.Error(logger).Log("err", err)
			}
		case <-pctx.Done():
		}
	}()

	level.Info(logger).Log("msg", "Listening on "+cfg.listenAddr)
	if err := srv.ListenAndServe(); err != nil {
		level.Error(logger).Log("err", err)
	}
}
