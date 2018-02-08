package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
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

var (
	cloudwatchApiCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cloudwatch_read_adapter_cloudwatch_api_calls_total",
			Help: "The total number of CloudWatch API calls",
		},
		[]string{"api", "status"},
	)
	indexerTargetsProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cloudwatch_read_adapter_indexer_targets_progress",
			Help: "The progress of indexer",
		},
		[]string{"namespace"},
	)
	indexerTargetsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cloudwatch_read_adapter_indexer_targets_total",
			Help: "The total number of index target",
		},
		[]string{"namespace"},
	)
	archiverTargetsProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cloudwatch_read_adapter_archiver_targets_progress",
			Help: "The progress of archiver",
		},
		[]string{"namespace"},
	)
	archiverTargetsTotal = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cloudwatch_read_adapter_archiver_targets_total",
			Help: "The total number of archive target",
		},
		[]string{"namespace"},
	)
)

func init() {
	prometheus.MustRegister(cloudwatchApiCalls)
	prometheus.MustRegister(indexerTargetsProgress)
	prometheus.MustRegister(indexerTargetsTotal)
	prometheus.MustRegister(archiverTargetsProgress)
	prometheus.MustRegister(archiverTargetsTotal)
}

type config struct {
	listenAddr  string
	configFile  string
	storagePath string
}

func runQuery(indexer *Indexer, archiver *Archiver, q *prompb.Query, logger log.Logger) []*prompb.TimeSeries {
	result := []*prompb.TimeSeries{}

	// TODO: improve this
	namespace := ""
	for _, m := range q.Matchers {
		if m.Type != prompb.LabelMatcher_EQ {
			continue
		}
		switch m.Name {
		case "Namespace":
			namespace = m.Value
		}
	}
	if namespace == "" {
		level.Debug(logger).Log("msg", "namespace is required")
		return result
	}

	startTime := time.Unix(int64(q.StartTimestampMs/1000), int64(q.StartTimestampMs%1000*1000))
	endTime := time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000))

	// get archived result
	if archiver.isArchived(startTime) {
		level.Info(logger).Log("msg", "querying for archive", "query", fmt.Sprintf("%+v", q))
		aq := *q
		aq.EndTimestampMs = archiver.archivedTimestamp.UnixNano() / 1000 / 1000
		archivedResult, err := archiver.query(&aq)
		if err != nil {
			level.Error(logger).Log("err", err)
			return result
		}
		result = append(result, archivedResult...)
		q.StartTimestampMs = aq.EndTimestampMs
	}

	// parse query
	var region string
	var queries []*cloudwatch.GetMetricStatisticsInput
	var err error
	if indexer.isExpired(endTime, []*string{&namespace}) {
		level.Info(logger).Log("msg", "querying for CloudWatch without index", "query", fmt.Sprintf("%+v", q))
		region, queries, err = getQueryWithoutIndex(q, indexer)
		if err != nil {
			level.Error(logger).Log("err", err)
			return result
		}
	} else {
		level.Info(logger).Log("msg", "querying for CloudWatch with index", "query", fmt.Sprintf("%+v", q))
		region, queries, err = getQueryWithIndex(q, indexer)
		if err != nil {
			level.Error(logger).Log("err", err)
			return result
		}
	}

	if len(queries) > 100 {
		level.Warn(logger).Log("msg", "Too many concurrent queries")
		return result
	}

	for _, query := range queries {
		cwResult, err := queryCloudWatch(region, query, q)
		if err != nil {
			level.Error(logger).Log("err", err)
			return result
		}
		result = append(result, cwResult...)
	}

	level.Info(logger).Log("msg", fmt.Sprintf("Returned %d time series.", len(result)))

	return result
}

func getQueryWithoutIndex(q *prompb.Query, indexer *Indexer) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	region := ""
	queries := make([]*cloudwatch.GetMetricStatisticsInput, 0)

	query := &cloudwatch.GetMetricStatisticsInput{}
	for _, m := range q.Matchers {
		if m.Type != prompb.LabelMatcher_EQ {
			continue // only support equal matcher
		}

		switch m.Name {
		case "Region":
			region = m.Value
		case "Namespace":
			query.Namespace = aws.String(m.Value)
		case "__name__":
			query.MetricName = aws.String(m.Value)
		case "Statistic":
			query.Statistics = []*string{aws.String(m.Value)}
		case "ExtendedStatistic":
			query.ExtendedStatistics = []*string{aws.String(m.Value)}
		case "Period":
			period, err := strconv.Atoi(m.Value)
			if err != nil {
				return region, queries, err
			}
			query.Period = aws.Int64(int64(period))
		default:
			query.Dimensions = append(query.Dimensions, &cloudwatch.Dimension{
				Name:  aws.String(m.Name),
				Value: aws.String(m.Value),
			})
		}
	}

	return region, queries, nil
}

func getQueryWithIndex(q *prompb.Query, indexer *Indexer) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	region := ""
	queries := make([]*cloudwatch.GetMetricStatisticsInput, 0)

	// TODO: improve this
	mm := make([]*prompb.LabelMatcher, 0)
	for _, m := range q.Matchers {
		if m.Name == "Statistic" || m.Name == "ExtendedStatistic" {
			continue
		}
		mm = append(mm, m)
	}

	matchers, err := fromLabelMatchers(mm)
	if err != nil {
		return region, queries, err
	}
	matchedLabelsList, err := indexer.getMatchedLables(matchers, q.StartTimestampMs, q.EndTimestampMs)
	if err != nil {
		return region, queries, err
	}

	for _, matchedLabels := range matchedLabelsList {
		query := &cloudwatch.GetMetricStatisticsInput{}
		for _, label := range matchedLabels {
			switch label.Name {
			case "Region":
				// TODO: support multiple region?
				region = label.Value
			case "Namespace":
				query.Namespace = aws.String(label.Value)
			case "__name__":
				query.MetricName = aws.String(label.Value)
			default:
				if query.Dimensions == nil {
					query.Dimensions = make([]*cloudwatch.Dimension, 0)
				}
				query.Dimensions = append(query.Dimensions, &cloudwatch.Dimension{
					Name:  aws.String(label.Name),
					Value: aws.String(label.Value),
				})
			}
		}
		// TODO: test this
		for _, m := range q.Matchers {
			if !(m.Type == prompb.LabelMatcher_EQ || m.Type == prompb.LabelMatcher_RE) {
				continue
			}
			statistics := make([]*string, 0)
			for _, s := range strings.Split(m.Value, "|") {
				statistics = append(statistics, aws.String(s))
			}
			switch m.Name {
			case "Statistic":
				query.Statistics = statistics
			case "ExtendedStatistic":
				query.ExtendedStatistics = statistics
			}
		}
		if len(query.Statistics) == 0 {
			query.Statistics = []*string{aws.String("Sum"), aws.String("SampleCount"), aws.String("Maximum"), aws.String("Minimum")}
			query.ExtendedStatistics = []*string{aws.String("p50.00"), aws.String("p90.00"), aws.String("p99.00")}
		}
		queries = append(queries, query)
	}

	return region, queries, nil
}

func queryCloudWatch(region string, query *cloudwatch.GetMetricStatisticsInput, q *prompb.Query) ([]*prompb.TimeSeries, error) {
	result := []*prompb.TimeSeries{}

	if query.Namespace == nil || query.MetricName == nil {
		return result, fmt.Errorf("missing parameter")
	}

	periodUnit := 60
	rangeAdjust := int64(0)
	if q.StartTimestampMs%int64(periodUnit*1000) != 0 {
		rangeAdjust = int64(1)
	}
	query.StartTime = aws.Time(time.Unix((q.StartTimestampMs/1000/int64(periodUnit)+rangeAdjust)*int64(periodUnit), 0))
	query.EndTime = aws.Time(time.Unix(q.EndTimestampMs/1000/int64(periodUnit)*int64(periodUnit), 0))

	// auto calibrate period
	period := 60
	timeDay := 24 * time.Hour
	if query.Period == nil {
		now := time.Now().UTC()
		timeRangeToNow := now.Sub(*query.StartTime)
		if timeRangeToNow < timeDay*15 { // until 15 days ago
			period = 60
		} else if timeRangeToNow <= (timeDay * 63) { // until 63 days ago
			period = 60 * 5
		} else if timeRangeToNow <= (timeDay * 455) { // until 455 days ago
			period = 60 * 60
		} else { // over 455 days, should return error, but try to long period
			period = 60 * 60
		}
	}
	queryTimeRange := (*query.EndTime).Sub(*query.StartTime).Seconds()
	if queryTimeRange/float64(period) >= 1440 {
		period = int(math.Ceil(queryTimeRange/float64(1440)/float64(periodUnit))) * periodUnit
	}
	query.Period = aws.Int64(int64(period))

	cfg := &aws.Config{Region: aws.String(region)}
	sess, err := session.NewSession(cfg)
	if err != nil {
		return result, err
	}

	svc := cloudwatch.New(sess, cfg)
	resp, err := svc.GetMetricStatistics(query)
	if err != nil {
		cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", "error").Add(float64(1))
		return result, err
	}
	cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", "success").Add(float64(1))

	// make time series
	paramStatistics := append(query.Statistics, query.ExtendedStatistics...)
	tsm := make(map[string]*prompb.TimeSeries)
	for _, s := range paramStatistics {
		ts := &prompb.TimeSeries{}
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "Region", Value: region})
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "Namespace", Value: *query.Namespace})
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "__name__", Value: *query.MetricName})
		for _, d := range query.Dimensions {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: *d.Name, Value: *d.Value})
		}
		if !isExtendedStatistics(s) {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: "Statistic", Value: *s})
		} else {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: "ExtendedStatistic", Value: *s})
		}
		tsm[*s] = ts
	}

	sort.Slice(resp.Datapoints, func(i, j int) bool {
		return resp.Datapoints[i].Timestamp.Before(*resp.Datapoints[j].Timestamp)
	})
	for _, dp := range resp.Datapoints {
		for _, s := range paramStatistics {
			value := 0.0
			switch *s {
			case "Sum":
				value = *dp.Sum
			case "SampleCount":
				value = *dp.SampleCount
			case "Maximum":
				value = *dp.Maximum
			case "Minimum":
				value = *dp.Minimum
			case "Average":
				value = *dp.Average
			default:
				if dp.ExtendedStatistics == nil {
					continue
				}
				value = *dp.ExtendedStatistics[*s]
			}
			ts := tsm[*s]
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: value, Timestamp: dp.Timestamp.Unix() * 1000})
		}
	}

	for _, ts := range tsm {
		result = append(result, ts)
	}

	return result, nil
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
	flag.StringVar(&cfg.listenAddr, "web.listen-address", ":9201", "Address to listen on for web endpoints.")
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

	// graceful shutdown
	eg := errgroup.Group{}
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// set default region
	region, err := GetDefaultRegion()
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	if len(readCfg.Targets[0].Index.Region) == 0 {
		readCfg.Targets[0].Index.Region = append(readCfg.Targets[0].Index.Region, &region)
	}
	if len(readCfg.Targets[0].Archive.Region) == 0 {
		readCfg.Targets[0].Archive.Region = append(readCfg.Targets[0].Archive.Region, &region)
	}

	indexer, err := NewIndexer(ctx, readCfg.Targets[0].Index, cfg.storagePath, log.With(logger, "component", "indexer"))
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	indexer.start(eg)
	archiver, err := NewArchiver(ctx, readCfg.Targets[0].Archive, cfg.storagePath, indexer, log.With(logger, "component", "archiver"))
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
	archiver.start(eg)

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
				{Timeseries: runQuery(indexer, archiver, req.Queries[0], logger)},
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
			os.Exit(0)
		case <-ctx.Done():
		}
	}()

	err = http.ListenAndServe(cfg.listenAddr, nil)
	if err != nil {
		level.Error(logger).Log("err", err)
		panic(err)
	}
}
