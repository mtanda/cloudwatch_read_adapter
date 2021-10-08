package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	prom_value "github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"golang.org/x/sync/errgroup"
)

var (
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
	indexerLastSuccessTimestamp = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cloudwatch_read_adapter_indexer_last_success_timestamp_seconds",
			Help: "The last timestamp of indexing target",
		},
		[]string{"namespace"},
	)
	metricNameMap = make(map[string]prometheus.Gauge)
)

func init() {
	prometheus.MustRegister(indexerTargetsProgress)
	prometheus.MustRegister(indexerTargetsTotal)
	prometheus.MustRegister(indexerLastSuccessTimestamp)
}

type Indexer struct {
	cloudwatch           *cloudwatch.CloudWatch
	ec2                  *ec2.EC2
	dynamodb             *dynamodb.DynamoDB
	db                   *tsdb.DB
	region               string
	namespace            []string
	interval             time.Duration
	indexedTimestampFrom time.Time
	s                    *IndexerState
	storagePath          string
	registry             prometheus.Gatherer
	logger               log.Logger
}

func NewIndexer(cfg IndexConfig, storagePath string, logger log.Logger) (*Indexer, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	retention, err := model.ParseDuration(cfg.Retention)
	if err != nil {
		return nil, err
	}

	awsCfg := &aws.Config{Region: aws.String(cfg.Region[0])}
	sess, err := session.NewSession(awsCfg)
	if err != nil {
		return nil, err
	}
	cloudwatch := cloudwatch.New(sess, awsCfg)
	ec2 := ec2.New(sess, awsCfg)
	dynamodb := dynamodb.New(sess, awsCfg)

	registry := prometheus.NewRegistry()
	db, err := tsdb.Open(
		storagePath+"/index",
		logger,
		registry,
		&tsdb.Options{
			RetentionDuration: time.Duration(retention).Milliseconds(), // milliseconds
			MinBlockDuration:  (time.Duration(2) * time.Hour).Milliseconds(),
			MaxBlockDuration:  (time.Duration(72) * time.Hour).Milliseconds(),
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	s := &IndexerState{
		TimestampTo: make(map[string]int64),
	}

	return &Indexer{
		cloudwatch:           cloudwatch,
		ec2:                  ec2,
		dynamodb:             dynamodb,
		db:                   db,
		region:               cfg.Region[0],
		namespace:            cfg.Namespace,
		interval:             time.Duration(10) * time.Minute,
		indexedTimestampFrom: time.Unix(0, 0),
		s:                    s,
		storagePath:          storagePath,
		registry:             registry,
		logger:               logger,
	}, nil
}

func (indexer *Indexer) start(eg *errgroup.Group, ctx context.Context) {
	level.Info(indexer.logger).Log("msg", fmt.Sprintf("index region = %s", indexer.region))
	level.Info(indexer.logger).Log("msg", fmt.Sprintf("index namespace = %+v", indexer.namespace))
	indexer.indexedTimestampFrom = time.Now().UTC()
	if state, err := indexer.loadState(); err == nil {
		indexer.s = state
		level.Info(indexer.logger).Log("msg", "state loaded", "timestamp", fmt.Sprintf("%+v", indexer.s.TimestampTo))
	} else {
		level.Error(indexer.logger).Log("err", err)
	}

	(*eg).Go(func() error {
		return indexer.index(ctx)
	})
}

func (indexer *Indexer) index(ctx context.Context) error {
	t := time.NewTimer(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			t.Reset(indexer.interval)

			level.Info(indexer.logger).Log("msg", "indexing start")

			now := time.Now().UTC()
			for _, namespace := range indexer.namespace {
				indexerTargetsProgress.WithLabelValues(namespace).Set(float64(0))
				indexerTargetsTotal.WithLabelValues(namespace).Set(float64(0))
			}
			for _, namespace := range indexer.namespace {
				level.Info(indexer.logger).Log("msg", fmt.Sprintf("indexing namespace = %s", namespace))

				var resp cloudwatch.ListMetricsOutput
				err := indexer.cloudwatch.ListMetricsPages(&cloudwatch.ListMetricsInput{
					Namespace: aws.String(namespace),
				},
					func(page *cloudwatch.ListMetricsOutput, lastPage bool) bool {
						metrics, _ := awsutil.ValuesAtPath(page, "Metrics")
						for _, metric := range metrics {
							resp.Metrics = append(resp.Metrics, metric.(*cloudwatch.Metric))
						}
						cloudwatchApiCalls.WithLabelValues("ListMetrics", namespace, "index", "success").Add(float64(1))
						return !lastPage
					})
				if err != nil {
					cloudwatchApiCalls.WithLabelValues("ListMetrics", namespace, "index", "error").Add(float64(1))
					level.Error(indexer.logger).Log("err", err)
					continue // ignore temporary error
				}

				app := indexer.db.Appender(ctx)
				metrics, err := indexer.filterOldMetrics(namespace, resp.Metrics)
				if err != nil {
					continue // ignore temporary error
				}
				indexerTargetsTotal.WithLabelValues(namespace).Set(float64(len(metrics)))
				for _, metric := range metrics {
					l := make(labels.Labels, 0)
					l = append(l, labels.Label{Name: "Region", Value: indexer.region})
					l = append(l, labels.Label{Name: "Namespace", Value: *metric.Namespace})
					l = append(l, labels.Label{Name: "MetricName", Value: *metric.MetricName})
					l = append(l, labels.Label{Name: "__name__", Value: SafeMetricName(*metric.MetricName)})
					for _, dimension := range metric.Dimensions {
						l = append(l, labels.Label{Name: *dimension.Name, Value: *dimension.Value})
					}
					ref, err := app.Append(0, l, now.Unix()*1000, 0.0)
					if err != nil {
						level.Error(indexer.logger).Log("err", err)
						return err
					}
					_ = ref

					if _, ok := metricNameMap[*metric.MetricName]; !ok {
						metricNameMap[*metric.MetricName] = prometheus.NewGauge(
							prometheus.GaugeOpts{
								Name: SafeMetricName(*metric.MetricName),
								Help: *metric.MetricName,
							},
						)
						prometheus.MustRegister(metricNameMap[*metric.MetricName])
						metricNameMap[*metric.MetricName].Set(float64(0))
					}
				}

				if err := app.Commit(); err != nil {
					level.Error(indexer.logger).Log("err", err)
					return err
				}

				indexer.s.TimestampTo[namespace] = now.Unix()
				if err := indexer.saveState(); err != nil {
					level.Error(indexer.logger).Log("err", err)
					return err
				}

				indexerTargetsProgress.WithLabelValues(namespace).Set(float64(len(metrics)))
				indexerLastSuccessTimestamp.WithLabelValues(namespace).Set(float64(now.Unix()))
			}

			level.Info(indexer.logger).Log("msg", "indexing completed")
		case <-ctx.Done():
			indexer.db.Close()
			level.Info(indexer.logger).Log("msg", "indexing stopped")
			return nil
		}
	}
}

func (indexer *Indexer) getMatchedLabels(ctx context.Context, matchers []*labels.Matcher, start int64, end int64) ([]labels.Labels, error) {
	matchedLabels := make([]labels.Labels, 0)
	dupCheck := make(map[string]bool)

	querier, err := indexer.db.Querier(start, end)
	if err != nil {
		return nil, err
	}
	defer querier.Close()

	dimensions := make(map[string]bool)
	for _, matcher := range matchers {
		name := matcher.Name
		if name == "Region" || name == "Namespace" || name == "MetricName" || name == "__name__" {
			continue
		}
		dimensions[name] = true
	}

	ss := querier.Select(ctx, false, nil, matchers...)
	for ss.Next() {
		s := ss.At()

		_labels := s.Labels()
		sort.Slice(_labels, func(i, j int) bool {
			return _labels[i].Name < _labels[j].Name
		})

		// filter labels which has extra dimensions
		if len(dimensions) != 0 {
			hasExtraDimensions := false
			for _, label := range _labels {
				name := label.Name
				if name == "Region" || name == "Namespace" || name == "MetricName" || name == "__name__" {
					continue
				}
				if _, ok := dimensions[name]; !ok {
					hasExtraDimensions = true
				}
			}
			if hasExtraDimensions {
				continue
			}
		}
		id := ""
		hasMetricName := false
		for _, label := range _labels {
			id = id + label.Name + label.Value
			if label.Name == "MetricName" {
				hasMetricName = true
			}
		}
		if !hasMetricName {
			continue
		}

		if _, ok := dupCheck[id]; !ok {
			matchedLabels = append(matchedLabels, _labels)
			dupCheck[id] = true
		}
	}

	return matchedLabels, nil
}

func (indexer *Indexer) Query(ctx context.Context, q *prompb.Query, maximumStep int64, lookbackDelta time.Duration) (resultMap, error) {
	result := make(resultMap)

	querier, err := indexer.db.Querier(q.Hints.StartMs, q.Hints.EndMs)
	if err != nil {
		return nil, err
	}
	defer querier.Close()

	step := maximumStep

	matchers, err := fromLabelMatchers(q.Matchers)
	if err != nil {
		return nil, err
	}

	ss := querier.Select(ctx, false, nil, matchers...)
	for ss.Next() {
		ts := &prompb.TimeSeries{}
		s := ss.At()

		labels := s.Labels()
		sort.Slice(labels, func(i, j int) bool {
			return labels[i].Name < labels[j].Name
		})
		id := ""
		for _, label := range labels {
			if label.Name == "MetricName" {
				continue
			}
			ts.Labels = append(ts.Labels, prompb.Label{Name: label.Name, Value: label.Value})
			id = id + label.Name + label.Value
		}

		lastTimestamp := q.Hints.StartMs
		it := s.Iterator(nil)
		refTime := q.Hints.StartMs
		for it.Next() != chunkenc.ValNone && refTime <= q.Hints.EndMs {
			t, v := it.At()
			for refTime < lastTimestamp && step > 0 { // for safety, check step
				refTime += (step * 1000)
			}
			if step <= int64(lookbackDelta.Seconds()) && step > 60 && (t-lastTimestamp) > (step*1000) {
				ts.Samples = append(ts.Samples, prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: lastTimestamp + (step * 1000)})
			}
			if (t > refTime) && (lastTimestamp > (refTime - (step * 1000))) {
				ts.Samples = append(ts.Samples, prompb.Sample{Value: v, Timestamp: t})
			}
			lastTimestamp = t
		}
		if step <= int64(lookbackDelta.Seconds()) && step > 60 && (q.Hints.EndMs > lastTimestamp) && (lastTimestamp <= (q.Hints.EndMs - (step * 1000))) {
			ts.Samples = append(ts.Samples, prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: lastTimestamp + (step * 1000)})
		}

		if _, ok := result[id]; ok {
			result[id].Samples = append(result[id].Samples, ts.Samples...)
		} else {
			result[id] = ts
		}
	}

	// sort by timestamp
	for _, ts := range result {
		sort.Slice(ts.Samples, func(i, j int) bool {
			return ts.Samples[i].Timestamp < ts.Samples[j].Timestamp
		})
	}

	return result, nil
}

type IndexerState struct {
	TimestampTo map[string]int64 `json:"timestampTo"`
}

func (indexer *Indexer) saveState() error {
	buf, err := json.Marshal(indexer.s)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(indexer.storagePath+"/indexer_state.json", buf, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (indexer *Indexer) loadState() (*IndexerState, error) {
	buf, err := ioutil.ReadFile(indexer.storagePath + "/indexer_state.json")
	if err != nil {
		return nil, err
	}

	var state IndexerState
	if err := json.Unmarshal(buf, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func (indexer *Indexer) canIndex(t time.Time, namespace []string) bool {
	result := true
	for _, n := range namespace {
		result = result && (time.Unix(indexer.s.TimestampTo[n], 0).Before(t) || indexer.indexedTimestampFrom.Before(t))
	}
	return result
}

func (indexer *Indexer) isIndexed(t time.Time, namespace []string) bool {
	for _, n := range namespace {
		if _, ok := indexer.s.TimestampTo[n]; !ok {
			return false
		}
		if t.After(time.Unix(indexer.s.TimestampTo[n], 0)) || (time.Unix(indexer.s.TimestampTo[n], 0).After(indexer.indexedTimestampFrom) && t.Before(indexer.indexedTimestampFrom)) {
			return false
		}
	}
	return true
}

func (indexer *Indexer) isExpired(t time.Time, namespace []string) bool {
	t = t.Add(-indexer.interval - 60*time.Second)
	for _, n := range namespace {
		if time.Unix(indexer.s.TimestampTo[n], 0).After(indexer.indexedTimestampFrom) && t.Before(indexer.indexedTimestampFrom) {
			t = indexer.indexedTimestampFrom
		}
	}
	return !indexer.isIndexed(t, namespace)
}

func (indexer *Indexer) filterOldMetrics(namespace string, metrics []*cloudwatch.Metric) ([]*cloudwatch.Metric, error) {
	filteredMetrics := make([]*cloudwatch.Metric, 0)
	filterMap := make(map[string]bool)

	switch namespace {
	case "AWS/EC2":
		err := indexer.ec2.DescribeInstancesPages(&ec2.DescribeInstancesInput{},
			func(page *ec2.DescribeInstancesOutput, lastPage bool) bool {
				for _, r := range page.Reservations {
					for _, i := range r.Instances {
						if *i.State.Name == "running" || *i.State.Name == "shutting-down" || *i.State.Name == "stopping" {
							filterMap[*i.InstanceId] = true
						} else {
							if (*i.LaunchTime).After(time.Now().UTC().Add(-indexer.interval * 3)) {
								filterMap[*i.InstanceId] = true
							}
						}
					}
				}
				return !lastPage
			})
		if err != nil {
			return nil, err
		}
		for _, metric := range metrics {
			leave := true
			for _, dimension := range metric.Dimensions {
				if *dimension.Name == "InstanceId" {
					_, leave = filterMap[*dimension.Value]
				}
			}
			if leave {
				filteredMetrics = append(filteredMetrics, metric)
			}
		}
	case "AWS/EBS":
		ctx := context.Background()
		err := indexer.ec2.DescribeVolumesPagesWithContext(ctx, &ec2.DescribeVolumesInput{},
			func(page *ec2.DescribeVolumesOutput, lastPage bool) bool {
				for _, v := range page.Volumes {
					if *v.State == "in-use" || *v.State == "deleting" {
						filterMap[*v.VolumeId] = true
					} else {
						if (*v.CreateTime).After(time.Now().UTC().Add(-indexer.interval * 3)) {
							filterMap[*v.VolumeId] = true
						}
					}
				}
				return !lastPage
			})
		if err != nil {
			return nil, err
		}
		for _, metric := range metrics {
			leave := true
			for _, dimension := range metric.Dimensions {
				if *dimension.Name == "VolumeId" {
					_, leave = filterMap[*dimension.Value]
				}
			}
			if leave {
				filteredMetrics = append(filteredMetrics, metric)
			}
		}
	case "AWS/DynamoDB":
		err := indexer.dynamodb.ListTablesPages(&dynamodb.ListTablesInput{},
			func(page *dynamodb.ListTablesOutput, lastPage bool) bool {
				for _, v := range page.TableNames {
					filterMap[*v] = true
				}
				return !lastPage
			})
		if err != nil {
			return nil, err
		}
		for _, metric := range metrics {
			leave := true
			for _, dimension := range metric.Dimensions {
				if *dimension.Name == "TableName" {
					_, leave = filterMap[*dimension.Value]
				}
			}
			if leave {
				filteredMetrics = append(filteredMetrics, metric)
			}
		}
	default:
		filteredMetrics = metrics
	}

	return filteredMetrics, nil
}
