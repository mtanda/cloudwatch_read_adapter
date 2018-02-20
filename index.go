package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
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
)

func init() {
	prometheus.MustRegister(indexerTargetsProgress)
	prometheus.MustRegister(indexerTargetsTotal)
}

type Indexer struct {
	cloudwatch           *cloudwatch.CloudWatch
	db                   *tsdb.DB
	region               string
	namespace            []string
	interval             time.Duration
	indexedTimestampFrom time.Time
	s                    *IndexerState
	storagePath          string
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

	db, err := tsdb.Open(
		storagePath+"/index",
		logger,
		prometheus.DefaultRegisterer,
		&tsdb.Options{
			WALFlushInterval:  10 * time.Second,
			RetentionDuration: uint64(retention),
			BlockRanges: []int64{
				2 * 60 * 60 * 1000,
				6 * 60 * 60 * 1000,
				24 * 60 * 60 * 1000,
				72 * 60 * 60 * 1000,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	s := &IndexerState{
		TimestampTo: make(map[string]int64),
	}

	return &Indexer{
		cloudwatch:           cloudwatch,
		db:                   db,
		region:               cfg.Region[0],
		namespace:            cfg.Namespace,
		interval:             time.Duration(10) * time.Minute,
		indexedTimestampFrom: time.Unix(0, 0),
		s:                    s,
		storagePath:          storagePath,
		logger:               logger,
	}, nil
}

func (indexer *Indexer) start(eg *errgroup.Group, ctx context.Context) {
	level.Info(indexer.logger).Log("msg", fmt.Sprintf("index region = %s", indexer.region))
	level.Info(indexer.logger).Log("msg", fmt.Sprintf("index namespace = %+v", indexer.namespace))
	indexer.indexedTimestampFrom = time.Now().UTC()
	if state, err := indexer.loadState(); err == nil {
		indexer.s = state
		level.Info(indexer.logger).Log("msg", "state loaded", "timestamp", indexer.s.TimestampTo)
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
						cloudwatchApiCalls.WithLabelValues("ListMetrics", "success").Add(float64(1))
						return !lastPage
					})
				if err != nil {
					cloudwatchApiCalls.WithLabelValues("ListMetrics", "error").Add(float64(1))
					level.Error(indexer.logger).Log("err", err)
					continue // ignore temporary error
				}

				app := indexer.db.Appender()
				indexerTargetsTotal.WithLabelValues(namespace).Set(float64(len(resp.Metrics)))
				for _, metric := range resp.Metrics {
					l := make(labels.Labels, 0)
					l = append(l, labels.Label{Name: "Region", Value: indexer.region})
					l = append(l, labels.Label{Name: "Namespace", Value: *metric.Namespace})
					l = append(l, labels.Label{Name: "__name__", Value: *metric.MetricName})
					for _, dimension := range metric.Dimensions {
						l = append(l, labels.Label{Name: *dimension.Name, Value: *dimension.Value})
					}
					ref, err := app.Add(l, now.Unix()*1000, 0.0)
					if err != nil {
						level.Error(indexer.logger).Log("err", err)
						return err
					}
					_ = ref
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

				indexerTargetsProgress.WithLabelValues(namespace).Set(float64(len(resp.Metrics)))
			}

			level.Info(indexer.logger).Log("msg", "indexing completed")
		case <-ctx.Done():
			indexer.db.Close()
			level.Info(indexer.logger).Log("msg", "indexing stopped")
			return nil
		}
	}
}

func (indexer *Indexer) getMatchedLables(matchers []labels.Matcher, start int64, end int64) ([]labels.Labels, error) {
	matchedLabels := make([]labels.Labels, 0)

	querier, err := indexer.db.Querier(start, end)
	if err != nil {
		return nil, err
	}
	defer querier.Close()

	ss, err := querier.Select(matchers...)
	if err != nil {
		return nil, err
	}
	for ss.Next() {
		s := ss.At()
		matchedLabels = append(matchedLabels, s.Labels())
	}

	return matchedLabels, nil
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
	t = t.Add(-indexer.interval)
	for _, n := range namespace {
		if time.Unix(indexer.s.TimestampTo[n], 0).After(indexer.indexedTimestampFrom) && t.Before(indexer.indexedTimestampFrom) {
			t = indexer.indexedTimestampFrom
		}
	}
	return !indexer.isIndexed(t, namespace)
}
