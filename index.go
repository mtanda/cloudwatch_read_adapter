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
)

type Indexer struct {
	ctx                  context.Context
	region               *string
	cloudwatch           *cloudwatch.CloudWatch
	db                   *tsdb.DB
	namespace            []*string
	interval             time.Duration
	indexedTimestampFrom time.Time // TODO: save this status on file
	indexedTimestampTo   time.Time
	storagePath          string
	logger               log.Logger
}

func NewIndexer(ctx context.Context, cfg IndexConfig, storagePath string, logger log.Logger) (*Indexer, error) {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	retention, err := model.ParseDuration(cfg.Retention)
	if err != nil {
		return nil, err
	}

	awsCfg := &aws.Config{Region: cfg.Region[0]}
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

	return &Indexer{
		ctx:                  ctx,
		region:               cfg.Region[0],
		cloudwatch:           cloudwatch,
		db:                   db,
		namespace:            cfg.Namespace,
		interval:             time.Duration(10) * time.Minute,
		indexedTimestampFrom: time.Unix(0, 0),
		indexedTimestampTo:   time.Unix(0, 0),
		storagePath:          storagePath,
		logger:               logger,
	}, nil
}

func (indexer *Indexer) start() {
	level.Info(indexer.logger).Log("msg", fmt.Sprintf("index region = %s", *indexer.region))
	level.Info(indexer.logger).Log("msg", fmt.Sprintf("index namespace = %+v", indexer.namespace))
	indexer.indexedTimestampFrom = time.Now().UTC()
	state, err := indexer.loadState()
	if err == nil {
		indexer.indexedTimestampTo = time.Unix(state.Timestamp, 0)
	}
	go indexer.index()
}

func (indexer *Indexer) index() {
	t := time.NewTimer(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			t.Reset(indexer.interval)

			level.Info(indexer.logger).Log("msg", "indexing start")

			now := time.Now().UTC()
			for _, namespace := range indexer.namespace {
				indexerTargetsProgress.WithLabelValues(*namespace).Set(float64(0))
				indexerTargetsTotal.WithLabelValues(*namespace).Set(float64(0))
			}
			for _, namespace := range indexer.namespace {
				level.Info(indexer.logger).Log("msg", fmt.Sprintf("indexing namespace = %s", *namespace))

				var resp cloudwatch.ListMetricsOutput
				err := indexer.cloudwatch.ListMetricsPages(&cloudwatch.ListMetricsInput{
					Namespace: namespace,
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
					continue
				}

				app := indexer.db.Appender()
				indexerTargetsTotal.WithLabelValues(*namespace).Set(float64(len(resp.Metrics)))
				for _, metric := range resp.Metrics {
					l := make(labels.Labels, 0)
					l = append(l, labels.Label{Name: "Region", Value: *indexer.region})
					l = append(l, labels.Label{Name: "Namespace", Value: *metric.Namespace})
					l = append(l, labels.Label{Name: "__name__", Value: *metric.MetricName})
					for _, dimension := range metric.Dimensions {
						l = append(l, labels.Label{Name: *dimension.Name, Value: *dimension.Value})
					}
					ref, err := app.Add(l, now.Unix()*1000, 0.0)
					if err != nil {
						level.Error(indexer.logger).Log("err", err)
						panic(err)
					}
					_ = ref
				}

				if err := app.Commit(); err != nil {
					level.Error(indexer.logger).Log("err", err)
					panic(err)
				}
				indexerTargetsProgress.WithLabelValues(*namespace).Set(float64(len(resp.Metrics)))
			}

			indexer.indexedTimestampTo = now
			if err := indexer.saveState(indexer.indexedTimestampTo.Unix()); err != nil {
				level.Error(indexer.logger).Log("err", err)
				panic(err)
			}
			level.Info(indexer.logger).Log("msg", "indexing completed")
		case <-indexer.ctx.Done():
			level.Info(indexer.logger).Log("msg", "indexing stopped")
			indexer.db.Close()
			return
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

	ss := querier.Select(matchers...)
	for ss.Next() {
		s := ss.At()
		matchedLabels = append(matchedLabels, s.Labels())
	}

	return matchedLabels, nil
}

type IndexerState struct {
	Timestamp int64 `json:"timestamp"`
}

func (indexer *Indexer) saveState(timestamp int64) error {
	state := IndexerState{
		Timestamp: timestamp,
	}
	buf, err := json.Marshal(state)
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

func (indexer *Indexer) canIndex(t time.Time) bool {
	return indexer.indexedTimestampFrom.Before(t)
}

func (indexer *Indexer) isIndexed(t time.Time, namespace []*string) bool {
	if t.Before(indexer.indexedTimestampFrom) || t.After(indexer.indexedTimestampTo) {
		return false
	}
	found := false
	for _, n := range indexer.namespace {
		for _, nn := range namespace {
			if *n == *nn {
				found = true
			}
		}
	}
	return found
}

func (indexer *Indexer) isExpired(t time.Time, namespace []*string) bool {
	t = t.Add(-indexer.interval)
	if t.Before(indexer.indexedTimestampFrom) {
		t = indexer.indexedTimestampFrom
	}
	return !indexer.isIndexed(t, namespace)
}