package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
)

type Archiver struct {
	ctx                   context.Context
	region                *string
	cloudwatch            *cloudwatch.CloudWatch
	db                    *tsdb.DB
	namespace             []*string
	statistics            []*string
	extendedStatistics    []*string
	interval              time.Duration
	indexer               *Indexer
	archivedTimestamp     time.Time
	currentNamespaceIndex int
	currentLabelIndex     int
	logger                log.Logger
}

func NewArchiver(ctx context.Context, cfg ArchiveConfig, storagePath string, indexer *Indexer, logger log.Logger) (*Archiver, error) {
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
		storagePath+"/archive",
		logger,
		prometheus.NewRegistry(), // TODO: check
		&tsdb.Options{
			WALFlushInterval:  10 * time.Second,
			RetentionDuration: uint64(retention),
			BlockRanges: []int64{
				24 * 60 * 60 * 1000,
				72 * 60 * 60 * 1000,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	return &Archiver{
		ctx:                   ctx,
		region:                cfg.Region[0],
		cloudwatch:            cloudwatch,
		db:                    db,
		namespace:             cfg.Namespace,
		statistics:            []*string{aws.String("Sum"), aws.String("SampleCount"), aws.String("Maximum"), aws.String("Minimum")}, // Sum, SampleCount, Maximum, Minimum, pNN.NN
		extendedStatistics:    []*string{aws.String("p50.00"), aws.String("p90.00"), aws.String("p99.00")},                           // TODO: add to config
		interval:              time.Duration(24) * time.Hour,
		indexer:               indexer,
		archivedTimestamp:     time.Unix(0, 0),
		currentNamespaceIndex: 0,
		currentLabelIndex:     0,
		logger:                logger,
	}, nil
}

func (archiver *Archiver) start() {
	level.Info(archiver.logger).Log("msg", fmt.Sprintf("archive region = %s", *archiver.region))
	level.Info(archiver.logger).Log("msg", fmt.Sprintf("archive namespace = %+v", archiver.namespace))
	state, err := archiver.loadState()
	if err == nil {
		archiver.archivedTimestamp = time.Unix(state.Timestamp, 0)
		archiver.currentNamespaceIndex = state.Namespace
		archiver.currentLabelIndex = state.Index
	}

	go archiver.archive()
}

func (archiver *Archiver) archive() {
	timeMargin := 15 * time.Minute // wait until CloudWatch record metrics
	//archiveTime := archiver.interval / 4

	t := time.NewTimer(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			t.Reset(archiver.interval)

			level.Info(archiver.logger).Log("msg", "archiving start")

			now := time.Now().UTC()
			endTime := now.Truncate(archiver.interval)
			if endTime.Add(timeMargin).After(now) {
				endTime = now.Add(-timeMargin)
			}
			startTime := endTime.Add(-archiver.interval)

			if !archiver.canArchive(endTime, now) {
				level.Info(archiver.logger).Log("msg", "not indexed yet, archiving canceled")
				t.Reset(time.Duration(1) * time.Minute)
				break
			}

			wg := &sync.WaitGroup{}
			app := archiver.db.Appender()
			ft := time.NewTicker(1 * time.Minute) // TODO: time.Sleep(archiveTime / time.Duration(len(matchedLabelsList)))
			wt := time.NewTicker(1 * time.Minute)
			wg.Add(1)
			go func() {
				level.Info(archiver.logger).Log("msg", fmt.Sprintf("archiving namespace = %s", *archiver.namespace[archiver.currentNamespaceIndex]))
				matchedLabelsList, err := archiver.getMatchedLabelsList(startTime, endTime)
				if err != nil {
					level.Error(archiver.logger).Log("err", err)
					return // TODO: retry?
				}

				for {
					select {
					case <-ft.C:
						matchedLabels := matchedLabelsList[archiver.currentLabelIndex]
						err = archiver.process(app, matchedLabels, startTime, endTime)
						if err != nil {
							level.Error(archiver.logger).Log("err", err)
						}

						archiver.currentLabelIndex++
						if archiver.currentLabelIndex == len(matchedLabelsList) {
							archiver.currentLabelIndex = 0
							archiver.currentNamespaceIndex++

							level.Info(archiver.logger).Log("msg", fmt.Sprintf("archiving namespace = %s", *archiver.namespace[archiver.currentNamespaceIndex]))
							matchedLabelsList, err = archiver.getMatchedLabelsList(startTime, endTime)
							if err != nil {
								level.Error(archiver.logger).Log("err", err)
								continue
							}

							if archiver.currentNamespaceIndex == len(archiver.namespace) {
								ft.Stop()
								wt.Stop()

								if err := app.Commit(); err != nil {
									level.Error(archiver.logger).Log("err", err)
									panic(err) // TODO: fix
								}
								archiver.saveState(archiver.archivedTimestamp.Unix(), archiver.currentNamespaceIndex, archiver.currentLabelIndex)
								level.Info(archiver.logger).Log("namespace", *archiver.namespace[archiver.currentNamespaceIndex], "index", archiver.currentLabelIndex, "len", len(matchedLabelsList))
								wg.Done()
							}
						}
					case <-wt.C:
						if err := app.Commit(); err != nil {
							level.Error(archiver.logger).Log("err", err)
							panic(err) // TODO: fix
						}
						namespace := archiver.namespace[archiver.currentNamespaceIndex]
						archiver.saveState(archiver.archivedTimestamp.Unix(), archiver.currentNamespaceIndex, archiver.currentLabelIndex)
						level.Info(archiver.logger).Log("namespace", *namespace, "index", archiver.currentLabelIndex, "len", len(matchedLabelsList))
					}
				}
			}()

			wg.Wait()
			archiver.archivedTimestamp = endTime
			archiver.saveState(archiver.archivedTimestamp.Unix(), 0, 0)
			level.Info(archiver.logger).Log("msg", "archiving completed")
		case <-archiver.ctx.Done():
			level.Info(archiver.logger).Log("msg", "archiving stopped")
			archiver.db.Close()
			return
		}
	}
}

func (archiver *Archiver) getMatchedLabelsList(startTime time.Time, endTime time.Time) ([]labels.Labels, error) {
	namespace := archiver.namespace[archiver.currentNamespaceIndex]
	matchers := []labels.Matcher{labels.NewEqualMatcher("Namespace", *namespace)}
	var matchedLabelsList []labels.Labels
	var err error
	if archiver.indexer.isIndexed(endTime, []*string{namespace}) {
		matchedLabelsList, err = archiver.indexer.getMatchedLables(matchers, startTime.Unix()*1000, endTime.Unix()*1000)
	} else {
		matchedLabelsList, err = archiver.indexer.getMatchedLables(matchers, startTime.Unix()*1000, archiver.indexer.indexedTimestampTo.Unix()*1000)
	}

	return matchedLabelsList, err
}

func (archiver *Archiver) process(app tsdb.Appender, _labels labels.Labels, startTime time.Time, endTime time.Time) error {
	timeAlignment := 60

	params := &cloudwatch.GetMetricStatisticsInput{}
	for _, label := range _labels {
		switch label.Name {
		case "Region":
			// ignore // TODO: support multiple region?
		case "Namespace":
			params.Namespace = aws.String(label.Value)
		case "__name__":
			params.MetricName = aws.String(label.Value)
		default:
			if params.Dimensions == nil {
				params.Dimensions = make([]*cloudwatch.Dimension, 0)
			}
			params.Dimensions = append(params.Dimensions, &cloudwatch.Dimension{
				Name:  aws.String(label.Name),
				Value: aws.String(label.Value),
			})
		}
	}
	params.Statistics = archiver.statistics
	params.ExtendedStatistics = archiver.extendedStatistics
	params.Period = aws.Int64(int64(timeAlignment))
	params.StartTime = aws.Time(startTime)
	params.EndTime = aws.Time(endTime)

	if params.Namespace == nil || params.MetricName == nil ||
		(params.Statistics == nil && params.ExtendedStatistics == nil) {
		return fmt.Errorf("missing parameter")
	}

	resp, err := archiver.cloudwatch.GetMetricStatistics(params)
	if err != nil {
		cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", "error").Add(float64(1))
		return err
	}
	cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", "success").Add(float64(1))

	sort.Slice(resp.Datapoints, func(i, j int) bool {
		return resp.Datapoints[i].Timestamp.Before(*resp.Datapoints[j].Timestamp)
	})

	paramStatistics := append(params.Statistics, params.ExtendedStatistics...)
	var ref uint64
	for _, dp := range resp.Datapoints {
		for _, s := range paramStatistics {
			// TODO: drop Average/Maximum/Minium in certain condition
			//if dp.SampleCount != nil && *dp.SampleCount == 1 && (*s == "Maximum" || *s == "Minimum") {
			//	continue // should be Maximum == Minimum == Average, drop
			//}

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

			l := make(labels.Labels, 0)
			l = append(l, labels.Label{Name: "Region", Value: *archiver.region})
			l = append(l, labels.Label{Name: "Namespace", Value: *params.Namespace})
			l = append(l, labels.Label{Name: "__name__", Value: *params.MetricName})
			for _, dimension := range params.Dimensions {
				l = append(l, labels.Label{Name: *dimension.Name, Value: *dimension.Value})
			}
			if !isExtendedStatistics(s) {
				l = append(l, labels.Label{Name: "Statistic", Value: *s})
			} else {
				l = append(l, labels.Label{Name: "ExtendedStatistic", Value: *s})
			}
			var errAdd error
			if ref != 0 {
				errAdd = app.AddFast(ref, dp.Timestamp.Unix()*1000, value)
			} else {
				ref, errAdd = app.Add(l, dp.Timestamp.Unix()*1000, value)
			}
			if errAdd != nil {
				level.Error(archiver.logger).Log("err", errAdd)
				continue
			}
		}
	}

	return nil
}

type State struct {
	Timestamp int64 `json:"timestamp"`
	Namespace int   `json:"namespace"`
	Index     int   `json:"index"`
	//Labels    string `json:"labels"`
}

func (archiver *Archiver) saveState(timestamp int64, namespace int, index int) error {
	state := State{
		Timestamp: timestamp,
		Namespace: namespace,
		Index:     index,
	}
	buf, err := json.Marshal(state)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("./data/archiver_state.json", buf, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (archiver *Archiver) loadState() (*State, error) {
	buf, err := ioutil.ReadFile("./data/archiver_state.json")
	if err != nil {
		return nil, err
	}

	var state State
	if err = json.Unmarshal(buf, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func (archiver *Archiver) query(q *prompb.Query) ([]*prompb.TimeSeries, error) {
	result := []*prompb.TimeSeries{}

	matchers, err := fromLabelMatchers(q.Matchers)
	if err != nil {
		return nil, err
	}

	querier, err := archiver.db.Querier(q.StartTimestampMs, q.EndTimestampMs)
	if err != nil {
		return nil, err
	}
	defer querier.Close()

	// TODO: generate Average result from Sum and SampleCount
	// TODO: generate Maximum/Minimum result from Average
	ss := querier.Select(matchers...)
	for ss.Next() {
		ts := &prompb.TimeSeries{}
		s := ss.At()

		labels := s.Labels()
		for _, label := range labels {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: label.Name, Value: label.Value})
		}

		it := s.Iterator()
		for it.Next() {
			t, v := it.At()
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: v, Timestamp: t})
		}

		result = append(result, ts)
	}

	return result, nil
}

func (archiver *Archiver) canArchive(endTime time.Time, now time.Time) bool {
	if !archiver.indexer.canIndex(endTime) && !archiver.indexer.isExpired(now, archiver.namespace) {
		return true
	}
	if !archiver.indexer.isIndexed(endTime, archiver.namespace) {
		return false
	}
	return true
}

func (archiver *Archiver) isArchived(t time.Time) bool {
	return t.Before(archiver.archivedTimestamp)
}
