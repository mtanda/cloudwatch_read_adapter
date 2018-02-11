package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
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
	"golang.org/x/sync/errgroup"
)

var (
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
	prometheus.MustRegister(archiverTargetsProgress)
	prometheus.MustRegister(archiverTargetsTotal)
}

type Archiver struct {
	cloudwatch            *cloudwatch.CloudWatch
	db                    *tsdb.DB
	indexer               *Indexer
	region                string
	namespace             []string
	statistics            []*string
	extendedStatistics    []*string
	interval              time.Duration
	archivedTimestamp     time.Time
	currentNamespaceIndex int
	currentLabelIndex     int
	storagePath           string
	logger                log.Logger
}

func NewArchiver(cfg ArchiveConfig, storagePath string, indexer *Indexer, logger log.Logger) (*Archiver, error) {
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
		cloudwatch:            cloudwatch,
		db:                    db,
		indexer:               indexer,
		region:                cfg.Region[0],
		namespace:             cfg.Namespace,
		statistics:            []*string{aws.String("Sum"), aws.String("SampleCount"), aws.String("Maximum"), aws.String("Minimum"), aws.String("Average")},
		extendedStatistics:    []*string{aws.String("p50.00"), aws.String("p90.00"), aws.String("p99.00")}, // TODO: add to config
		interval:              time.Duration(24) * time.Hour,
		archivedTimestamp:     time.Unix(0, 0),
		currentNamespaceIndex: 0,
		currentLabelIndex:     0,
		storagePath:           storagePath,
		logger:                logger,
	}, nil
}

func (archiver *Archiver) start(eg *errgroup.Group, ctx context.Context) {
	if len(archiver.namespace) == 0 {
		return
	}

	level.Info(archiver.logger).Log("msg", fmt.Sprintf("archive region = %s", archiver.region))
	level.Info(archiver.logger).Log("msg", fmt.Sprintf("archive namespace = %+v", archiver.namespace))
	state, err := archiver.loadState()
	if err == nil {
		archiver.archivedTimestamp = time.Unix(state.Timestamp, 0)
		archiver.currentNamespaceIndex = state.Namespace
		archiver.currentLabelIndex = state.Index
		level.Info(archiver.logger).Log("msg", "state loaded", "timestamp", archiver.archivedTimestamp, "namespace", archiver.namespace[archiver.currentNamespaceIndex], "index", archiver.currentLabelIndex)
	}

	(*eg).Go(func() error {
		return archiver.archive(ctx)
	})
}

func (archiver *Archiver) archive(ctx context.Context) error {
	timeMargin := 15 * time.Minute // wait until CloudWatch record metrics
	apiCallRate := 0.5

	t := time.NewTimer(1 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			now := time.Now().UTC()
			endTime := now.Truncate(archiver.interval)
			startTime := endTime.Add(-archiver.interval)
			nextStartTime := endTime.Add(archiver.interval).Add(timeMargin)
			t.Reset(nextStartTime.Sub(now))

			if archiver.isArchived(endTime.Add(-1 * time.Second)) {
				level.Info(archiver.logger).Log("msg", "already archived")
				break
			}
			if endTime.Add(timeMargin).After(now) {
				t.Reset(endTime.Add(timeMargin).Sub(now))
				break
			}

			level.Info(archiver.logger).Log("msg", "archiving start")

			if !archiver.canArchive(endTime, now) {
				level.Info(archiver.logger).Log("msg", "not indexed yet, archiving canceled")
				t.Reset(time.Duration(1) * time.Minute)
				break
			}

			if archiver.currentNamespaceIndex == 0 && archiver.currentLabelIndex == 0 {
				for _, namespace := range archiver.namespace {
					archiverTargetsProgress.WithLabelValues(namespace).Set(float64(0))
					archiverTargetsTotal.WithLabelValues(namespace).Set(float64(0))
				}
			}

			cps := math.Floor(400 * apiCallRate) // support 400 transactions per second (TPS). https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
			ft := time.NewTimer(0)
			wt := time.NewTimer(0)
			wg := &sync.WaitGroup{}
			wg.Add(1)
			go func() {
				level.Info(archiver.logger).Log("msg", fmt.Sprintf("archiving namespace = %s", archiver.namespace[archiver.currentNamespaceIndex]))
				matchedLabelsList, err := archiver.getMatchedLabelsList(archiver.namespace[archiver.currentNamespaceIndex], startTime, endTime)
				if err != nil {
					level.Error(archiver.logger).Log("err", err)
					return // TODO: retry?
				}
				archiverTargetsTotal.WithLabelValues(archiver.namespace[archiver.currentNamespaceIndex]).Set(float64(len(matchedLabelsList)))

				app := archiver.db.Appender()
				for {
					select {
					case <-ft.C:
						ft.Reset(1 * time.Second / time.Duration(cps))

						matchedLabels := matchedLabelsList[archiver.currentLabelIndex]
						err = archiver.process(app, matchedLabels, startTime, endTime)
						if err != nil {
							level.Error(archiver.logger).Log("err", err)
						}

						archiver.currentLabelIndex++
						if archiver.currentLabelIndex == len(matchedLabelsList) {
							if archiver.currentNamespaceIndex == len(archiver.namespace)-1 {
								// archive finished
								if !ft.Stop() {
									<-ft.C
								}
								if !wt.Stop() {
									<-wt.C
								}

								if err := app.Commit(); err != nil {
									level.Error(archiver.logger).Log("err", err)
									panic(err) // TODO: fix
								}

								if err := archiver.saveState(archiver.archivedTimestamp.Unix(), archiver.currentNamespaceIndex, archiver.currentLabelIndex); err != nil {
									level.Error(archiver.logger).Log("err", err)
									panic(err)
								}
								archiverTargetsProgress.WithLabelValues(archiver.namespace[archiver.currentNamespaceIndex]).Set(float64(archiver.currentLabelIndex))
								level.Info(archiver.logger).Log("namespace", archiver.namespace[archiver.currentNamespaceIndex], "index", archiver.currentLabelIndex, "len", len(matchedLabelsList))

								// reset index for next archiving cycle
								archiver.currentLabelIndex = 0
								archiver.currentNamespaceIndex = 0

								wg.Done()
							} else {
								// archive next namespace
								archiver.currentLabelIndex = 0
								archiver.currentNamespaceIndex++

								level.Info(archiver.logger).Log("msg", fmt.Sprintf("archiving namespace = %s", archiver.namespace[archiver.currentNamespaceIndex]))
								matchedLabelsList, err = archiver.getMatchedLabelsList(archiver.namespace[archiver.currentNamespaceIndex], startTime, endTime)
								if err != nil {
									level.Error(archiver.logger).Log("err", err)
									//continue
									panic(err) // TODO: fix
								}
								archiverTargetsTotal.WithLabelValues(archiver.namespace[archiver.currentNamespaceIndex]).Set(float64(len(matchedLabelsList)))
							}
						}
					case <-wt.C:
						wt.Reset(1 * time.Minute)

						if err := app.Commit(); err != nil {
							level.Error(archiver.logger).Log("err", err)
							panic(err) // TODO: fix
						}
						namespace := archiver.namespace[archiver.currentNamespaceIndex]
						if err := archiver.saveState(archiver.archivedTimestamp.Unix(), archiver.currentNamespaceIndex, archiver.currentLabelIndex); err != nil {
							level.Error(archiver.logger).Log("err", err)
							panic(err)
						}
						archiverTargetsProgress.WithLabelValues(namespace).Set(float64(archiver.currentLabelIndex))
						level.Info(archiver.logger).Log("namespace", namespace, "index", archiver.currentLabelIndex, "len", len(matchedLabelsList))
					}
				}
			}()

			wg.Wait()
			archiver.archivedTimestamp = endTime.Add(-1 * time.Second)
			if err := archiver.saveState(archiver.archivedTimestamp.Unix(), 0, 0); err != nil {
				level.Error(archiver.logger).Log("err", err)
				panic(err)
			}
			level.Info(archiver.logger).Log("msg", "archiving completed")
		case <-ctx.Done():
			archiver.db.Close()
			level.Info(archiver.logger).Log("msg", "archiving stopped")
			return nil
		}
	}
}

func (archiver *Archiver) getMatchedLabelsList(namespace string, startTime time.Time, endTime time.Time) ([]labels.Labels, error) {
	matchers := []labels.Matcher{labels.NewEqualMatcher("Namespace", namespace)}
	var matchedLabelsList []labels.Labels
	var err error
	if archiver.indexer.isIndexed(endTime, []string{namespace}) {
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
	refs := make(map[string]uint64)
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
			l = append(l, labels.Label{Name: "Region", Value: archiver.region})
			l = append(l, labels.Label{Name: "Namespace", Value: *params.Namespace})
			l = append(l, labels.Label{Name: "__name__", Value: *params.MetricName})
			for _, dimension := range params.Dimensions {
				l = append(l, labels.Label{Name: *dimension.Name, Value: *dimension.Value})
			}
			if !isExtendedStatistics(*s) {
				l = append(l, labels.Label{Name: "Statistic", Value: *s})
			} else {
				l = append(l, labels.Label{Name: "ExtendedStatistic", Value: *s})
			}
			var errAdd error
			if _, ok := refs[*s]; ok {
				errAdd = app.AddFast(refs[*s], dp.Timestamp.Unix()*1000, value)
			} else {
				refs[*s], errAdd = app.Add(l, dp.Timestamp.Unix()*1000, value)
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

	err = ioutil.WriteFile(archiver.storagePath+"/archiver_state.json", buf, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (archiver *Archiver) loadState() (*State, error) {
	buf, err := ioutil.ReadFile(archiver.storagePath + "/archiver_state.json")
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
	return t.Before(archiver.archivedTimestamp) || t.Equal(archiver.archivedTimestamp)
}
