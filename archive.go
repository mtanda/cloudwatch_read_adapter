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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	prom_value "github.com/prometheus/prometheus/pkg/value"
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
	cloudwatch         *cloudwatch.CloudWatch
	db                 *tsdb.DB
	indexer            *Indexer
	region             string
	namespace          []string
	statistics         []*string
	extendedStatistics []*string
	interval           time.Duration
	retention          time.Duration
	s                  *ArchiverState
	storagePath        string
	logger             log.Logger
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

	s := &ArchiverState{
		Timestamp: make(map[string]int64),
		Namespace: 0,
		Index:     0,
	}

	return &Archiver{
		cloudwatch:         cloudwatch,
		db:                 db,
		indexer:            indexer,
		region:             cfg.Region[0],
		namespace:          cfg.Namespace,
		statistics:         []*string{aws.String("Sum"), aws.String("SampleCount"), aws.String("Maximum"), aws.String("Minimum"), aws.String("Average")},
		extendedStatistics: []*string{aws.String("p50.00"), aws.String("p90.00"), aws.String("p99.00")}, // TODO: add to config
		interval:           time.Duration(24/4) * time.Hour,
		retention:          time.Duration(retention),
		s:                  s,
		storagePath:        storagePath,
		logger:             logger,
	}, nil
}

func (archiver *Archiver) start(eg *errgroup.Group, ctx context.Context) {
	if len(archiver.namespace) == 0 {
		return
	}

	level.Info(archiver.logger).Log("msg", fmt.Sprintf("archive region = %s", archiver.region))
	level.Info(archiver.logger).Log("msg", fmt.Sprintf("archive namespace = %+v", archiver.namespace))
	if state, err := archiver.loadState(); err == nil {
		archiver.s = state
		level.Info(archiver.logger).Log("msg", "state loaded", "timestamp", fmt.Sprintf("%+v", archiver.s.Timestamp), "namespace", archiver.namespace[archiver.s.Namespace], "index", archiver.s.Index)
	} else {
		level.Error(archiver.logger).Log("err", err)
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

			if archiver.isArchived(endTime.Add(-1*time.Second), archiver.namespace) {
				level.Info(archiver.logger).Log("msg", "already archived")
				break
			}
			if endTime.Add(timeMargin).After(now) {
				t.Reset(endTime.Add(timeMargin).Sub(now))
				break
			}

			level.Info(archiver.logger).Log("msg", "archiving start")

			if !archiver.canArchive(endTime, now, archiver.namespace) {
				level.Info(archiver.logger).Log("msg", "not indexed yet, archiving canceled")
				t.Reset(time.Duration(1) * time.Minute)
				break
			}

			if archiver.s.Namespace == 0 && archiver.s.Index == 0 {
				for _, namespace := range archiver.namespace {
					archiverTargetsProgress.WithLabelValues(namespace).Set(float64(0))
					archiverTargetsTotal.WithLabelValues(namespace).Set(float64(0))
				}
			}

			cps := math.Floor(400 * apiCallRate) // support 400 transactions per second (TPS). https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/cloudwatch_limits.html
			ft := time.NewTimer(0)
			wt := time.NewTimer(0)
			eg, actx := errgroup.WithContext(ctx)
			(*eg).Go(func() error {
				level.Info(archiver.logger).Log("msg", fmt.Sprintf("archiving namespace = %s", archiver.namespace[archiver.s.Namespace]))
				matchedLabelsList, err := archiver.getMatchedLabelsList(archiver.namespace[archiver.s.Namespace], startTime, endTime)
				if err != nil {
					return err
				}
				archiverTargetsTotal.WithLabelValues(archiver.namespace[archiver.s.Namespace]).Set(float64(len(matchedLabelsList)))

				archiver.db.DisableCompactions()
				app := archiver.db.Appender()
				l := make(labels.Labels, 0)
				l = append(l, labels.Label{Name: "__name__", Value: "dummy"})
				if _, err = app.Add(l, startTime.Unix()*1000, 0); err != nil {
					return err
				}
				if err := app.Commit(); err != nil {
					return err
				}
				appenders := make(map[int]*tsdb.Appender)
				for i := range archiver.namespace {
					app := archiver.db.Appender()
					appenders[i] = &app
				}
				for {
					select {
					case <-ft.C:
						ft.Reset(1 * time.Second / time.Duration(cps))

						if len(matchedLabelsList) > 0 {
							matchedLabels := matchedLabelsList[archiver.s.Index]
							err = archiver.process(*appenders[archiver.s.Namespace], matchedLabels, startTime, endTime)
							if err != nil {
								return err
							}
							archiver.s.Index++
						}

						if archiver.s.Index == len(matchedLabelsList) {
							lastNamespace := archiver.s.Namespace
							archiver.s.Namespace++
							if archiver.s.Namespace == len(archiver.namespace) {
								// archive finished
								if !ft.Stop() {
									<-ft.C
								}
								if !wt.Stop() {
									<-wt.C
								}

								archiver.db.EnableCompactions()
								if err := (*appenders[lastNamespace]).Commit(); err != nil {
									return err
								}
								appenders[lastNamespace] = nil // release appender
								archiver.s.Timestamp[archiver.namespace[lastNamespace]] = endTime.Add(-1 * time.Second).Unix()

								level.Info(archiver.logger).Log("namespace", archiver.namespace[lastNamespace], "index", archiver.s.Index, "len", len(matchedLabelsList))
								archiverTargetsProgress.WithLabelValues(archiver.namespace[lastNamespace]).Set(float64(archiver.s.Index))

								// reset index for next archiving cycle
								archiver.s.Index = 0
								archiver.s.Namespace = 0

								if err := archiver.saveState(); err != nil {
									return err
								}
								level.Info(archiver.logger).Log("msg", "archiving completed")

								return nil
							} else {
								if err := (*appenders[lastNamespace]).Commit(); err != nil {
									return err
								}
								appenders[lastNamespace] = nil // release appender
								archiver.s.Timestamp[archiver.namespace[lastNamespace]] = endTime.Add(-1 * time.Second).Unix()
								if err := archiver.saveState(); err != nil {
									return err
								}

								level.Info(archiver.logger).Log("namespace", archiver.namespace[lastNamespace], "index", archiver.s.Index, "len", len(matchedLabelsList))
								archiverTargetsProgress.WithLabelValues(archiver.namespace[lastNamespace]).Set(float64(archiver.s.Index))

								// archive next namespace
								archiver.s.Index = 0

								level.Info(archiver.logger).Log("msg", fmt.Sprintf("archiving namespace = %s", archiver.namespace[archiver.s.Namespace]))
								matchedLabelsList, err = archiver.getMatchedLabelsList(archiver.namespace[archiver.s.Namespace], startTime, endTime)
								if err != nil {
									return err
								}
								archiverTargetsTotal.WithLabelValues(archiver.namespace[archiver.s.Namespace]).Set(float64(len(matchedLabelsList)))
							}
						}
					case <-actx.Done():
						if !ft.Stop() {
							<-ft.C
						}
						if !wt.Stop() {
							<-wt.C
						}

						return nil
					}
				}
			})

			if err := eg.Wait(); err != nil {
				level.Error(archiver.logger).Log("err", err)
				archiver.db.Close()
				return err
			}
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
		matchedLabelsList, err = archiver.indexer.getMatchedLables(matchers, startTime.Unix()*1000, archiver.indexer.s.TimestampTo[namespace]*1000)
	}

	return matchedLabelsList, err
}

func (archiver *Archiver) process(app tsdb.Appender, _labels labels.Labels, startTime time.Time, endTime time.Time) error {
	timeAlignment := 60

	var resp *cloudwatch.GetMetricStatisticsOutput
	var params *cloudwatch.GetMetricStatisticsInput
	var err error
	for _, period := range []int{timeAlignment, 300} {
		params = &cloudwatch.GetMetricStatisticsInput{}
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
		params.Period = aws.Int64(int64(period))
		params.StartTime = aws.Time(startTime)
		params.EndTime = aws.Time(endTime)

		if params.Namespace == nil || params.MetricName == nil ||
			(params.Statistics == nil && params.ExtendedStatistics == nil) {
			return fmt.Errorf("missing parameter")
		}

		resp, err = archiver.cloudwatch.GetMetricStatistics(params)
		if err != nil {
			cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", *params.Namespace, "archive", "error").Add(float64(1))
			return err
		}
		cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", *params.Namespace, "archive", "success").Add(float64(1))

		if len(resp.Datapoints) > 0 {
			break
		}
	}

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
			if !isExtendedStatistics(*s) {
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
				}
			} else {
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
				return err
			}
		}
	}

	return nil
}

type ArchiverState struct {
	Timestamp map[string]int64 `json:"timestamp"`
	Namespace int              `json:"namespace"`
	Index     int              `json:"index"`
}

func (archiver *Archiver) saveState() error {
	buf, err := json.Marshal(archiver.s)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(archiver.storagePath+"/archiver_state.json", buf, 0644)
	if err != nil {
		return err
	}

	return nil
}

func (archiver *Archiver) loadState() (*ArchiverState, error) {
	buf, err := ioutil.ReadFile(archiver.storagePath + "/archiver_state.json")
	if err != nil {
		return nil, err
	}

	var state ArchiverState
	if err = json.Unmarshal(buf, &state); err != nil {
		return nil, err
	}

	return &state, nil
}

func (archiver *Archiver) query(q *prompb.Query, maximumStep int) (resultMap, error) {
	result := make(resultMap)

	matchers, err := fromLabelMatchers(q.Matchers)
	if err != nil {
		return nil, err
	}

	querier, err := archiver.db.Querier(q.StartTimestampMs, q.EndTimestampMs)
	if err != nil {
		return nil, err
	}
	defer querier.Close()

	step := int64(maximumStep)

	// TODO: generate Average result from Sum and SampleCount
	// TODO: generate Maximum/Minimum result from Average
	ss, err := querier.Select(matchers...)
	if err != nil {
		return nil, err
	}
	for ss.Next() {
		ts := &prompb.TimeSeries{}
		s := ss.At()

		labels := s.Labels()
		sort.Slice(labels, func(i, j int) bool {
			return labels[i].Name < labels[j].Name
		})
		id := ""
		for _, label := range labels {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: label.Name, Value: label.Value})
			id = id + label.Name + label.Value
		}

		var lastTimestamp int64
		var lastValue float64
		t, v := int64(0), float64(0)
		it := s.Iterator()
		refTime := q.StartTimestampMs
		for it.Next() && refTime < q.EndTimestampMs {
			lastTimestamp = t
			lastValue = v
			t, v = it.At()
			for refTime < lastTimestamp {
				refTime += (step * 1000)
			}
			if (t > refTime) && (lastTimestamp > (refTime - (step * 1000))) {
				ts.Samples = append(ts.Samples, &prompb.Sample{Value: lastValue, Timestamp: lastTimestamp})
				if (t - lastTimestamp) > (step * 1000) {
					ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: lastTimestamp + (step * 1000)})
				}
			}
		}
		if (q.EndTimestampMs > lastTimestamp) && (lastTimestamp > (q.EndTimestampMs - (step * 1000))) {
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: lastValue, Timestamp: lastTimestamp})
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: lastTimestamp + (step * 1000)})
		}

		if _, ok := result[id]; ok {
			if result[id].Samples[0].Timestamp < ts.Samples[0].Timestamp {
				result[id].Samples = append(result[id].Samples, ts.Samples...)
			} else {
				result[id].Samples = append(ts.Samples, result[id].Samples...)
			}
		} else {
			result[id] = ts
		}
	}

	return result, nil
}

func (archiver *Archiver) canArchive(endTime time.Time, now time.Time, namespace []string) bool {
	if !archiver.indexer.canIndex(endTime, namespace) && !archiver.indexer.isExpired(now, namespace) {
		return true // for initial archiving
	}
	if !archiver.indexer.isIndexed(endTime, namespace) {
		return false
	}
	return true
}

func (archiver *Archiver) isArchived(t time.Time, namespace []string) bool {
	for _, n := range namespace {
		if _, ok := archiver.s.Timestamp[n]; !ok {
			return false
		}
		if t.After(time.Unix(archiver.s.Timestamp[n], 0)) {
			return false
		}
	}
	return true
}

func (archiver *Archiver) isExpired(t time.Time) bool {
	expiredTime := time.Now().Add(-archiver.retention)
	if t.After(expiredTime) {
		return false
	}
	return true
}
