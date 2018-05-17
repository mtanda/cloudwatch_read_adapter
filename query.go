package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/prometheus/client_golang/prometheus"
	prom_value "github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/prompb"
)

var (
	cloudwatchApiCalls = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cloudwatch_read_adapter_cloudwatch_api_calls_total",
			Help: "The total number of CloudWatch API calls",
		},
		[]string{"api", "namespace", "from", "status"},
	)
)

func init() {
	prometheus.MustRegister(cloudwatchApiCalls)
}

func getQueryWithoutIndex(q *prompb.Query, indexer *Indexer, maximumStep int) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
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
			v, err := strconv.ParseInt(m.Value, 10, 64)
			if err != nil {
				return region, queries, err
			}
			if v > int64(maximumStep) {
				v = int64(maximumStep) / 60 * 60
			}
			query.Period = aws.Int64(v)
		default:
			if m.Value != "" {
				query.Dimensions = append(query.Dimensions, &cloudwatch.Dimension{
					Name:  aws.String(m.Name),
					Value: aws.String(m.Value),
				})
			}
		}
	}
	query.StartTime = aws.Time(time.Unix(int64(q.StartTimestampMs/1000), int64(q.StartTimestampMs%1000*1000)))
	query.EndTime = aws.Time(time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000)))
	queries = append(queries, query)

	return region, queries, nil
}

func getQueryWithIndex(q *prompb.Query, indexer *Indexer, maximumStep int) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	region := ""
	queries := make([]*cloudwatch.GetMetricStatisticsInput, 0)

	// index doesn't have statistics label, get label matchers without statistics
	mm := make([]*prompb.LabelMatcher, 0)
	for _, m := range q.Matchers {
		if m.Name == "Statistic" || m.Name == "ExtendedStatistic" || m.Name == "Period" {
			continue
		}
		mm = append(mm, m)
	}

	matchers, err := fromLabelMatchers(mm)
	if err != nil {
		return region, queries, err
	}
	iq := *q
	if time.Unix(q.EndTimestampMs/1000, 0).Sub(time.Unix(q.StartTimestampMs/1000, 0)) < 2*indexer.interval {
		// expand enough long period to match index
		iq.StartTimestampMs = time.Unix(q.EndTimestampMs/1000, 0).Add(-2*indexer.interval).Unix() * 1000
	}
	matchedLabelsList, err := indexer.getMatchedLables(matchers, iq.StartTimestampMs, q.EndTimestampMs)
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
				if label.Value != "" {
					query.Dimensions = append(query.Dimensions, &cloudwatch.Dimension{
						Name:  aws.String(label.Name),
						Value: aws.String(label.Value),
					})
				}
			}
		}
		for _, m := range q.Matchers {
			if !(m.Type == prompb.LabelMatcher_EQ || m.Type == prompb.LabelMatcher_RE) {
				continue // only support equal matcher or regex matcher with alternation
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
			case "Period":
				if m.Type == prompb.LabelMatcher_EQ {
					v, err := strconv.ParseInt(m.Value, 10, 64)
					if err != nil {
						return region, queries, err
					}
					if v > int64(maximumStep) {
						v = int64(maximumStep) / 60 * 60
					}
					query.Period = aws.Int64(v)
				}
			}
		}
		if len(query.Statistics) == 0 && len(query.ExtendedStatistics) == 0 {
			query.Statistics = []*string{aws.String("Sum"), aws.String("SampleCount"), aws.String("Maximum"), aws.String("Minimum"), aws.String("Average")}
			query.ExtendedStatistics = []*string{aws.String("p50.00"), aws.String("p90.00"), aws.String("p99.00")}
		}
		query.StartTime = aws.Time(time.Unix(int64(q.StartTimestampMs/1000), int64(q.StartTimestampMs%1000*1000)))
		query.EndTime = aws.Time(time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000)))
		queries = append(queries, query)
	}

	return region, queries, nil
}

func queryCloudWatch(svc *cloudwatch.CloudWatch, region string, query *cloudwatch.GetMetricStatisticsInput, q *prompb.Query, lookbackDelta time.Duration) (resultMap, error) {
	result := make(resultMap)

	if query.Namespace == nil || query.MetricName == nil {
		return result, fmt.Errorf("missing parameter")
	}

	// align time range
	periodUnit := 60
	rangeAdjust := 0 * time.Second
	if q.StartTimestampMs%int64(periodUnit*1000) != 0 {
		rangeAdjust = time.Duration(periodUnit) * time.Second
	}
	query.StartTime = aws.Time(query.StartTime.Truncate(time.Duration(periodUnit)).Add(rangeAdjust))
	query.EndTime = aws.Time(query.EndTime.Truncate(time.Duration(periodUnit)))

	// auto calibrate period
	highResolution := true
	if query.Period == nil {
		period := calibratePeriod(*query.StartTime)
		queryTimeRange := (*query.EndTime).Sub(*query.StartTime).Seconds()
		if queryTimeRange/float64(period) >= 1440 {
			period = int64(math.Ceil(queryTimeRange/float64(1440)/float64(periodUnit))) * int64(periodUnit)
		}
		query.Period = aws.Int64(period)
		highResolution = false
	}

	startTime := *query.StartTime
	endTime := *query.EndTime
	var resp *cloudwatch.GetMetricStatisticsOutput
	for startTime.Before(endTime) {
		query.StartTime = aws.Time(startTime)
		if highResolution {
			startTime = startTime.Add(time.Duration(1440*(*query.Period)) * time.Second)
		} else {
			startTime = endTime
		}
		query.EndTime = aws.Time(startTime)

		partResp, err := svc.GetMetricStatistics(query)
		if err != nil {
			cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", *query.Namespace, "query", "error").Add(float64(1))
			return result, err
		}
		if resp != nil {
			resp.Datapoints = append(resp.Datapoints, partResp.Datapoints...)
		} else {
			resp = partResp
		}
		cloudwatchApiCalls.WithLabelValues("GetMetricStatistics", *query.Namespace, "query", "success").Add(float64(1))
		if len(resp.Datapoints) > PROMETHEUS_MAXIMUM_STEPS {
			return result, fmt.Errorf("exceed maximum datapoints")
		}
	}

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
		if !isExtendedStatistics(*s) {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: "Statistic", Value: *s})
		} else {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: "ExtendedStatistic", Value: *s})
		}
		tsm[*s] = ts
	}

	sort.Slice(resp.Datapoints, func(i, j int) bool {
		return resp.Datapoints[i].Timestamp.Before(*resp.Datapoints[j].Timestamp)
	})
	var lastTimestamp time.Time
	for _, dp := range resp.Datapoints {
		for _, s := range paramStatistics {
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
			ts := tsm[*s]
			if !lastTimestamp.IsZero() && lastTimestamp.Add(time.Duration(*query.Period)*time.Second).Before(*dp.Timestamp) {
				ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + *query.Period) * 1000})
			}
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: value, Timestamp: dp.Timestamp.Unix() * 1000})
		}
		lastTimestamp = *dp.Timestamp
	}
	if !lastTimestamp.IsZero() && lastTimestamp.Before(endTime) && lastTimestamp.Before(time.Now().Add(-lookbackDelta)) {
		for _, s := range paramStatistics {
			ts := tsm[*s]
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + *query.Period) * 1000})
		}
	}

	for _, ts := range tsm {
		id := ""
		sort.Slice(ts.Labels, func(i, j int) bool {
			return ts.Labels[i].Name < ts.Labels[j].Name
		})
		for _, label := range ts.Labels {
			id = id + label.Name + label.Value
		}
		result[id] = ts
	}

	return result, nil
}

func calibratePeriod(startTime time.Time) int64 {
	var period int64

	timeDay := 24 * time.Hour
	now := time.Now().UTC()
	timeRangeToNow := now.Sub(startTime)
	if timeRangeToNow < timeDay*15 { // until 15 days ago
		period = 60
	} else if timeRangeToNow <= (timeDay * 63) { // until 63 days ago
		period = 60 * 5
	} else if timeRangeToNow <= (timeDay * 455) { // until 455 days ago
		period = 60 * 60
	} else { // over 455 days, should return error, but try to long period
		period = 60 * 60
	}

	return period
}
