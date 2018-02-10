package main

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/prometheus/prometheus/prompb"
)

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
	queries = append(queries, query)

	return region, queries, nil
}

func getQueryWithIndex(q *prompb.Query, indexer *Indexer) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	region := ""
	queries := make([]*cloudwatch.GetMetricStatisticsInput, 0)

	// index doesn't have statistics label, get label matchers without statistics
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
			}
		}
		if len(query.Statistics) == 0 && len(query.ExtendedStatistics) == 0 {
			query.Statistics = []*string{aws.String("Sum"), aws.String("SampleCount"), aws.String("Maximum"), aws.String("Minimum"), aws.String("Average")}
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
	period := calibratePeriod(*query.StartTime)
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

func calibratePeriod(startTime time.Time) int {
	var period int

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
