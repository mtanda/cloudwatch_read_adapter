package main

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
	clientCache = make(map[string]*cloudwatch.CloudWatch)
)

func init() {
	prometheus.MustRegister(cloudwatchApiCalls)
}

func getQueryWithoutIndex(q *prompb.Query, indexer *Indexer, maximumStep int64) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
	region := ""
	queries := make([]*cloudwatch.GetMetricStatisticsInput, 0)

	query := &cloudwatch.GetMetricStatisticsInput{}
	for _, m := range q.Matchers {
		if m.Type != prompb.LabelMatcher_EQ {
			continue // only support equal matcher
		}

		oldMetricName := ""
		switch m.Name {
		case "__name__":
			oldMetricName = m.Value
		case "Region":
			region = m.Value
		case "Namespace":
			query.Namespace = aws.String(m.Value)
		case "MetricName":
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
			maximumStep = int64(math.Max(float64(maximumStep), float64(60)))
			if v < maximumStep {
				v = maximumStep
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
		if query.MetricName == nil {
			query.MetricName = aws.String(oldMetricName) // backward compatibility
		}
	}
	query.StartTime = aws.Time(time.Unix(int64(q.StartTimestampMs/1000), int64(q.StartTimestampMs%1000*1000)))
	query.EndTime = aws.Time(time.Unix(int64(q.EndTimestampMs/1000), int64(q.EndTimestampMs%1000*1000)))
	queries = append(queries, query)

	return region, queries, nil
}

func getQueryWithIndex(q *prompb.Query, indexer *Indexer, maximumStep int64) (string, []*cloudwatch.GetMetricStatisticsInput, error) {
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
	matchedLabelsList, err := indexer.getMatchedLabels(matchers, iq.StartTimestampMs, q.EndTimestampMs)
	if err != nil {
		return region, queries, err
	}

	for _, matchedLabels := range matchedLabelsList {
		query := &cloudwatch.GetMetricStatisticsInput{}
		for _, label := range matchedLabels {
			oldMetricName := ""
			switch label.Name {
			case "__name__":
				oldMetricName = label.Value
			case "Region":
				// TODO: support multiple region?
				region = label.Value
			case "Namespace":
				query.Namespace = aws.String(label.Value)
			case "MetricName":
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
			if query.MetricName == nil {
				query.MetricName = aws.String(oldMetricName) // backward compatibility
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
					maximumStep = int64(math.Max(float64(maximumStep), float64(60)))
					if v < maximumStep {
						v = maximumStep
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

func isSingleStatistic(queries []*cloudwatch.GetMetricStatisticsInput) bool {
	s := ""
	for _, query := range queries {
		if len(query.Statistics) > 1 && len(query.ExtendedStatistics) > 1 {
			return false
		}
		if len(query.Statistics) == 1 {
			if s == "" {
				s = *query.Statistics[0]
				continue
			}
			if s != *query.Statistics[0] {
				return false
			}
		}
		if len(query.ExtendedStatistics) == 1 {
			if s == "" {
				s = *query.ExtendedStatistics[0]
				continue
			}
			if s != *query.ExtendedStatistics[0] {
				return false
			}
		}
	}
	return true
}

func queryCloudWatch(region string, queries []*cloudwatch.GetMetricStatisticsInput, q *prompb.Query, lookbackDelta time.Duration, result resultMap) error {
	if !isSingleStatistic(queries) {
		if len(queries) > 200 {
			return fmt.Errorf("Too many concurrent queries")
		}
		for _, query := range queries {
			cwResult, err := queryCloudWatchGetMetricStatistics(region, query, q, lookbackDelta)
			if err != nil {
				return err
			}
			result.append(cwResult)
		}
	} else {
		if len(queries)/70 > 25 {
			return fmt.Errorf("Too many concurrent queries")
		}
		for i := 0; i < len(queries); i += 70 {
			e := int(math.Min(float64(i+70), float64(len(queries))))
			cwResult, err := queryCloudWatchGetMetricData(region, queries[i:e], q, lookbackDelta)
			if err != nil {
				return err
			}
			result.append(cwResult)
		}
	}
	return nil
}

func queryCloudWatchGetMetricStatistics(region string, query *cloudwatch.GetMetricStatisticsInput, q *prompb.Query, lookbackDelta time.Duration) (resultMap, error) {
	result := make(resultMap)
	svc, err := getClient(region)
	if err != nil {
		return nil, err
	}

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
		if len(resp.Datapoints) > PROMETHEUS_MAXIMUM_POINTS {
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
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "__name__", Value: SafeMetricName(*query.MetricName)})
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
			if *query.Period > 60 && !lastTimestamp.IsZero() && lastTimestamp.Add(time.Duration(*query.Period)*time.Second).Before(*dp.Timestamp) {
				ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + *query.Period) * 1000})
			}
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: value, Timestamp: dp.Timestamp.Unix() * 1000})
		}
		lastTimestamp = *dp.Timestamp
	}
	if *query.Period > 60 && !lastTimestamp.IsZero() && lastTimestamp.Before(endTime) && lastTimestamp.Before(time.Now().UTC().Add(-lookbackDelta)) {
		for _, s := range paramStatistics {
			ts := tsm[*s]
			ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + *query.Period) * 1000})
		}
	}

	// generate unique id
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

func queryCloudWatchGetMetricData(region string, queries []*cloudwatch.GetMetricStatisticsInput, q *prompb.Query, lookbackDelta time.Duration) (resultMap, error) {
	result := make(resultMap)
	svc, err := getClient(region)
	if err != nil {
		return nil, err
	}

	params := &cloudwatch.GetMetricDataInput{
		ScanBy: aws.String("TimestampAscending"),
	}
	var namespace string
	var period int64
	periodUnit := 60

	// convert to GetMetricData query
	for i, query := range queries {
		// auto calibrate period
		if query.Period == nil {
			period := calibratePeriod(*query.StartTime)
			queryTimeRange := (*query.EndTime).Sub(*query.StartTime).Seconds()
			if queryTimeRange/float64(period) >= 1440 {
				period = int64(math.Ceil(queryTimeRange/float64(1440)/float64(periodUnit))) * int64(periodUnit)
			}
			query.Period = aws.Int64(period)
		}

		mdq := &cloudwatch.MetricDataQuery{
			Id:         aws.String("id" + strconv.Itoa(i)),
			ReturnData: aws.Bool(true),
		}
		mdq.MetricStat = &cloudwatch.MetricStat{
			Metric: &cloudwatch.Metric{
				Namespace:  query.Namespace,
				MetricName: query.MetricName,
			},
			Period: query.Period,
		}
		namespace = *query.Namespace
		period = *query.Period
		for _, d := range query.Dimensions {
			mdq.MetricStat.Metric.Dimensions = append(mdq.MetricStat.Metric.Dimensions,
				&cloudwatch.Dimension{
					Name:  d.Name,
					Value: d.Value,
				})
		}
		if len(query.Statistics) == 1 {
			mdq.MetricStat.Stat = query.Statistics[0]
		} else {
			mdq.MetricStat.Stat = query.ExtendedStatistics[0]
		}
		params.MetricDataQueries = append(params.MetricDataQueries, mdq)

		// query range should be same among queries
		params.StartTime = query.StartTime
		params.EndTime = query.EndTime
	}

	// align time range
	rangeAdjust := 0 * time.Second
	if q.StartTimestampMs%int64(periodUnit*1000) != 0 {
		rangeAdjust = time.Duration(periodUnit) * time.Second
	}
	params.StartTime = aws.Time(params.StartTime.Truncate(time.Duration(periodUnit)).Add(rangeAdjust))
	params.EndTime = aws.Time(params.EndTime.Truncate(time.Duration(periodUnit)))
	if (params.EndTime).Sub(*params.StartTime)/(time.Duration(period)*time.Second) > PROMETHEUS_MAXIMUM_POINTS {
		return result, fmt.Errorf("exceed maximum datapoints")
	}

	tsm := make(map[string]*prompb.TimeSeries)
	for _, r := range params.MetricDataQueries {
		ts := &prompb.TimeSeries{}
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "Region", Value: region})
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "Namespace", Value: *r.MetricStat.Metric.Namespace})
		ts.Labels = append(ts.Labels, &prompb.Label{Name: "__name__", Value: SafeMetricName(*r.MetricStat.Metric.MetricName)})
		for _, d := range r.MetricStat.Metric.Dimensions {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: *d.Name, Value: *d.Value})
		}
		s := *r.MetricStat.Stat
		if !isExtendedStatistics(s) {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: "Statistic", Value: s})
		} else {
			ts.Labels = append(ts.Labels, &prompb.Label{Name: "ExtendedStatistic", Value: s})
		}
		tsm[*r.Id] = ts
	}

	ctx := context.Background()
	nextToken := ""
	for {
		if nextToken != "" {
			params.NextToken = aws.String(nextToken)
		}

		resp, err := svc.GetMetricDataWithContext(ctx, params)
		if err != nil {
			cloudwatchApiCalls.WithLabelValues("GetMetricData", namespace, "query", "error").Add(float64(len(params.MetricDataQueries)))
			return nil, err
		}
		cloudwatchApiCalls.WithLabelValues("GetMetricData", namespace, "query", "success").Add(float64(len(params.MetricDataQueries)))
		for _, r := range resp.MetricDataResults {
			ts := tsm[*r.Id]
			for i, t := range r.Timestamps {
				ts.Samples = append(ts.Samples, &prompb.Sample{Value: *r.Values[i], Timestamp: t.Unix() * 1000})
				if period <= 60 {
					continue
				}
				if i != len(r.Timestamps) && i != 0 {
					lastTimestamp := r.Timestamps[i-1]
					if lastTimestamp.Add(time.Duration(period) * time.Second).Before(*r.Timestamps[i]) {
						ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + period) * 1000})
					}
				} else if i == len(r.Timestamps) {
					lastTimestamp := r.Timestamps[i]
					if lastTimestamp.Before(*params.EndTime) && lastTimestamp.Before(time.Now().UTC().Add(-lookbackDelta)) {
						ts.Samples = append(ts.Samples, &prompb.Sample{Value: math.Float64frombits(prom_value.StaleNaN), Timestamp: (lastTimestamp.Unix() + period) * 1000})
					}
				}
			}
		}

		if resp.NextToken == nil || *resp.NextToken == "" {
			break
		}
		nextToken = *resp.NextToken
	}

	// generate unique id
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

func getClient(region string) (*cloudwatch.CloudWatch, error) {
	if client, ok := clientCache[region]; ok {
		return client, nil
	}
	cfg := &aws.Config{Region: aws.String(region)}
	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}
	clientCache[region] = cloudwatch.New(sess, cfg)
	return clientCache[region], nil
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
