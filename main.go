package main

import (
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/log"

	"github.com/prometheus/prometheus/storage/remote"
)

type byTimestamp []*cloudwatch.Datapoint

func (t byTimestamp) Len() int           { return len(t) }
func (t byTimestamp) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t byTimestamp) Less(i, j int) bool { return t[i].Timestamp.Before(*t[j].Timestamp) }

func runQuery(q *remote.Query) []*remote.TimeSeries {
	result := []*remote.TimeSeries{}

	// parse query
	var region string
	var query cloudwatch.GetMetricStatisticsInput
	for _, m := range q.Matchers {
		if m.Type != remote.MatchType_EQUAL {
			continue // only support equal matcher
		}

		switch strings.ToLower(m.Name) {
		case "region":
			region = m.Value
		case "namespace":
			query.Namespace = aws.String(m.Value)
		case "metricname":
			query.MetricName = aws.String(m.Value)
		case "statistics":
			query.Statistics = []*string{aws.String(m.Value)}
		case "extendedstatistics":
			query.ExtendedStatistics = []*string{aws.String(m.Value)}
		case "period":
			period, err := strconv.Atoi(m.Value)
			if err != nil {
				log.Errorf("Invalid period = %s", m.Value)
				return result
			}
			query.Period = aws.Int64(int64(period))
		default:
			query.Dimensions = append(query.Dimensions, &cloudwatch.Dimension{
				Name:  aws.String(m.Name),
				Value: aws.String(m.Value),
			})
		}
	}

	periodUnit := 60
	if *query.Namespace == "AWS/EC2" {
		periodUnit = 300
	}

	query.StartTime = aws.Time(time.Unix((q.StartTimestampMs/1000/int64(periodUnit)+1)*int64(periodUnit), 0))
	query.EndTime = aws.Time(time.Unix(q.EndTimestampMs/1000/int64(periodUnit)*int64(periodUnit), 0))

	// auto calibrate period
	period := 60
	timeDay := 24 * time.Hour
	if query.Period == nil {
		now := time.Now()
		timeRangeToNow := now.Sub(*query.StartTime)
		if timeRangeToNow < timeDay*15 { // until 15 days ago
			if *query.Namespace == "AWS/EC2" {
				period = 300
			} else {
				period = 60
			}
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
		log.Errorf("Failed to initialize session")
		return result
	}

	svc := cloudwatch.New(sess, cfg)
	resp, err := svc.GetMetricStatistics(&query)
	if err != nil {
		log.Errorf("Failed to get metrics: query = %+v", query)
		return result
	}

	// make time series
	ts := &remote.TimeSeries{}
	for _, d := range query.Dimensions {
		ts.Labels = append(ts.Labels, &remote.LabelPair{Name: *d.Name, Value: *d.Value})
	}
	ts.Labels = append(ts.Labels, &remote.LabelPair{Name: "region", Value: region})
	ts.Labels = append(ts.Labels, &remote.LabelPair{Name: "namespace", Value: *query.Namespace})
	ts.Labels = append(ts.Labels, &remote.LabelPair{Name: "statistics", Value: *query.Statistics[0]})
	//ts.Labels = append(ts.Labels, &remote.LabelPair{Name: "extendedStatistics", Value: *query.ExtendedStatistics[0]})
	ts.Labels = append(ts.Labels, &remote.LabelPair{Name: "period", Value: strconv.FormatInt(*query.Period, 10)})

	sort.Sort(byTimestamp(resp.Datapoints))
	for _, dp := range resp.Datapoints {
		value := 0.0
		// TODO: support extended statistics
		switch *query.Statistics[0] {
		case "Average":
			value = *dp.Average
		case "Maximum":
			value = *dp.Maximum
		case "Minimum":
			value = *dp.Minimum
		case "Sum":
			value = *dp.Sum
		case "SampleCount":
			value = *dp.SampleCount
		}
		ts.Samples = append(ts.Samples, &remote.Sample{Value: value, TimestampMs: dp.Timestamp.Unix() * 1000})
	}
	result = append(result, ts)
	log.Infof("Returned %d time series.", len(result))

	return result
}

func main() {
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

		var req remote.ReadRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if len(req.Queries) != 1 {
			http.Error(w, "Can only handle one query.", http.StatusBadRequest)
			return
		}

		resp := remote.ReadResponse{
			Results: []*remote.QueryResult{
				{Timeseries: runQuery(req.Queries[0])},
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

	http.ListenAndServe(":8080", nil)
}
