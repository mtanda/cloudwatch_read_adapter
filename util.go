package main

import (
	"fmt"
	"os"
	"regexp"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
)

var invalidMetricNamePattern = regexp.MustCompile(`[^a-zA-Z0-9:_]`)

func fromLabelMatchers(matchers []*prompb.LabelMatcher) ([]*labels.Matcher, error) {
	result := make([]*labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var m *labels.Matcher
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			m = labels.MustNewMatcher(labels.MatchEqual, matcher.Name, matcher.Value)
		case prompb.LabelMatcher_NEQ:
			m = labels.MustNewMatcher(labels.MatchNotEqual, matcher.Name, matcher.Value)
		case prompb.LabelMatcher_RE:
			m = labels.MustNewMatcher(labels.MatchRegexp, matcher.Name, "^(?:"+matcher.Value+")$")
		case prompb.LabelMatcher_NRE:
			m = labels.MustNewMatcher(labels.MatchNotRegexp, matcher.Name, "^(?:"+matcher.Value+")$")
		default:
			return nil, fmt.Errorf("invalid matcher type")
		}
		result = append(result, m)
	}
	return result, nil
}

func isExtendedStatistics(s string) bool {
	return s != "Sum" && s != "SampleCount" && s != "Maximum" && s != "Minimum" && s != "Average"
}

var regionCache = ""

func GetDefaultRegion() (string, error) {
	var region string

	if regionCache != "" {
		return regionCache, nil
	}

	metadata := ec2metadata.New(session.New(), &aws.Config{
		MaxRetries: aws.Int(0),
	})
	if metadata.Available() {
		var err error
		region, err = metadata.Region()
		if err != nil {
			return "", err
		}
		if region != "" {
			regionCache = region
		}
	} else {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
	}

	return region, nil
}

func SafeMetricName(name string) string {
	if len(name) == 0 {
		return ""
	}
	name = invalidMetricNamePattern.ReplaceAllString(name, "_")
	if '0' <= name[0] && name[0] <= '9' {
		name = "_" + name
	}
	return name
}

type resultMap map[string]*prompb.TimeSeries

func (x resultMap) append(y resultMap) {
	for id, yts := range y {
		if xts, ok := x[id]; ok {
			if (len(xts.Samples) > 0 && len(yts.Samples) > 0) && xts.Samples[0].Timestamp < yts.Samples[0].Timestamp {
				xts.Samples = append(xts.Samples, yts.Samples...)
			} else {
				xts.Samples = append(yts.Samples, xts.Samples...)
			}
		} else {
			x[id] = yts
		}
	}
}
func (x resultMap) slice() []*prompb.TimeSeries {
	s := []*prompb.TimeSeries{}
	for _, v := range x {
		s = append(s, v)
	}
	return s
}
