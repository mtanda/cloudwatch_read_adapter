package main

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/ec2/imds"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/labels"
)

var invalidMetricNamePattern = regexp.MustCompile(`[^a-zA-Z0-9:_]`)

func fromLabelMatchers(matchers []*prompb.LabelMatcher) ([]labels.Matcher, error) {
	result := make([]labels.Matcher, 0, len(matchers))
	for _, matcher := range matchers {
		var m labels.Matcher
		var err error
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			m = labels.NewEqualMatcher(matcher.Name, matcher.Value)
		case prompb.LabelMatcher_NEQ:
			m = labels.Not(labels.NewEqualMatcher(matcher.Name, matcher.Value))
		case prompb.LabelMatcher_RE:
			m, err = labels.NewRegexpMatcher(matcher.Name, "^(?:"+matcher.Value+")$")
			if err != nil {
				return nil, err
			}
		case prompb.LabelMatcher_NRE:
			m, err = labels.NewRegexpMatcher(matcher.Name, "^(?:"+matcher.Value+")$")
			if err != nil {
				return nil, err
			}
			m = labels.Not(m)
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

	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}

	client := imds.NewFromConfig(cfg)
	response, err := client.GetRegion(ctx, &imds.GetRegionInput{})
	if err != nil {
		region = os.Getenv("AWS_REGION")
		if region == "" {
			region = "us-east-1"
		}
	} else {
		region = response.Region
		if region != "" {
			regionCache = region
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
