package main

import (
	"fmt"

	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/tsdb/labels"
)

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

func isExtendedStatistics(s *string) bool {
	return *s != "Sum" && *s != "SampleCount" && *s != "Maximum" && *s != "Minimum" && *s != "Average"
}
