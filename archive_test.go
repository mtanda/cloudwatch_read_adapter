package main_test

import (
	"io/ioutil"
	"testing"

	"github.com/go-kit/kit/log"
	main "github.com/mtanda/cloudwatch_read_adapter"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/prometheus/tsdb/labels"
)

func TestQuery(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	testutil.Ok(t, err)

	cfg := main.ArchiveConfig{
		Region:    []string{"us-east-1"},
		Namespace: []string{"AWS/EC2"},
		Retention: "1d",
	}
	indexer := &main.Indexer{}
	logger := log.NewNopLogger()
	archiver, err := main.NewArchiver(cfg, tmpdir, indexer, logger)
	testutil.Ok(t, err)

	l := make(labels.Labels, 0)
	l = append(l, labels.Label{Name: "__name__", Value: "test"})
	err = archiver.ExposeSetTestData(l, [][]int64{{1533687120000, 1}})
	testutil.Ok(t, err)

	query := prompb.Query{
		StartTimestampMs: 1533682980000,
		EndTimestampMs:   1533704880000,
		Matchers: []*prompb.LabelMatcher{
			&prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "test",
			},
		},
	}
	result, err := archiver.Query(&query, 61)
	testutil.Ok(t, err)
	testutil.Equals(t, len(result["__name__test"].Samples), 2)
	testutil.Equals(t, result["__name__test"].Samples[0].Timestamp, int64(1533687120000))
	testutil.Equals(t, result["__name__test"].Samples[1].Timestamp, int64(1533687181000))
}
