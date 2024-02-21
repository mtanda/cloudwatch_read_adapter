package main_test

import (
	"context"
	"io/ioutil"
	"testing"
	"time"

	"github.com/go-kit/kit/log"
	main "github.com/mtanda/cloudwatch_read_adapter"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "test")
	require.Equal(t, nil, err)

	cfg := main.ArchiveConfig{
		Region:    []string{"us-east-1"},
		Namespace: []string{"AWS/EC2"},
		Retention: "1d",
	}
	indexer := &main.Indexer{}
	logger := log.NewNopLogger()
	archiver, err := main.NewArchiver(cfg, tmpdir, indexer, logger)
	require.Equal(t, nil, err)

	l := make(labels.Labels, 0)
	l = append(l, labels.Label{Name: "__name__", Value: "test"})
	err = archiver.ExposeSetTestData(l, [][]int64{{1533687120000, 1}})
	require.Equal(t, nil, err)

	query := prompb.Query{
		StartTimestampMs: 1533682980000,
		EndTimestampMs:   1533704880000,
		Hints: &prompb.ReadHints{
			StartMs: 1533682980000,
			EndMs:   1533704880000,
			Func:    "",
		},
		Matchers: []*prompb.LabelMatcher{
			&prompb.LabelMatcher{
				Type:  prompb.LabelMatcher_EQ,
				Name:  "__name__",
				Value: "test",
			},
		},
	}
	ctx := context.Background()
	result, err := archiver.Query(ctx, &query, 61, 5*time.Minute)
	require.Equal(t, nil, err)
	require.Equal(t, 3, len(result["__name__test"].Samples))
	require.Equal(t, int64(1533683041000), result["__name__test"].Samples[0].Timestamp)
	require.Equal(t, int64(1533687120000), result["__name__test"].Samples[1].Timestamp)
	require.Equal(t, int64(1533687181000), result["__name__test"].Samples[2].Timestamp)
}
