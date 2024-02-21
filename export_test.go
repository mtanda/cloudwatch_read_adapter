package main

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
)

func (archiver *Archiver) ExposeSetTestData(l labels.Labels, points [][]int64) error {
	ctx := context.Background()
	app := archiver.db.Appender(ctx)
	var refs storage.SeriesRef
	var err error
	for _, point := range points {
		refs, err = app.Append(refs, l, point[0], float64(point[1]))
		if err != nil {
			return err
		}
	}
	err = app.Commit()
	return err
}
