package main

import "github.com/prometheus/tsdb/labels"

func (archiver *Archiver) ExposeSetTestData(l labels.Labels, points [][]int64) error {
	app := archiver.db.Appender()
	var refs uint64
	var err error
	for _, point := range points {
		if refs != 0 {
			err = app.AddFast(refs, point[0], float64(point[1]))
		} else {
			_, err = app.Add(l, point[0], float64(point[1]))
		}
		if err != nil {
			return err
		}
	}
	err = app.Commit()
	return err
}
