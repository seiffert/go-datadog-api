/*
 * Datadog API for Go
 *
 * Please see the included LICENSE file for licensing information.
 *
 * Copyright 2013 by authors and contributors.
 */

package datadog

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// DataPoint is a tuple of [UNIX timestamp, value]. This has to use floats
// because the value could be non-integer.
type DataPoint [2]float64

// Metric represents a collection of data points that we might send or receive
// on one single metric line.
type Metric struct {
	Metric string      `json:"metric"`
	Points []DataPoint `json:"points"`
	Type   string      `json:"type"`
	Host   string      `json:"host"`
	Tags   []string    `json:"tags"`
}

// reqPostSeries from /api/v1/series
type reqPostSeries struct {
	Series []Metric `json:"series"`
}

// PostSeries takes as input a slice of metrics and then posts them up to the
// server for posting data.
func (self *Client) PostMetrics(series []Metric) error {
	return self.doJsonRequest("POST", "/v1/series",
		reqPostSeries{Series: series}, nil)
}

func (self *Client) QueryMetrics(q string, start, end time.Time) ([]QuerySeries, error) {
	vals := url.Values{}
	vals.Add("start", strconv.FormatInt(start.Unix(), 10))
	vals.Add("to", strconv.FormatInt(end.Unix(), 10))
	vals.Add("query", q)

	var result resQueryMetrics
	err := self.doJsonRequest("GET", fmt.Sprintf("/v1/query?%s", vals.Encode()), nil, &result)
	if err != nil {
		return nil, err
	}

	return result.Series, nil
}

type resQueryMetrics struct {
	Series []QuerySeries `json:"series"`
}

type QuerySeries struct {
	Expression string
	Metric     string
	Aggr       string
	Scope      map[string]string
	Start      time.Time
	End        time.Time
	PointList  []DataPoint
}

func (s *QuerySeries) UnmarshalJSON(data []byte) error {
	dst := struct {
		Expression string      `json:"expression"`
		Metric     string      `json:"metric"`
		Aggr       string      `json:"aggr"`
		Scope      string      `json:"scope"`
		Start      int         `json:"start"`
		End        int         `json:"end"`
		PointList  []DataPoint `json:"pointlist"`
	}{}

	if err := json.Unmarshal(data, &dst); err != nil {
		return err
	}

	s.Expression = dst.Expression
	s.Metric = dst.Metric
	s.Aggr = dst.Aggr
	s.Start = time.Unix(int64(dst.Start), 0)
	s.End = time.Unix(int64(dst.End), 0)
	s.PointList = dst.PointList
	s.Scope = map[string]string{}

	for _, scopeItem := range strings.Split(dst.Scope, ",") {
		i := strings.Split(scopeItem, ":")
		s.Scope[i[0]] = i[1]
	}

	return nil
}
