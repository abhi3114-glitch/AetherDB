// Package model defines the core data types for AetherDB time-series database.
package model

import (
	"hash/fnv"
	"sort"
	"strings"
)

// Sample represents a single data point in a time series.
type Sample struct {
	Timestamp int64   // Unix timestamp in milliseconds
	Value     float64 // The metric value
}

// Samples is a slice of Sample that implements sort.Interface for sorting by timestamp.
type Samples []Sample

func (s Samples) Len() int           { return len(s) }
func (s Samples) Less(i, j int) bool { return s[i].Timestamp < s[j].Timestamp }
func (s Samples) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// Labels represents a set of key-value pairs that identify a time series.
type Labels map[string]string

// Hash returns a unique hash for the labels.
func (l Labels) Hash() uint64 {
	h := fnv.New64a()
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(l[k]))
		h.Write([]byte{0})
	}
	return h.Sum64()
}

// String returns a string representation of labels in the format {key1="value1", key2="value2"}.
func (l Labels) String() string {
	if len(l) == 0 {
		return "{}"
	}
	keys := make([]string, 0, len(l))
	for k := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	sb.WriteString("{")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(k)
		sb.WriteString(`="`)
		sb.WriteString(l[k])
		sb.WriteString(`"`)
	}
	sb.WriteString("}")
	return sb.String()
}

// Match checks if the labels match the given matchers.
func (l Labels) Match(matchers []LabelMatcher) bool {
	for _, m := range matchers {
		if !m.Matches(l[m.Name]) {
			return false
		}
	}
	return true
}

// Series represents a time series identified by a metric name and labels.
type Series struct {
	Name   string // Metric name (e.g., "cpu_usage")
	Labels Labels // Key-value labels
	hash   uint64 // Cached hash for fast lookup
}

// NewSeries creates a new Series with the given name and labels.
func NewSeries(name string, labels Labels) *Series {
	if labels == nil {
		labels = make(Labels)
	}
	s := &Series{
		Name:   name,
		Labels: labels,
	}
	s.computeHash()
	return s
}

// computeHash computes and caches the hash for this series.
func (s *Series) computeHash() {
	h := fnv.New64a()
	h.Write([]byte(s.Name))
	h.Write([]byte{0})

	keys := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(s.Labels[k]))
		h.Write([]byte{0})
	}
	s.hash = h.Sum64()
}

// Hash returns the cached hash for this series.
func (s *Series) Hash() uint64 {
	return s.hash
}

// String returns a string representation of the series.
func (s *Series) String() string {
	return s.Name + s.Labels.String()
}

// SeriesRef is a reference to a series in the index.
type SeriesRef uint64

// TimeSeries combines a series with its samples.
type TimeSeries struct {
	Series  *Series
	Samples Samples
}

// Point represents a single point in query results.
type Point struct {
	Timestamp int64
	Value     float64
}

// QueryResult holds the result of a query operation.
type QueryResult struct {
	Series  *Series
	Samples []Sample
}
