package model

import (
	"regexp"
	"strings"
)

// MatchType defines the type of label matching.
type MatchType int

const (
	MatchEqual     MatchType = iota // =
	MatchNotEqual                   // !=
	MatchRegexp                     // =~
	MatchNotRegexp                  // !~
)

// LabelMatcher represents a matcher for a single label.
type LabelMatcher struct {
	Type  MatchType
	Name  string
	Value string
	re    *regexp.Regexp // Compiled regex for regex matchers
}

// NewLabelMatcher creates a new LabelMatcher.
func NewLabelMatcher(matchType MatchType, name, value string) (*LabelMatcher, error) {
	m := &LabelMatcher{
		Type:  matchType,
		Name:  name,
		Value: value,
	}
	if matchType == MatchRegexp || matchType == MatchNotRegexp {
		re, err := regexp.Compile("^(?:" + value + ")$")
		if err != nil {
			return nil, err
		}
		m.re = re
	}
	return m, nil
}

// Matches checks if the given value matches this matcher.
func (m *LabelMatcher) Matches(value string) bool {
	switch m.Type {
	case MatchEqual:
		return value == m.Value
	case MatchNotEqual:
		return value != m.Value
	case MatchRegexp:
		return m.re.MatchString(value)
	case MatchNotRegexp:
		return !m.re.MatchString(value)
	default:
		return false
	}
}

// String returns a string representation of the matcher.
func (m *LabelMatcher) String() string {
	var op string
	switch m.Type {
	case MatchEqual:
		op = "="
	case MatchNotEqual:
		op = "!="
	case MatchRegexp:
		op = "=~"
	case MatchNotRegexp:
		op = "!~"
	}
	return m.Name + op + `"` + m.Value + `"`
}

// MetricSelector represents a metric name with label matchers.
type MetricSelector struct {
	Name     string
	Matchers []*LabelMatcher
}

// NewMetricSelector creates a new MetricSelector from a metric expression.
// Format: metric_name{label1="value1", label2=~"regex.*"}
func NewMetricSelector(name string, matchers []*LabelMatcher) *MetricSelector {
	return &MetricSelector{
		Name:     name,
		Matchers: matchers,
	}
}

// Matches checks if the given series matches this selector.
func (s *MetricSelector) Matches(series *Series) bool {
	if s.Name != "" && s.Name != series.Name {
		return false
	}
	for _, m := range s.Matchers {
		if !m.Matches(series.Labels[m.Name]) {
			return false
		}
	}
	return true
}

// String returns the string representation of the selector.
func (s *MetricSelector) String() string {
	if len(s.Matchers) == 0 {
		return s.Name
	}
	var sb strings.Builder
	sb.WriteString(s.Name)
	sb.WriteString("{")
	for i, m := range s.Matchers {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(m.String())
	}
	sb.WriteString("}")
	return sb.String()
}

// TimeRange represents a time range for queries.
type TimeRange struct {
	Start int64 // Start timestamp in milliseconds
	End   int64 // End timestamp in milliseconds
}

// Contains checks if the given timestamp is within this range.
func (r TimeRange) Contains(ts int64) bool {
	return ts >= r.Start && ts <= r.End
}

// Overlaps checks if this range overlaps with another range.
func (r TimeRange) Overlaps(other TimeRange) bool {
	return r.Start <= other.End && r.End >= other.Start
}
