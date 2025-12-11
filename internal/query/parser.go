// Package query implements the query engine for AetherDB.
package query

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"aetherdb/internal/model"
)

// Query represents a parsed query.
type Query struct {
	MetricName string
	Matchers   []*model.LabelMatcher
	Duration   time.Duration // Time range like [5m]
	StartTime  int64         // Absolute start time (ms)
	EndTime    int64         // Absolute end time (ms)
	Function   string        // Aggregation function: sum, avg, max, min, delta
	Step       time.Duration // Step interval for range queries
}

// Parser parses query strings into Query objects.
type Parser struct{}

// NewParser creates a new Parser.
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses a query string.
// Format: func(metric_name{label1="value1", label2=~"regex.*"}[duration])
// Examples:
//   - cpu_usage
//   - cpu_usage{host="server1"}
//   - sum(cpu_usage{host="server1"}[5m])
//   - avg(memory_used[1h])
func (p *Parser) Parse(input string) (*Query, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, fmt.Errorf("empty query")
	}

	q := &Query{
		EndTime: time.Now().UnixMilli(),
	}

	// Check for function wrapper
	funcMatch := regexp.MustCompile(`^(\w+)\((.*)\)$`)
	if matches := funcMatch.FindStringSubmatch(input); matches != nil {
		q.Function = strings.ToLower(matches[1])
		if !isValidFunction(q.Function) {
			return nil, fmt.Errorf("unknown function: %s", q.Function)
		}
		input = matches[2]
	}

	// Parse duration [5m], [1h], etc.
	durationMatch := regexp.MustCompile(`\[(\d+)([smhd])\]$`)
	if matches := durationMatch.FindStringSubmatch(input); matches != nil {
		value, _ := strconv.Atoi(matches[1])
		unit := matches[2]

		switch unit {
		case "s":
			q.Duration = time.Duration(value) * time.Second
		case "m":
			q.Duration = time.Duration(value) * time.Minute
		case "h":
			q.Duration = time.Duration(value) * time.Hour
		case "d":
			q.Duration = time.Duration(value) * 24 * time.Hour
		}

		q.StartTime = q.EndTime - q.Duration.Milliseconds()
		input = durationMatch.ReplaceAllString(input, "")
	} else {
		// Default to last 5 minutes
		q.Duration = 5 * time.Minute
		q.StartTime = q.EndTime - q.Duration.Milliseconds()
	}

	// Parse metric name and labels
	labelStart := strings.Index(input, "{")
	if labelStart == -1 {
		// Just metric name
		q.MetricName = strings.TrimSpace(input)
	} else {
		q.MetricName = strings.TrimSpace(input[:labelStart])

		// Parse labels
		labelEnd := strings.LastIndex(input, "}")
		if labelEnd == -1 {
			return nil, fmt.Errorf("unclosed label selector")
		}

		labelsStr := input[labelStart+1 : labelEnd]
		matchers, err := parseLabels(labelsStr)
		if err != nil {
			return nil, fmt.Errorf("parse labels: %w", err)
		}
		q.Matchers = matchers
	}

	if q.MetricName == "" {
		return nil, fmt.Errorf("missing metric name")
	}

	return q, nil
}

// parseLabels parses label matchers from a string.
func parseLabels(s string) ([]*model.LabelMatcher, error) {
	if s == "" {
		return nil, nil
	}

	var matchers []*model.LabelMatcher

	// Split by comma, but respect quotes
	parts := splitLabels(s)

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		matcher, err := parseLabelMatcher(part)
		if err != nil {
			return nil, err
		}
		matchers = append(matchers, matcher)
	}

	return matchers, nil
}

// splitLabels splits label string by comma, respecting quotes.
func splitLabels(s string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false

	for _, r := range s {
		if r == '"' {
			inQuote = !inQuote
			current.WriteRune(r)
		} else if r == ',' && !inQuote {
			parts = append(parts, current.String())
			current.Reset()
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// parseLabelMatcher parses a single label matcher.
func parseLabelMatcher(s string) (*model.LabelMatcher, error) {
	// Try different operators in order of specificity
	operators := []struct {
		op        string
		matchType model.MatchType
	}{
		{"=~", model.MatchRegexp},
		{"!~", model.MatchNotRegexp},
		{"!=", model.MatchNotEqual},
		{"=", model.MatchEqual},
	}

	for _, op := range operators {
		idx := strings.Index(s, op.op)
		if idx != -1 {
			name := strings.TrimSpace(s[:idx])
			value := strings.TrimSpace(s[idx+len(op.op):])

			// Remove quotes
			value = strings.Trim(value, `"'`)

			matcher, err := model.NewLabelMatcher(op.matchType, name, value)
			if err != nil {
				return nil, err
			}
			return matcher, nil
		}
	}

	return nil, fmt.Errorf("invalid label matcher: %s", s)
}

// isValidFunction checks if a function name is valid.
func isValidFunction(name string) bool {
	switch name {
	case "sum", "avg", "max", "min", "delta", "count", "rate":
		return true
	default:
		return false
	}
}

// Selector creates a MetricSelector from the query.
func (q *Query) Selector() *model.MetricSelector {
	return model.NewMetricSelector(q.MetricName, q.Matchers)
}

// TimeRange returns the time range for the query.
func (q *Query) TimeRange() (start, end int64) {
	return q.StartTime, q.EndTime
}

// SetTimeRange sets explicit start and end times.
func (q *Query) SetTimeRange(start, end int64) {
	q.StartTime = start
	q.EndTime = end
	q.Duration = time.Duration(end-start) * time.Millisecond
}
