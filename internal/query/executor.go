package query

import (
	"fmt"
	"sort"

	"aetherdb/internal/model"
	"aetherdb/internal/storage"
)

// Result represents the result of a query.
type Result struct {
	Type   ResultType
	Scalar float64        // For scalar results
	Vector []VectorSample // For instant vector results
	Matrix []MatrixSeries // For range vector results
}

// ResultType indicates the type of query result.
type ResultType int

const (
	ResultTypeScalar ResultType = iota
	ResultTypeVector
	ResultTypeMatrix
)

// VectorSample represents a single sample in a vector result.
type VectorSample struct {
	Series    *model.Series
	Timestamp int64
	Value     float64
}

// MatrixSeries represents a series with samples in a matrix result.
type MatrixSeries struct {
	Series  *model.Series
	Samples []model.Sample
}

// Executor executes queries against the storage engine.
type Executor struct {
	engine *storage.Engine
	parser *Parser
}

// NewExecutor creates a new Executor.
func NewExecutor(engine *storage.Engine) *Executor {
	return &Executor{
		engine: engine,
		parser: NewParser(),
	}
}

// Execute parses and executes a query string.
func (e *Executor) Execute(queryStr string) (*Result, error) {
	query, err := e.parser.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}

	return e.ExecuteQuery(query)
}

// ExecuteQuery executes a parsed query.
func (e *Executor) ExecuteQuery(query *Query) (*Result, error) {
	// Fetch data from storage
	selector := query.Selector()
	start, end := query.TimeRange()

	results, err := e.engine.Query(selector, start, end)
	if err != nil {
		return nil, fmt.Errorf("storage query: %w", err)
	}

	// No aggregation function - return raw data as matrix
	if query.Function == "" {
		return e.buildMatrixResult(results), nil
	}

	// Apply aggregation function
	aggFunc, ok := Aggregations[query.Function]
	if !ok {
		return nil, fmt.Errorf("unknown function: %s", query.Function)
	}

	return e.applyAggregation(results, aggFunc), nil
}

// buildMatrixResult creates a matrix result from query results.
func (e *Executor) buildMatrixResult(results []model.QueryResult) *Result {
	matrix := make([]MatrixSeries, len(results))
	for i, r := range results {
		// Sort samples by timestamp
		samples := make([]model.Sample, len(r.Samples))
		copy(samples, r.Samples)
		sort.Slice(samples, func(a, b int) bool {
			return samples[a].Timestamp < samples[b].Timestamp
		})

		matrix[i] = MatrixSeries{
			Series:  r.Series,
			Samples: samples,
		}
	}

	return &Result{
		Type:   ResultTypeMatrix,
		Matrix: matrix,
	}
}

// applyAggregation applies an aggregation function to query results.
func (e *Executor) applyAggregation(results []model.QueryResult, aggFunc AggFunc) *Result {
	// Check if we have any results
	if len(results) == 0 {
		return &Result{
			Type:   ResultTypeScalar,
			Scalar: 0,
		}
	}

	// For a single series, return a scalar
	if len(results) == 1 {
		return &Result{
			Type:   ResultTypeScalar,
			Scalar: aggFunc(results[0].Samples),
		}
	}

	// For multiple series, return a vector with aggregated values
	vector := make([]VectorSample, len(results))
	for i, r := range results {
		var ts int64
		if len(r.Samples) > 0 {
			ts = r.Samples[len(r.Samples)-1].Timestamp
		}

		vector[i] = VectorSample{
			Series:    r.Series,
			Timestamp: ts,
			Value:     aggFunc(r.Samples),
		}
	}

	return &Result{
		Type:   ResultTypeVector,
		Vector: vector,
	}
}

// ExecuteInstant executes an instant query at a specific timestamp.
func (e *Executor) ExecuteInstant(queryStr string, timestamp int64) (*Result, error) {
	query, err := e.parser.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}

	// Set time range to a small window around the timestamp
	lookback := int64(5 * 60 * 1000) // 5 minutes in milliseconds
	query.SetTimeRange(timestamp-lookback, timestamp)

	return e.ExecuteQuery(query)
}

// ExecuteRange executes a range query over a time period.
func (e *Executor) ExecuteRange(queryStr string, start, end int64, step int64) (*Result, error) {
	query, err := e.parser.Parse(queryStr)
	if err != nil {
		return nil, fmt.Errorf("parse query: %w", err)
	}

	query.SetTimeRange(start, end)

	// For range queries with steps, we'd evaluate at each step
	// For now, we just return the full range
	return e.ExecuteQuery(query)
}

// QueryLabels returns all label values for a given label name.
func (e *Executor) QueryLabels(labelName string) ([]string, error) {
	// This would require a label index in the storage engine
	// For now, return empty
	return nil, nil
}

// QuerySeries returns all series matching the given selectors.
func (e *Executor) QuerySeries(matchers []*model.LabelMatcher, start, end int64) ([]*model.Series, error) {
	selector := model.NewMetricSelector("", matchers)
	results, err := e.engine.Query(selector, start, end)
	if err != nil {
		return nil, err
	}

	series := make([]*model.Series, len(results))
	for i, r := range results {
		series[i] = r.Series
	}
	return series, nil
}
