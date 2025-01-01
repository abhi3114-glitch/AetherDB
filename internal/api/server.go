// Package api implements the REST API for AetherDB.
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"aetherdb/internal/downsample"
	"aetherdb/internal/model"
	"aetherdb/internal/query"
	"aetherdb/internal/storage"

	"github.com/gorilla/mux"
)

// Server is the HTTP API server.
type Server struct {
	engine    *storage.Engine
	executor  *query.Executor
	scheduler *downsample.Scheduler
	router    *mux.Router
	addr      string
}

// NewServer creates a new API server.
func NewServer(engine *storage.Engine, scheduler *downsample.Scheduler, addr string) *Server {
	s := &Server{
		engine:    engine,
		executor:  query.NewExecutor(engine),
		scheduler: scheduler,
		router:    mux.NewRouter(),
		addr:      addr,
	}

	s.setupRoutes()
	return s
}

// setupRoutes configures the HTTP routes.
func (s *Server) setupRoutes() {
	// API v1 routes
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Write endpoint
	api.HandleFunc("/write", s.handleWrite).Methods("POST")

	// Query endpoints
	api.HandleFunc("/query", s.handleQuery).Methods("GET", "POST")
	api.HandleFunc("/query_range", s.handleQueryRange).Methods("GET", "POST")

	// Metadata endpoints
	api.HandleFunc("/series", s.handleSeries).Methods("GET")
	api.HandleFunc("/labels", s.handleLabels).Methods("GET")
	api.HandleFunc("/label/{name}/values", s.handleLabelValues).Methods("GET")

	// Status endpoint
	api.HandleFunc("/status", s.handleStatus).Methods("GET")

	// Admin endpoints
	api.HandleFunc("/admin/flush", s.handleFlush).Methods("POST")
	api.HandleFunc("/admin/compact", s.handleCompact).Methods("POST")
	api.HandleFunc("/admin/downsample", s.handleDownsample).Methods("POST")

	// Health check
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	log.Printf("Starting AetherDB API server on %s", s.addr)
	return http.ListenAndServe(s.addr, s.router)
}

// handleWrite handles the write endpoint.
// Line protocol format: metric_name,label1=value1,label2=value2 value=123.45 timestamp
func (s *Server) handleWrite(w http.ResponseWriter, r *http.Request) {
	body := make([]byte, r.ContentLength)
	if _, err := r.Body.Read(body); err != nil && err.Error() != "EOF" {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	lines := strings.Split(string(body), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		series, sample, err := parseLineProtocol(line)
		if err != nil {
			http.Error(w, fmt.Sprintf("Parse error: %v", err), http.StatusBadRequest)
			return
		}

		if err := s.engine.Write(series, []model.Sample{sample}); err != nil {
			http.Error(w, fmt.Sprintf("Write error: %v", err), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

// parseLineProtocol parses a line protocol string.
// Format: metric_name,label1=value1,label2=value2 value=123.45 timestamp_ns
// Or simpler: metric_name,label1=value1 123.45 timestamp_ms
func parseLineProtocol(line string) (*model.Series, model.Sample, error) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return nil, model.Sample{}, fmt.Errorf("invalid line protocol")
	}

	// Parse metric and labels
	metricParts := strings.Split(parts[0], ",")
	metricName := metricParts[0]

	labels := make(model.Labels)
	for _, lp := range metricParts[1:] {
		kv := strings.SplitN(lp, "=", 2)
		if len(kv) == 2 {
			labels[kv[0]] = kv[1]
		}
	}

	series := model.NewSeries(metricName, labels)

	// Parse value
	var value float64
	valuePart := parts[1]
	if strings.HasPrefix(valuePart, "value=") {
		valuePart = strings.TrimPrefix(valuePart, "value=")
	}
	var err error
	value, err = strconv.ParseFloat(valuePart, 64)
	if err != nil {
		return nil, model.Sample{}, fmt.Errorf("invalid value: %s", valuePart)
	}

	// Parse timestamp (optional, default to now)
	var timestamp int64
	if len(parts) >= 3 {
		timestamp, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return nil, model.Sample{}, fmt.Errorf("invalid timestamp: %s", parts[2])
		}
		// Convert nanoseconds to milliseconds if needed
		if timestamp > 1e15 {
			timestamp = timestamp / 1e6
		}
	} else {
		timestamp = time.Now().UnixMilli()
	}

	sample := model.Sample{
		Timestamp: timestamp,
		Value:     value,
	}

	return series, sample, nil
}

// handleQuery handles instant queries.
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	queryStr := r.URL.Query().Get("query")
	if queryStr == "" {
		if r.Method == "POST" {
			r.ParseForm()
			queryStr = r.FormValue("query")
		}
	}

	if queryStr == "" {
		http.Error(w, "Missing query parameter", http.StatusBadRequest)
		return
	}

	// Parse optional time parameter
	timeStr := r.URL.Query().Get("time")
	var timestamp int64
	if timeStr != "" {
		t, err := parseTime(timeStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid time: %v", err), http.StatusBadRequest)
			return
		}
		timestamp = t
	} else {
		timestamp = time.Now().UnixMilli()
	}

	result, err := s.executor.ExecuteInstant(queryStr, timestamp)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusBadRequest)
		return
	}

	writeJSONResponse(w, result)
}

// handleQueryRange handles range queries.
func (s *Server) handleQueryRange(w http.ResponseWriter, r *http.Request) {
	queryStr := r.URL.Query().Get("query")
	if queryStr == "" {
		http.Error(w, "Missing query parameter", http.StatusBadRequest)
		return
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")
	stepStr := r.URL.Query().Get("step")

	start, err := parseTime(startStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid start: %v", err), http.StatusBadRequest)
		return
	}

	end, err := parseTime(endStr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Invalid end: %v", err), http.StatusBadRequest)
		return
	}

	var step int64 = 60000 // Default 1 minute in ms
	if stepStr != "" {
		stepDur, err := parseDuration(stepStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid step: %v", err), http.StatusBadRequest)
			return
		}
		step = stepDur.Milliseconds()
	}

	result, err := s.executor.ExecuteRange(queryStr, start, end, step)
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusBadRequest)
		return
	}

	writeJSONResponse(w, result)
}

// handleSeries lists all series matching the given selectors.
func (s *Server) handleSeries(w http.ResponseWriter, r *http.Request) {
	// Get match[] parameters
	matches := r.URL.Query()["match[]"]
	if len(matches) == 0 {
		matches = []string{""} // Match all
	}

	startStr := r.URL.Query().Get("start")
	endStr := r.URL.Query().Get("end")

	start := time.Now().Add(-24 * time.Hour).UnixMilli()
	end := time.Now().UnixMilli()

	if startStr != "" {
		var err error
		start, err = parseTime(startStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid start: %v", err), http.StatusBadRequest)
			return
		}
	}
	if endStr != "" {
		var err error
		end, err = parseTime(endStr)
		if err != nil {
			http.Error(w, fmt.Sprintf("Invalid end: %v", err), http.StatusBadRequest)
			return
		}
	}

	var allSeries []*model.Series
	for _, match := range matches {
		selector := model.NewMetricSelector(match, nil)
		results, err := s.engine.Query(selector, start, end)
		if err != nil {
			http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
			return
		}

		for _, r := range results {
			allSeries = append(allSeries, r.Series)
		}
	}

	response := make([]map[string]string, len(allSeries))
	for i, series := range allSeries {
		resp := make(map[string]string)
		resp["__name__"] = series.Name
		for k, v := range series.Labels {
			resp[k] = v
		}
		response[i] = resp
	}

	writeJSONResponse(w, map[string]interface{}{
		"status": "success",
		"data":   response,
	})
}

// handleLabels lists all label names.
func (s *Server) handleLabels(w http.ResponseWriter, r *http.Request) {
	// This would require a label index
	// For now, return empty
	writeJSONResponse(w, map[string]interface{}{
		"status": "success",
		"data":   []string{},
	})
}

// handleLabelValues lists values for a label.
func (s *Server) handleLabelValues(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	_ = vars["name"]

	// This would require a label index
	// For now, return empty
	writeJSONResponse(w, map[string]interface{}{
		"status": "success",
		"data":   []string{},
	})
}

// handleStatus returns database status.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	status := s.engine.Status()

	response := map[string]interface{}{
		"status": "success",
		"data": map[string]interface{}{
			"numSeries":    status.NumSeries,
			"numBlocks":    status.NumBlocks,
			"totalSamples": status.TotalSamples,
			"memtable": map[string]interface{}{
				"size":    status.MemTableSize,
				"minTime": status.MemTableMinTime,
				"maxTime": status.MemTableMaxTime,
			},
		},
	}

	writeJSONResponse(w, response)
}

// handleHealth returns health status.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

// handleFlush forces a memtable flush.
func (s *Server) handleFlush(w http.ResponseWriter, r *http.Request) {
	if err := s.engine.Flush(); err != nil {
		http.Error(w, fmt.Sprintf("Flush error: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	writeJSONResponse(w, map[string]string{"status": "success"})
}

// handleCompact forces compaction.
func (s *Server) handleCompact(w http.ResponseWriter, r *http.Request) {
	// Compaction is handled in the background
	w.WriteHeader(http.StatusOK)
	writeJSONResponse(w, map[string]string{"status": "triggered"})
}

// handleDownsample forces downsampling.
func (s *Server) handleDownsample(w http.ResponseWriter, r *http.Request) {
	if s.scheduler != nil {
		if err := s.scheduler.ForceDownsample(); err != nil {
			http.Error(w, fmt.Sprintf("Downsample error: %v", err), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	writeJSONResponse(w, map[string]string{"status": "success"})
}

// parseTime parses a time string (Unix timestamp or RFC3339).
func parseTime(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty time string")
	}

	// Try Unix timestamp (seconds or milliseconds)
	if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
		// If less than year 3000 in seconds, assume seconds
		if ts < 32503680000 {
			return ts * 1000, nil
		}
		return ts, nil
	}

	// Try RFC3339
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return 0, err
	}
	return t.UnixMilli(), nil
}

// parseDuration parses a duration string (e.g., "5m", "1h").
func parseDuration(s string) (time.Duration, error) {
	// Handle plain numbers as seconds
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(d * float64(time.Second)), nil
	}

	// Try Go duration format
	return time.ParseDuration(s)
}

// writeJSONResponse writes a JSON response.
func writeJSONResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}
