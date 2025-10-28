package crperf

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics tracks CR creation performance metrics
type Metrics struct {
	created      int64
	failed       int64
	totalLatency int64 // nanoseconds
	startTime    time.Time
	mu           sync.Mutex
	latencies    []time.Duration // Sampled for percentile calculation

	// Response size tracking
	totalResponseSize int64
	responseSizes     []int64
	minResponseSize   int64
	maxResponseSize   int64

	// Latency bounds
	minLatency int64
	maxLatency int64

	// Status code tracking
	statusCodes map[int]int64
	statusMu    sync.Mutex

	// Error categorization
	errorCategories map[string]int64
	errorMu         sync.Mutex
	errorSamples    map[string]string // Sample error message per category
}

// NewMetrics creates a new metrics tracker
func NewMetrics() *Metrics {
	return &Metrics{
		startTime:       time.Now(),
		latencies:       make([]time.Duration, 0, 100000),
		responseSizes:   make([]int64, 0, 100000),
		statusCodes:     make(map[int]int64),
		errorCategories: make(map[string]int64),
		errorSamples:    make(map[string]string),
		minLatency:      1<<63 - 1, // Max int64
		maxLatency:      0,
		minResponseSize: 1<<63 - 1,
		maxResponseSize: 0,
	}
}

// Record records a CR creation attempt with enhanced metrics
func (m *Metrics) Record(latency time.Duration, responseSize int64, statusCode int, err error) {
	latencyNanos := int64(latency)

	if err == nil {
		atomic.AddInt64(&m.created, 1)
		atomic.AddInt64(&m.totalLatency, latencyNanos)
		atomic.AddInt64(&m.totalResponseSize, responseSize)

		// Update min/max latency
		for {
			current := atomic.LoadInt64(&m.minLatency)
			if latencyNanos >= current || atomic.CompareAndSwapInt64(&m.minLatency, current, latencyNanos) {
				break
			}
		}
		for {
			current := atomic.LoadInt64(&m.maxLatency)
			if latencyNanos <= current || atomic.CompareAndSwapInt64(&m.maxLatency, current, latencyNanos) {
				break
			}
		}

		// Update min/max response size
		for {
			current := atomic.LoadInt64(&m.minResponseSize)
			if responseSize >= current || atomic.CompareAndSwapInt64(&m.minResponseSize, current, responseSize) {
				break
			}
		}
		for {
			current := atomic.LoadInt64(&m.maxResponseSize)
			if responseSize <= current || atomic.CompareAndSwapInt64(&m.maxResponseSize, current, responseSize) {
				break
			}
		}

		// Sample 1% of latencies and response sizes for percentile calculation
		m.mu.Lock()
		if len(m.latencies) < 100000 && (atomic.LoadInt64(&m.created)%100 == 0) {
			m.latencies = append(m.latencies, latency)
			m.responseSizes = append(m.responseSizes, responseSize)
		}
		m.mu.Unlock()

		// Track status codes
		m.statusMu.Lock()
		m.statusCodes[statusCode]++
		m.statusMu.Unlock()
	} else {
		atomic.AddInt64(&m.failed, 1)

		// Categorize error
		category := categorizeError(err)
		m.errorMu.Lock()
		m.errorCategories[category]++
		if _, exists := m.errorSamples[category]; !exists {
			m.errorSamples[category] = err.Error()
		}
		m.errorMu.Unlock()

		// Track failed status code
		if statusCode > 0 {
			m.statusMu.Lock()
			m.statusCodes[statusCode]++
			m.statusMu.Unlock()
		}
	}
}

// categorizeError categorizes errors into types
func categorizeError(err error) string {
	if err == nil {
		return "Success"
	}

	errStr := err.Error()

	// Check for common error patterns
	switch {
	case contains(errStr, "timeout", "deadline exceeded", "context deadline"):
		return "Timeout"
	case contains(errStr, "429", "rate limit", "too many requests"):
		return "RateLimit"
	case contains(errStr, "409", "conflict", "already exists"):
		return "Conflict"
	case contains(errStr, "400", "invalid", "validation", "required"):
		return "Validation"
	case contains(errStr, "403", "forbidden", "unauthorized", "permission"):
		return "Authorization"
	case contains(errStr, "404", "not found"):
		return "NotFound"
	case contains(errStr, "500", "503", "internal server error", "service unavailable"):
		return "ServerError"
	case contains(errStr, "connection refused", "connection reset", "network"):
		return "NetworkError"
	default:
		return "Other"
	}
}

// contains checks if any of the substrings exist in the string (case-insensitive)
func contains(s string, substrings ...string) bool {
	s = toLower(s)
	for _, substr := range substrings {
		if containsSubstr(s, toLower(substr)) {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	result := make([]byte, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		result[i] = c
	}
	return string(result)
}

func containsSubstr(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(substr) > len(s) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		match := true
		for j := 0; j < len(substr); j++ {
			if s[i+j] != substr[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// GetCreated returns number of successfully created CRs
func (m *Metrics) GetCreated() int64 {
	return atomic.LoadInt64(&m.created)
}

// GetFailed returns number of failed CR creations
func (m *Metrics) GetFailed() int64 {
	return atomic.LoadInt64(&m.failed)
}

// Stats holds summary statistics
type Stats struct {
	Created     int64
	Failed      int64
	SuccessRate float64
	Duration    time.Duration
	Rate        float64
	AvgLatency  time.Duration
	MinLatency  time.Duration
	MaxLatency  time.Duration
	P50Latency  time.Duration
	P95Latency  time.Duration
	P99Latency  time.Duration

	// Response size metrics
	AvgResponseSize int64
	MinResponseSize int64
	MaxResponseSize int64
	P50ResponseSize int64
	P95ResponseSize int64
	P99ResponseSize int64
	TotalBandwidth  int64

	// Status code distribution
	StatusCodes map[int]int64

	// Error breakdown
	ErrorCategories map[string]int64
	ErrorSamples    map[string]string
}

// GetStats returns summary statistics
func (m *Metrics) GetStats() Stats {
	created := atomic.LoadInt64(&m.created)
	failed := atomic.LoadInt64(&m.failed)
	totalLat := atomic.LoadInt64(&m.totalLatency)
	totalSize := atomic.LoadInt64(&m.totalResponseSize)
	minLat := atomic.LoadInt64(&m.minLatency)
	maxLat := atomic.LoadInt64(&m.maxLatency)
	minSize := atomic.LoadInt64(&m.minResponseSize)
	maxSize := atomic.LoadInt64(&m.maxResponseSize)
	duration := time.Since(m.startTime)

	stats := Stats{
		Created:  created,
		Failed:   failed,
		Duration: duration,
	}

	// Reset min values if no successes
	if created == 0 {
		minLat = 0
		minSize = 0
	}

	stats.MinLatency = time.Duration(minLat)
	stats.MaxLatency = time.Duration(maxLat)
	stats.MinResponseSize = minSize
	stats.MaxResponseSize = maxSize
	stats.TotalBandwidth = totalSize

	total := created + failed
	if total > 0 {
		stats.SuccessRate = float64(created) / float64(total) * 100
	}

	if duration.Seconds() > 0 {
		stats.Rate = float64(created) / duration.Seconds()
	}

	if created > 0 {
		stats.AvgLatency = time.Duration(totalLat / created)
		stats.AvgResponseSize = totalSize / created
	}

	// Calculate latency percentiles from samples
	m.mu.Lock()
	latencySamples := make([]time.Duration, len(m.latencies))
	copy(latencySamples, m.latencies)
	responseSizeSamples := make([]int64, len(m.responseSizes))
	copy(responseSizeSamples, m.responseSizes)
	m.mu.Unlock()

	if len(latencySamples) > 0 {
		sort.Slice(latencySamples, func(i, j int) bool {
			return latencySamples[i] < latencySamples[j]
		})
		stats.P50Latency = latencySamples[len(latencySamples)*50/100]
		stats.P95Latency = latencySamples[len(latencySamples)*95/100]
		stats.P99Latency = latencySamples[len(latencySamples)*99/100]
	}

	if len(responseSizeSamples) > 0 {
		sort.Slice(responseSizeSamples, func(i, j int) bool {
			return responseSizeSamples[i] < responseSizeSamples[j]
		})
		stats.P50ResponseSize = responseSizeSamples[len(responseSizeSamples)*50/100]
		stats.P95ResponseSize = responseSizeSamples[len(responseSizeSamples)*95/100]
		stats.P99ResponseSize = responseSizeSamples[len(responseSizeSamples)*99/100]
	}

	// Copy status codes
	m.statusMu.Lock()
	stats.StatusCodes = make(map[int]int64, len(m.statusCodes))
	for k, v := range m.statusCodes {
		stats.StatusCodes[k] = v
	}
	m.statusMu.Unlock()

	// Copy error categories
	m.errorMu.Lock()
	stats.ErrorCategories = make(map[string]int64, len(m.errorCategories))
	for k, v := range m.errorCategories {
		stats.ErrorCategories[k] = v
	}
	stats.ErrorSamples = make(map[string]string, len(m.errorSamples))
	for k, v := range m.errorSamples {
		stats.ErrorSamples[k] = v
	}
	m.errorMu.Unlock()

	return stats
}