package config

import "time"

// CRBenchmarkConfig holds configuration for CR benchmarking
type CRBenchmarkConfig struct {
	// CRD Configuration
	CRDName      string // e.g., "myresources.example.com/v1"
	TemplatePath string // Path to CR template YAML

	// Scale Configuration
	TotalCRs    int // Total CRs to create
	Concurrency int // Number of concurrent workers
	RateLimit   int // Max CRs per second
	BatchSize   int // CRs per batch for metrics reporting

	// Namespace
	Namespace string

	// Performance Testing
	PerfWait      time.Duration // Wait before perf tests
	PerfTests     int           // Number of perf test calls
	PerfChunkSize int           // Chunk size for list operations (0 = no chunking)
	UseCBOR       bool          // Use CBOR encoding instead of JSON
	SkipPerfTests bool          // Skip performance tests

	// Cleanup
	CleanupOnly  bool // Only cleanup and exit
	PerfOnly     bool // Only run performance tests, skip creation
	RecreateCRD  bool // Force delete and recreate CRD (default: only create if missing)

	// Output
	OutputDir string // Directory for markdown report
}

// CR Benchmark defaults
const (
	DefaultCRNamespace    = "cr-benchmark"
	DefaultCRTotalCRs     = 1000
	DefaultCRConcurrency  = 50
	DefaultCRRateLimit    = 500
	DefaultCRBatchSize    = 1000
	DefaultCRPerfWait     = 30
	DefaultCRPerfTests    = 5
)