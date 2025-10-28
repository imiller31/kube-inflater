package crbenchmark

import (
	"flag"
	"fmt"
	"os"
	"time"

	cfgpkg "kube-inflater/internal/config"
)

// LoadConfigFromFlags loads configuration from command-line flags and environment variables
func LoadConfigFromFlags() *cfgpkg.CRBenchmarkConfig {
	cfg := &cfgpkg.CRBenchmarkConfig{
		Namespace:     getenvStr("NAMESPACE", cfgpkg.DefaultCRNamespace),
		TotalCRs:      getenvInt("TOTAL_CRS", cfgpkg.DefaultCRTotalCRs),
		Concurrency:   getenvInt("CONCURRENCY", cfgpkg.DefaultCRConcurrency),
		RateLimit:     getenvInt("RATE_LIMIT", cfgpkg.DefaultCRRateLimit),
		BatchSize:     getenvInt("BATCH_SIZE", cfgpkg.DefaultCRBatchSize),
		PerfWait:      time.Duration(getenvInt("PERF_WAIT", cfgpkg.DefaultCRPerfWait)) * time.Second,
		PerfTests:     getenvInt("PERF_TESTS", cfgpkg.DefaultCRPerfTests),
		PerfChunkSize: getenvInt("PERF_CHUNK_SIZE", 0), // 0 = no chunking, fetch all at once
		UseCBOR:       getenvStr("USE_CBOR", "") == "true",
	}

	scopeClusterwide := false
	flag.BoolVar(&scopeClusterwide, "clusterwide", false, "Test clusterwide CRD instead of namespaced")
	flag.StringVar(&cfg.Namespace, "namespace", cfg.Namespace, "Namespace for namespaced CRs")
	flag.IntVar(&cfg.TotalCRs, "total", cfg.TotalCRs, "Total CRs to create")
	flag.IntVar(&cfg.Concurrency, "workers", cfg.Concurrency, "Concurrent workers")
	flag.IntVar(&cfg.RateLimit, "rate-limit", cfg.RateLimit, "Max CRs per second")
	flag.IntVar(&cfg.BatchSize, "batch-size", cfg.BatchSize, "CRs per batch for metrics")
	flag.BoolVar(&cfg.CleanupOnly, "cleanup-only", false, "Only cleanup CRs and exit")
	flag.BoolVar(&cfg.PerfOnly, "perf-only", false, "Only run performance tests on existing CRs (skip creation)")
	flag.BoolVar(&cfg.RecreateCRD, "recreate-crd", false, "Force delete and recreate CRD (default: only create if missing)")
	flag.BoolVar(&cfg.SkipPerfTests, "skip-perf-tests", false, "Skip performance tests")

	perfWaitSec := int(cfg.PerfWait / time.Second)
	flag.IntVar(&perfWaitSec, "perf-wait", perfWaitSec, "Seconds to wait before performance tests")
	flag.IntVar(&cfg.PerfTests, "perf-tests", cfg.PerfTests, "Number of performance test calls")
	flag.IntVar(&cfg.PerfChunkSize, "chunk-size", cfg.PerfChunkSize, "Chunk size for LIST operations (0=fetch all, mimics kubectl --chunk-size)")
	flag.BoolVar(&cfg.UseCBOR, "use-cbor", cfg.UseCBOR, "Use CBOR encoding instead of JSON (requires API server support)")

	outputDir := flag.String("output-dir", ".", "Directory to save markdown report")
	flag.Parse()

	cfg.OutputDir = *outputDir
	cfg.PerfWait = time.Duration(perfWaitSec) * time.Second
	
	// Set CRD based on scope
	if scopeClusterwide {
		cfg.CRDName = ClusterwiseCRDName
	} else {
		cfg.CRDName = NamespacedCRDName
	}

	return cfg
}

func getenvInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			return n
		}
	}
	return def
}

func getenvStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}