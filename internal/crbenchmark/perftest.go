package crbenchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	cfgpkg "kube-inflater/internal/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ReadPerfMetrics holds read operation performance metrics
type ReadPerfMetrics struct {
	Operation   string // "LIST", "GET", or "UPDATE"
	ChunkSize   int    // For LIST operations (0 = fetch all at once)
	Count       int    // Number of items returned
	UsedCBOR    bool   // Whether CBOR encoding was used
	AvgLatency  time.Duration
	MinLatency  time.Duration
	MaxLatency  time.Duration
	P50Latency  time.Duration
	P95Latency  time.Duration
	P99Latency  time.Duration
	AvgSize     int64
	Samples     int
	SuccessRate float64
}

// RunPerformanceTests executes various read/write performance tests
func RunPerformanceTests(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig) []ReadPerfMetrics {
	if cfg.SkipPerfTests {
		LogInfo("Skipping performance tests (--skip-perf-tests)")
		return nil
	}

	LogPerf(fmt.Sprintf("Waiting %v before performance tests...", cfg.PerfWait))
	time.Sleep(cfg.PerfWait)

	gvr := GetGVR(cfg.CRDName)

	// Get total CR count first using pagination
	var totalCRs int
	continueToken := ""
	for {
		listOpts := metav1.ListOptions{
			Limit:    1000000,
			Continue: continueToken,
		}

		var list *unstructured.UnstructuredList
		var err error
		if cfg.CRDName == NamespacedCRDName {
			list, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).
				List(ctx, listOpts)
		} else {
			list, err = clients.DynamicClient.Resource(gvr).
				List(ctx, listOpts)
		}

		if err != nil || list == nil {
			break
		}

		totalCRs += len(list.Items)

		continueToken = list.GetContinue()
		if continueToken == "" {
			break
		}
	}

	LogPerf(fmt.Sprintf("Running read performance tests on %d CRs...", totalCRs))

	var results []ReadPerfMetrics

	// Test LIST operations at different chunk sizes (mimicking kubectl --chunk-size)
	// 0 means fetch all at once (no chunking)
	chunkSizes := []int{100000, 1000000, 0}

	// If user specified a chunk size, test only that one
	if cfg.PerfChunkSize > 0 {
		chunkSizes = []int{cfg.PerfChunkSize}
	}

	for _, chunkSize := range chunkSizes {
		if chunkSize == 0 {
			LogPerf("Testing LIST operations with no chunking...")
		} else {
			LogPerf(fmt.Sprintf("Testing LIST operations with chunk size: %d...", chunkSize))
		}
		result := testListOperation(ctx, clients, gvr, cfg, chunkSize, cfg.PerfTests)
		if result != nil {
			results = append(results, *result)
		}
	}

	// Test GET operations (random CRs)
	if totalCRs > 0 {
		LogPerf(fmt.Sprintf("Testing GET operations on random CRs (%d samples)...", cfg.PerfTests))
		result := testGetOperation(ctx, clients, gvr, cfg, totalCRs, cfg.PerfTests)
		if result != nil {
			results = append(results, *result)
		}
	}

	// Test UPDATE operations (random CRs)
	if totalCRs > 0 {
		LogPerf(fmt.Sprintf("Testing UPDATE operations on random CRs (%d samples)...", cfg.PerfTests))
		result := testUpdateOperation(ctx, clients, gvr, cfg, totalCRs, cfg.PerfTests)
		if result != nil {
			results = append(results, *result)
		}
	}

	// Print comprehensive results
	PrintReadPerformanceResults(results, totalCRs)
	return results
}

func testListOperation(ctx context.Context, clients *Clients, gvr schema.GroupVersionResource,
	cfg *cfgpkg.CRBenchmarkConfig, chunkSize int, iterations int) *ReadPerfMetrics {

	var latencies []time.Duration
	var sizes []int64
	var itemCounts []int
	successful := 0

	for i := 0; i < iterations; i++ {
		start := time.Now()
		totalItems, totalSize, err := performListIteration(ctx, clients, gvr, cfg, chunkSize)
		latency := time.Since(start)

		if err == nil {
			successful++
			latencies = append(latencies, latency)
			sizes = append(sizes, totalSize)
			itemCounts = append(itemCounts, totalItems)
		}
	}

	if len(latencies) == 0 {
		return nil
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	return &ReadPerfMetrics{
		Operation:   "LIST",
		ChunkSize:   chunkSize,
		UsedCBOR:    cfg.UseCBOR,
		Samples:     iterations,
		SuccessRate: float64(successful) / float64(iterations) * 100,
		MinLatency:  latencies[0],
		MaxLatency:  latencies[len(latencies)-1],
		P50Latency:  latencies[len(latencies)*50/100],
		P95Latency:  latencies[len(latencies)*95/100],
		P99Latency:  latencies[len(latencies)*99/100],
		Count:       calculateAverage(itemCounts),
		AvgLatency:  calculateAverageLatency(latencies),
		AvgSize:     calculateAverageSize(sizes),
	}
}

func performListIteration(ctx context.Context, clients *Clients, gvr schema.GroupVersionResource,
	cfg *cfgpkg.CRBenchmarkConfig, chunkSize int) (int, int64, error) {

	totalItems := 0
	totalSize := int64(0)
	continueToken := ""

	for {
		listOpts := metav1.ListOptions{Continue: continueToken}
		if chunkSize > 0 {
			listOpts.Limit = int64(chunkSize)
		}

		var list *unstructured.UnstructuredList
		var err error

		if cfg.CRDName == NamespacedCRDName {
			list, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).List(ctx, listOpts)
		} else {
			list, err = clients.DynamicClient.Resource(gvr).List(ctx, listOpts)
		}

		if err != nil || list == nil {
			return totalItems, totalSize, err
		}

		totalItems += len(list.Items)
		if data, marshalErr := json.Marshal(list); marshalErr == nil {
			totalSize += int64(len(data))
		}

		continueToken = list.GetContinue()
		if continueToken == "" {
			break
		}
	}

	return totalItems, totalSize, nil
}

func calculateAverage(values []int) int {
	if len(values) == 0 {
		return 0
	}
	total := 0
	for _, v := range values {
		total += v
	}
	return total / len(values)
}

func calculateAverageLatency(latencies []time.Duration) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	total := time.Duration(0)
	for _, l := range latencies {
		total += l
	}
	return total / time.Duration(len(latencies))
}

func calculateAverageSize(sizes []int64) int64 {
	if len(sizes) == 0 {
		return 0
	}
	total := int64(0)
	for _, s := range sizes {
		total += s
	}
	return total / int64(len(sizes))
}

// testGetOperation tests GET operations on random CRs
func testGetOperation(ctx context.Context, clients *Clients, gvr schema.GroupVersionResource,
	cfg *cfgpkg.CRBenchmarkConfig, totalCRs int, iterations int) *ReadPerfMetrics {

	// Get list of CR names first using pagination
	var crNames []string
	continueToken := ""
	for {
		listOpts := metav1.ListOptions{
			Limit:    10000,
			Continue: continueToken,
		}

		var list *unstructured.UnstructuredList
		var err error
		if cfg.CRDName == NamespacedCRDName {
			list, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).
				List(ctx, listOpts)
		} else {
			list, err = clients.DynamicClient.Resource(gvr).
				List(ctx, listOpts)
		}

		if err != nil || list == nil {
			break
		}

		for _, item := range list.Items {
			crNames = append(crNames, item.GetName())
		}

		continueToken = list.GetContinue()
		if continueToken == "" {
			break
		}
	}

	if len(crNames) == 0 {
		return nil
	}

	var latencies []time.Duration
	var sizes []int64
	successful := 0

	for i := 0; i < iterations; i++ {
		// Pick random CR
		idx := i % len(crNames)
		name := crNames[idx]

		start := time.Now()
		var result *unstructured.Unstructured
		var err error

		if cfg.CRDName == NamespacedCRDName {
			result, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).
				Get(ctx, name, metav1.GetOptions{})
		} else {
			result, err = clients.DynamicClient.Resource(gvr).
				Get(ctx, name, metav1.GetOptions{})
		}

		latency := time.Since(start)

		if err == nil && result != nil {
			successful++
			latencies = append(latencies, latency)

			// Calculate response size
			if data, marshalErr := json.Marshal(result); marshalErr == nil {
				sizes = append(sizes, int64(len(data)))
			}
		}
	}

	if len(latencies) == 0 {
		return nil
	}

	// Sort for percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate metrics
	metrics := &ReadPerfMetrics{
		Operation:   "GET",
		UsedCBOR:    cfg.UseCBOR,
		Samples:     iterations,
		SuccessRate: float64(successful) / float64(iterations) * 100,
		MinLatency:  latencies[0],
		MaxLatency:  latencies[len(latencies)-1],
		P50Latency:  latencies[len(latencies)*50/100],
		P95Latency:  latencies[len(latencies)*95/100],
		P99Latency:  latencies[len(latencies)*99/100],
	}

	// Calculate average latency
	var totalLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
	}
	metrics.AvgLatency = totalLatency / time.Duration(len(latencies))

	// Calculate average size
	if len(sizes) > 0 {
		var totalSize int64
		for _, s := range sizes {
			totalSize += s
		}
		metrics.AvgSize = totalSize / int64(len(sizes))
	}

	return metrics
}

// testUpdateOperation tests UPDATE operations on random CRs
func testUpdateOperation(ctx context.Context, clients *Clients, gvr schema.GroupVersionResource,
	cfg *cfgpkg.CRBenchmarkConfig, totalCRs int, iterations int) *ReadPerfMetrics {

	// Get list of CR names first using pagination
	var crNames []string
	continueToken := ""
	for {
		listOpts := metav1.ListOptions{
			Limit:    10000,
			Continue: continueToken,
		}

		var list *unstructured.UnstructuredList
		var err error
		if cfg.CRDName == NamespacedCRDName {
			list, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).
				List(ctx, listOpts)
		} else {
			list, err = clients.DynamicClient.Resource(gvr).
				List(ctx, listOpts)
		}

		if err != nil || list == nil {
			break
		}

		for _, item := range list.Items {
			crNames = append(crNames, item.GetName())
		}

		continueToken = list.GetContinue()
		if continueToken == "" {
			break
		}
	}

	if len(crNames) == 0 {
		return nil
	}

	var latencies []time.Duration
	var sizes []int64
	successful := 0

	for i := 0; i < iterations; i++ {
		// Pick random CR
		idx := i % len(crNames)
		name := crNames[idx]

		// Get the CR first
		var cr *unstructured.Unstructured
		var err error

		if cfg.CRDName == NamespacedCRDName {
			cr, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).
				Get(ctx, name, metav1.GetOptions{})
		} else {
			cr, err = clients.DynamicClient.Resource(gvr).
				Get(ctx, name, metav1.GetOptions{})
		}

		if err != nil {
			continue
		}

		// Update the spec with a counter
		spec, found, _ := unstructured.NestedMap(cr.Object, "spec")
		if !found {
			spec = make(map[string]interface{})
		}

		// Increment update counter
		updateCount := int64(0)
		if count, ok := spec["updateCount"].(int64); ok {
			updateCount = count
		} else if count, ok := spec["updateCount"].(float64); ok {
			updateCount = int64(count)
		}
		spec["updateCount"] = updateCount + 1

		if err := unstructured.SetNestedMap(cr.Object, spec, "spec"); err != nil {
			continue
		}

		// Perform the update and measure latency
		start := time.Now()
		var result *unstructured.Unstructured

		if cfg.CRDName == NamespacedCRDName {
			result, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).
				Update(ctx, cr, metav1.UpdateOptions{})
		} else {
			result, err = clients.DynamicClient.Resource(gvr).
				Update(ctx, cr, metav1.UpdateOptions{})
		}

		latency := time.Since(start)

		if err == nil && result != nil {
			successful++
			latencies = append(latencies, latency)

			// Calculate response size
			if data, marshalErr := json.Marshal(result); marshalErr == nil {
				sizes = append(sizes, int64(len(data)))
			}
		}
	}

	if len(latencies) == 0 {
		return nil
	}

	// Sort for percentiles
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	// Calculate metrics
	metrics := &ReadPerfMetrics{
		Operation:   "UPDATE",
		UsedCBOR:    cfg.UseCBOR,
		Samples:     iterations,
		SuccessRate: float64(successful) / float64(iterations) * 100,
		MinLatency:  latencies[0],
		MaxLatency:  latencies[len(latencies)-1],
		P50Latency:  latencies[len(latencies)*50/100],
		P95Latency:  latencies[len(latencies)*95/100],
		P99Latency:  latencies[len(latencies)*99/100],
	}

	// Calculate average latency
	var totalLatency time.Duration
	for _, l := range latencies {
		totalLatency += l
	}
	metrics.AvgLatency = totalLatency / time.Duration(len(latencies))

	// Calculate average size
	if len(sizes) > 0 {
		var totalSize int64
		for _, s := range sizes {
			totalSize += s
		}
		metrics.AvgSize = totalSize / int64(len(sizes))
	}

	return metrics
}
