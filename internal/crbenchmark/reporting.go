package crbenchmark

import (
	"fmt"
	"sort"
	"strings"
	"time"

	cfgpkg "kube-inflater/internal/config"
	"kube-inflater/internal/crperf"
)

// PrintFinalStats prints creation performance statistics
func PrintFinalStats(metrics *crperf.Metrics, cfg *cfgpkg.CRBenchmarkConfig, clusterCtx ClusterContext) {
	stats := metrics.GetStats()

	fmt.Println("========================================================================")
	LogInfo("🎈 CR BENCHMARK RESULTS")
	fmt.Println("========================================================================")

	// Cluster Context
	LogInfo("")
	LogInfo("CLUSTER CONTEXT:")
	LogInfo(fmt.Sprintf("  Nodes:            %d", clusterCtx.NodeCount))
	LogInfo(fmt.Sprintf("  Pods:             %d", clusterCtx.PodCount))
	LogInfo(fmt.Sprintf("  Namespaces:       %d", clusterCtx.NamespaceCount))
	LogInfo(fmt.Sprintf("  Existing CRs:     %d", clusterCtx.ExistingCRs))
	LogInfo(fmt.Sprintf("  Total CRDs:       %d", clusterCtx.TotalCRDs))

	// Creation Stats
	LogInfo("")
	LogInfo("CREATION STATISTICS:")
	LogInfo(fmt.Sprintf("  Total CRs created:  %d", stats.Created))
	LogInfo(fmt.Sprintf("  Total failed:       %d", stats.Failed))
	LogInfo(fmt.Sprintf("  Success rate:       %.2f%%", stats.SuccessRate))
	LogInfo(fmt.Sprintf("  Total duration:     %v", stats.Duration))
	LogInfo(fmt.Sprintf("  Overall rate:       %.0f CRs/sec", stats.Rate))

	// Latency Stats
	LogInfo("")
	LogInfo("LATENCY METRICS:")
	LogInfo(fmt.Sprintf("  Average latency:  %v", stats.AvgLatency))
	LogInfo(fmt.Sprintf("  Min latency:      %v", stats.MinLatency))
	LogInfo(fmt.Sprintf("  Max latency:      %v", stats.MaxLatency))
	LogInfo(fmt.Sprintf("  P50 latency:      %v", stats.P50Latency))
	LogInfo(fmt.Sprintf("  P95 latency:      %v", stats.P95Latency))
	LogInfo(fmt.Sprintf("  P99 latency:      %v", stats.P99Latency))

	// Response Size Stats
	LogInfo("")
	LogInfo("RESPONSE SIZE METRICS:")
	LogInfo(fmt.Sprintf("  Average size:     %s", formatBytes(stats.AvgResponseSize)))
	LogInfo(fmt.Sprintf("  Min size:         %s", formatBytes(stats.MinResponseSize)))
	LogInfo(fmt.Sprintf("  Max size:         %s", formatBytes(stats.MaxResponseSize)))
	LogInfo(fmt.Sprintf("  P50 size:         %s", formatBytes(stats.P50ResponseSize)))
	LogInfo(fmt.Sprintf("  P95 size:         %s", formatBytes(stats.P95ResponseSize)))
	LogInfo(fmt.Sprintf("  P99 size:         %s", formatBytes(stats.P99ResponseSize)))
	LogInfo(fmt.Sprintf("  Total bandwidth:  %s", formatBytes(stats.TotalBandwidth)))

	// Status Code Distribution
	if len(stats.StatusCodes) > 0 {
		LogInfo("")
		LogInfo("HTTP STATUS CODES:")
		// Sort status codes for consistent output
		codes := make([]int, 0, len(stats.StatusCodes))
		for code := range stats.StatusCodes {
			codes = append(codes, code)
		}
		sort.Ints(codes)
		for _, code := range codes {
			count := stats.StatusCodes[code]
			pct := float64(count) / float64(stats.Created+stats.Failed) * 100
			LogInfo(fmt.Sprintf("  %d: %d (%.1f%%)", code, count, pct))
		}
	}

	// Error Categories
	if len(stats.ErrorCategories) > 0 {
		LogInfo("")
		LogInfo("ERROR BREAKDOWN:")
		// Sort error categories for consistent output
		categories := make([]string, 0, len(stats.ErrorCategories))
		for cat := range stats.ErrorCategories {
			categories = append(categories, cat)
		}
		sort.Strings(categories)
		for _, cat := range categories {
			count := stats.ErrorCategories[cat]
			sample := stats.ErrorSamples[cat]
			if len(sample) > 60 {
				sample = sample[:57] + "..."
			}
			LogInfo(fmt.Sprintf("  %s: %d", cat, count))
			if sample != "" {
				LogInfo(fmt.Sprintf("    Sample: %s", sample))
			}
		}
	}

	LogInfo("")
	fmt.Println("========================================================================")
}

// PrintReadPerformanceResults prints comprehensive read performance results
func PrintReadPerformanceResults(results []ReadPerfMetrics, totalCRs int) {
	fmt.Println("\n========================================================================")
	LogPerf("📖 READ PERFORMANCE RESULTS")
	fmt.Println("========================================================================")
	
	LogPerf(fmt.Sprintf("Total CRs in cluster: %d", totalCRs))
	LogPerf("")
	
	// Group by operation type
	listResults := []ReadPerfMetrics{}
	getResults := []ReadPerfMetrics{}
	updateResults := []ReadPerfMetrics{}
	
	for _, r := range results {
		if r.Operation == "LIST" {
			listResults = append(listResults, r)
		} else if r.Operation == "GET" {
			getResults = append(getResults, r)
		} else if r.Operation == "UPDATE" {
			updateResults = append(updateResults, r)
		}
	}
	
	// Print LIST results
	if len(listResults) > 0 {
		LogPerf("LIST OPERATIONS BY CHUNK SIZE:")
		LogPerf("")
		LogPerf(fmt.Sprintf("%-12s | %-10s | %-10s | %-10s | %-10s | %-10s | %-12s",
			"Chunk Size", "Avg", "P50", "P95", "P99", "Max", "Avg Size"))
		LogPerf("-------------|------------|------------|------------|------------|------------|-------------")
		
		for _, r := range listResults {
			chunkStr := "ALL"
			if r.ChunkSize > 0 {
				chunkStr = fmt.Sprintf("%d", r.ChunkSize)
			}
			
			LogPerf(fmt.Sprintf("%-12s | %-10v | %-10v | %-10v | %-10v | %-10v | %-12s",
				chunkStr,
				r.AvgLatency.Round(time.Microsecond),
				r.P50Latency.Round(time.Microsecond),
				r.P95Latency.Round(time.Microsecond),
				r.P99Latency.Round(time.Microsecond),
				r.MaxLatency.Round(time.Microsecond),
				formatBytes(r.AvgSize)))
		}
		LogPerf("")
	}
	
	// Print GET results
	if len(getResults) > 0 {
		LogPerf("GET OPERATIONS:")
		LogPerf("")
		for _, r := range getResults {
			LogPerf(fmt.Sprintf("  Samples:      %d", r.Samples))
			LogPerf(fmt.Sprintf("  Success rate: %.1f%%", r.SuccessRate))
			LogPerf(fmt.Sprintf("  Avg latency:  %v", r.AvgLatency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  Min latency:  %v", r.MinLatency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  Max latency:  %v", r.MaxLatency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  P50 latency:  %v", r.P50Latency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  P95 latency:  %v", r.P95Latency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  P99 latency:  %v", r.P99Latency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  Avg size:     %s", formatBytes(r.AvgSize)))
		}
		LogPerf("")
	}
	
	// Print UPDATE results
	if len(updateResults) > 0 {
		LogPerf("UPDATE OPERATIONS:")
		LogPerf("")
		for _, r := range updateResults {
			LogPerf(fmt.Sprintf("  Samples:      %d", r.Samples))
			LogPerf(fmt.Sprintf("  Success rate: %.1f%%", r.SuccessRate))
			LogPerf(fmt.Sprintf("  Avg latency:  %v", r.AvgLatency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  Min latency:  %v", r.MinLatency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  Max latency:  %v", r.MaxLatency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  P50 latency:  %v", r.P50Latency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  P95 latency:  %v", r.P95Latency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  P99 latency:  %v", r.P99Latency.Round(time.Microsecond)))
			LogPerf(fmt.Sprintf("  Avg size:     %s", formatBytes(r.AvgSize)))
		}
		LogPerf("")
	}
	
	// Analysis hints
	LogPerf("ANALYSIS:")
	if len(listResults) > 1 {
		// Compare smallest vs largest LIST
		smallest := listResults[0]
		largest := listResults[len(listResults)-1]
		ratio := float64(largest.AvgLatency) / float64(smallest.AvgLatency)
		
		smallestStr := "ALL"
		if smallest.ChunkSize > 0 {
			smallestStr = fmt.Sprintf("chunk-size=%d", smallest.ChunkSize)
		}
		largestStr := "ALL"
		if largest.ChunkSize > 0 {
			largestStr = fmt.Sprintf("chunk-size=%d", largest.ChunkSize)
		}
		LogPerf(fmt.Sprintf("  LIST(%s) vs LIST(%s): %.1fx latency increase", smallestStr, largestStr, ratio))
		if ratio > 2 {
			LogPerf("  ℹ️  Chunking provides better performance for large result sets")
		}
		
		// Check if P99 is much higher than average
		for _, r := range listResults {
			if r.ChunkSize == 0 || r.ChunkSize >= 1000 {
				p99Ratio := float64(r.P99Latency) / float64(r.AvgLatency)
				if p99Ratio > 3 {
					chunkStr := "ALL"
					if r.ChunkSize > 0 {
						chunkStr = fmt.Sprintf("%d", r.ChunkSize)
					}
					LogPerf(fmt.Sprintf("  ⚠️  LIST(chunk-size=%s): P99 is %.1fx average - tail latency issue", chunkStr, p99Ratio))
				}
			}
		}
	}
	
	// Compare GET vs UPDATE if both available
	if len(getResults) > 0 && len(updateResults) > 0 {
		getRatio := float64(getResults[0].AvgLatency) / float64(updateResults[0].AvgLatency)
		LogPerf(fmt.Sprintf("  GET vs UPDATE: UPDATE is %.1fx slower", 1/getRatio))
		
		if updateResults[0].AvgLatency > getResults[0].AvgLatency*2 {
			LogPerf("  ⚠️  UPDATE significantly slower than GET - may indicate write contention")
		}
	}
	
	fmt.Println("========================================================================")
}

// formatBytes converts bytes to human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// GenerateMarkdownReport creates a comprehensive markdown report
func GenerateMarkdownReport(
	cfg *cfgpkg.CRBenchmarkConfig,
	clusterCtx ClusterContext,
	creationStats *crperf.Stats,
	deletionStats *crperf.Stats,
	readPerfResults []ReadPerfMetrics,
) string {
	var report strings.Builder
	timestamp := time.Now().Format("2006-01-02 15:04:05 UTC")
	
	scope := "Namespaced"
	if cfg.CRDName == ClusterwiseCRDName {
		scope = "Clusterwide"
	}

	// Header
	report.WriteString(fmt.Sprintf("# CR Benchmark Performance Report\n\n"))
	report.WriteString(fmt.Sprintf("**Generated:** %s  \n", timestamp))
	report.WriteString(fmt.Sprintf("**CRD Type:** %s (%s)  \n", cfg.CRDName, scope))
	report.WriteString(fmt.Sprintf("**Total CRs Created:** %d  \n", cfg.TotalCRs))
	report.WriteString(fmt.Sprintf("**Workers:** %d  \n", cfg.Concurrency))
	report.WriteString(fmt.Sprintf("**Rate Limit:** %d CRs/sec  \n\n", cfg.RateLimit))

	// Cluster Context
	report.WriteString("## Cluster Context\n\n")
	report.WriteString(fmt.Sprintf("- **Nodes:** %d\n", clusterCtx.NodeCount))
	report.WriteString(fmt.Sprintf("- **Pods:** %d\n", clusterCtx.PodCount))
	report.WriteString(fmt.Sprintf("- **Namespaces:** %d\n", clusterCtx.NamespaceCount))
	report.WriteString(fmt.Sprintf("- **Existing CRs (before test):** %d\n", clusterCtx.ExistingCRs))
	report.WriteString(fmt.Sprintf("- **Total CRDs:** %d\n", clusterCtx.TotalCRDs))
	report.WriteString(fmt.Sprintf("- **Test Start Time:** %s\n\n", clusterCtx.Timestamp.Format("2006-01-02 15:04:05 UTC")))

	// Creation Performance
	if creationStats != nil {
		report.WriteString("## Creation Performance\n\n")
		report.WriteString("### Statistics\n\n")
		report.WriteString(fmt.Sprintf("- **Total CRs Created:** %d\n", creationStats.Created))
		report.WriteString(fmt.Sprintf("- **Total Failed:** %d\n", creationStats.Failed))
		report.WriteString(fmt.Sprintf("- **Success Rate:** %.2f%%\n", creationStats.SuccessRate))
		report.WriteString(fmt.Sprintf("- **Total Duration:** %v\n", creationStats.Duration))
		report.WriteString(fmt.Sprintf("- **Overall Rate:** %.0f CRs/sec\n\n", creationStats.Rate))

		report.WriteString("### Latency Metrics\n\n")
		report.WriteString("| Metric | Value |\n")
		report.WriteString("|--------|-------|\n")
		report.WriteString(fmt.Sprintf("| Average | %v |\n", creationStats.AvgLatency))
		report.WriteString(fmt.Sprintf("| Min | %v |\n", creationStats.MinLatency))
		report.WriteString(fmt.Sprintf("| Max | %v |\n", creationStats.MaxLatency))
		report.WriteString(fmt.Sprintf("| P50 | %v |\n", creationStats.P50Latency))
		report.WriteString(fmt.Sprintf("| P95 | %v |\n", creationStats.P95Latency))
		report.WriteString(fmt.Sprintf("| P99 | %v |\n\n", creationStats.P99Latency))

		report.WriteString("### Response Size Metrics\n\n")
		report.WriteString("| Metric | Value |\n")
		report.WriteString("|--------|-------|\n")
		report.WriteString(fmt.Sprintf("| Average | %s |\n", formatBytes(creationStats.AvgResponseSize)))
		report.WriteString(fmt.Sprintf("| Min | %s |\n", formatBytes(creationStats.MinResponseSize)))
		report.WriteString(fmt.Sprintf("| Max | %s |\n", formatBytes(creationStats.MaxResponseSize)))
		report.WriteString(fmt.Sprintf("| P50 | %s |\n", formatBytes(creationStats.P50ResponseSize)))
		report.WriteString(fmt.Sprintf("| P95 | %s |\n", formatBytes(creationStats.P95ResponseSize)))
		report.WriteString(fmt.Sprintf("| P99 | %s |\n", formatBytes(creationStats.P99ResponseSize)))
		report.WriteString(fmt.Sprintf("| Total Bandwidth | %s |\n\n", formatBytes(creationStats.TotalBandwidth)))

		// HTTP Status Codes
		if len(creationStats.StatusCodes) > 0 {
			report.WriteString("### HTTP Status Codes\n\n")
			report.WriteString("| Status Code | Count | Percentage |\n")
			report.WriteString("|-------------|-------|------------|\n")
			
			codes := make([]int, 0, len(creationStats.StatusCodes))
			for code := range creationStats.StatusCodes {
				codes = append(codes, code)
			}
			sort.Ints(codes)
			
			for _, code := range codes {
				count := creationStats.StatusCodes[code]
				pct := float64(count) / float64(creationStats.Created+creationStats.Failed) * 100
				report.WriteString(fmt.Sprintf("| %d | %d | %.1f%% |\n", code, count, pct))
			}
			report.WriteString("\n")
		}

		// Error Categories
		if len(creationStats.ErrorCategories) > 0 {
			report.WriteString("### Error Breakdown\n\n")
			report.WriteString("| Category | Count | Sample Error |\n")
			report.WriteString("|----------|-------|-------------|\n")
			
			categories := make([]string, 0, len(creationStats.ErrorCategories))
			for cat := range creationStats.ErrorCategories {
				categories = append(categories, cat)
			}
			sort.Strings(categories)
			
			for _, cat := range categories {
				count := creationStats.ErrorCategories[cat]
				sample := creationStats.ErrorSamples[cat]
				if len(sample) > 60 {
					sample = sample[:57] + "..."
				}
				sample = strings.ReplaceAll(sample, "|", "\\|")
				report.WriteString(fmt.Sprintf("| %s | %d | %s |\n", cat, count, sample))
			}
			report.WriteString("\n")
		}
	}

	// Deletion Performance
	if deletionStats != nil && deletionStats.Created > 0 {
		report.WriteString("## Deletion Performance\n\n")
		report.WriteString("### Statistics\n\n")
		report.WriteString(fmt.Sprintf("- **Total CRs Deleted:** %d\n", deletionStats.Created))
		report.WriteString(fmt.Sprintf("- **Total Failed:** %d\n", deletionStats.Failed))
		report.WriteString(fmt.Sprintf("- **Success Rate:** %.2f%%\n", deletionStats.SuccessRate))
		report.WriteString(fmt.Sprintf("- **Total Duration:** %v\n", deletionStats.Duration))
		report.WriteString(fmt.Sprintf("- **Overall Rate:** %.0f CRs/sec\n\n", deletionStats.Rate))

		report.WriteString("### Latency Metrics\n\n")
		report.WriteString("| Metric | Value |\n")
		report.WriteString("|--------|-------|\n")
		report.WriteString(fmt.Sprintf("| Average | %v |\n", deletionStats.AvgLatency))
		report.WriteString(fmt.Sprintf("| Min | %v |\n", deletionStats.MinLatency))
		report.WriteString(fmt.Sprintf("| Max | %v |\n", deletionStats.MaxLatency))
		report.WriteString(fmt.Sprintf("| P50 | %v |\n", deletionStats.P50Latency))
		report.WriteString(fmt.Sprintf("| P95 | %v |\n", deletionStats.P95Latency))
		report.WriteString(fmt.Sprintf("| P99 | %v |\n\n", deletionStats.P99Latency))
	}

	// Read Performance Tests
	if len(readPerfResults) > 0 {
		report.WriteString("## Read/Write Performance Tests\n\n")
		
		// Group by operation type
		listResults := []ReadPerfMetrics{}
		getResults := []ReadPerfMetrics{}
		updateResults := []ReadPerfMetrics{}
		
		for _, r := range readPerfResults {
			switch r.Operation {
			case "LIST":
				listResults = append(listResults, r)
			case "GET":
				getResults = append(getResults, r)
			case "UPDATE":
				updateResults = append(updateResults, r)
			}
		}

		// LIST Operations
		if len(listResults) > 0 {
			report.WriteString("### LIST Operations by Scale\n\n")
			report.WriteString("| Limit | Avg Latency | P50 | P95 | P99 | Max | Avg Size |\n")
			report.WriteString("|-------|-------------|-----|-----|-----|-----|----------|\n")
			
			for _, r := range listResults {
				chunkStr := "ALL"
				if r.ChunkSize > 0 {
					chunkStr = fmt.Sprintf("%d", r.ChunkSize)
				}
				
				report.WriteString(fmt.Sprintf("| %s | %v | %v | %v | %v | %v | %s |\n",
					chunkStr,
					r.AvgLatency.Round(time.Microsecond),
					r.P50Latency.Round(time.Microsecond),
					r.P95Latency.Round(time.Microsecond),
					r.P99Latency.Round(time.Microsecond),
					r.MaxLatency.Round(time.Microsecond),
					formatBytes(r.AvgSize)))
			}
			report.WriteString("\n")
		}

		// GET Operations
		if len(getResults) > 0 {
			report.WriteString("### GET Operations\n\n")
			for _, r := range getResults {
				report.WriteString(fmt.Sprintf("- **Samples:** %d\n", r.Samples))
				report.WriteString(fmt.Sprintf("- **Success Rate:** %.1f%%\n", r.SuccessRate))
				report.WriteString(fmt.Sprintf("- **Average Latency:** %v\n", r.AvgLatency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **P50 Latency:** %v\n", r.P50Latency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **P95 Latency:** %v\n", r.P95Latency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **P99 Latency:** %v\n", r.P99Latency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **Average Size:** %s\n\n", formatBytes(r.AvgSize)))
			}
		}

		// UPDATE Operations
		if len(updateResults) > 0 {
			report.WriteString("### UPDATE Operations\n\n")
			for _, r := range updateResults {
				report.WriteString(fmt.Sprintf("- **Samples:** %d\n", r.Samples))
				report.WriteString(fmt.Sprintf("- **Success Rate:** %.1f%%\n", r.SuccessRate))
				report.WriteString(fmt.Sprintf("- **Average Latency:** %v\n", r.AvgLatency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **P50 Latency:** %v\n", r.P50Latency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **P95 Latency:** %v\n", r.P95Latency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **P99 Latency:** %v\n", r.P99Latency.Round(time.Microsecond)))
				report.WriteString(fmt.Sprintf("- **Average Size:** %s\n\n", formatBytes(r.AvgSize)))
			}
		}

		// Performance Analysis
		report.WriteString("### Performance Analysis\n\n")
		if len(listResults) > 1 {
			smallest := listResults[0]
			largest := listResults[len(listResults)-1]
			ratio := float64(largest.AvgLatency) / float64(smallest.AvgLatency)
			
			report.WriteString(fmt.Sprintf("- **LIST Scaling:** LIST(1) vs LIST(ALL) shows %.1fx latency increase\n", ratio))
			if ratio > 10 {
				report.WriteString("  - ⚠️  High latency increase - large result sets impact performance significantly\n")
			}
			
			// Check tail latency
			for _, r := range listResults {
				if r.ChunkSize == 0 || r.ChunkSize >= 1000 {
					p99Ratio := float64(r.P99Latency) / float64(r.AvgLatency)
					if p99Ratio > 3 {
						chunkStr := "ALL"
						if r.ChunkSize > 0 {
							chunkStr = fmt.Sprintf("chunk-size=%d", r.ChunkSize)
						}
						report.WriteString(fmt.Sprintf("  - ⚠️  LIST(%s): P99 is %.1fx average - tail latency issue detected\n", chunkStr, p99Ratio))
					}
				}
			}
		}
		
		// GET vs UPDATE comparison
		if len(getResults) > 0 && len(updateResults) > 0 {
			getRatio := float64(getResults[0].AvgLatency) / float64(updateResults[0].AvgLatency)
			report.WriteString(fmt.Sprintf("- **GET vs UPDATE:** UPDATE is %.1fx slower than GET\n", 1/getRatio))
			
			if updateResults[0].AvgLatency > getResults[0].AvgLatency*2 {
				report.WriteString("  - ⚠️  UPDATE significantly slower than GET - may indicate write contention\n")
			}
		}
		report.WriteString("\n")
	}

	// Summary and Recommendations
	report.WriteString("## Summary\n\n")
	if creationStats != nil {
		if creationStats.SuccessRate >= 99.9 {
			report.WriteString("✅ **Creation Performance:** Excellent - high success rate with stable latency\n\n")
		} else if creationStats.SuccessRate >= 95 {
			report.WriteString("⚠️  **Creation Performance:** Good - some errors detected, review error breakdown\n\n")
		} else {
			report.WriteString("❌ **Creation Performance:** Poor - high error rate, investigate API server capacity\n\n")
		}
	}

	report.WriteString("---\n")
	report.WriteString("*Generated by kube-inflater CR Benchmark Tool*\n")

	return report.String()
}