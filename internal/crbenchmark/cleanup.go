package crbenchmark

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	cfgpkg "kube-inflater/internal/config"
	"kube-inflater/internal/crperf"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func CleanupResources(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig) error {
	LogInfo("Starting cleanup of CR benchmark resources...")

	gvr := GetGVR(cfg.CRDName)
	crList := listCRsForDeletion(ctx, clients, gvr, cfg)

	if len(crList) == 0 {
		LogInfo("No CRs found to delete")
	} else {
		LogInfo(fmt.Sprintf("Found %d CRs to delete, starting deletion with metrics tracking...", len(crList)))
		deleteMetrics := deleteCRs(ctx, clients, cfg, gvr, crList)
		PrintDeletionStats(deleteMetrics, cfg)
	}

	deleteNamespaceIfNeeded(ctx, clients, cfg)

	LogInfo("Cleanup completed")
	return nil
}

func listCRsForDeletion(ctx context.Context, clients *Clients, gvr schema.GroupVersionResource, cfg *cfgpkg.CRBenchmarkConfig) []string {
	listOpts := metav1.ListOptions{LabelSelector: "app=cr-benchmark"}
	
	var list *unstructured.UnstructuredList
	var err error
	
	if cfg.CRDName == NamespacedCRDName {
		list, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).List(ctx, listOpts)
	} else {
		list, err = clients.DynamicClient.Resource(gvr).List(ctx, listOpts)
	}

	if err != nil {
		scope := "namespaced"
		if cfg.CRDName == ClusterwiseCRDName {
			scope = "clusterwide"
		}
		LogWarn(fmt.Sprintf("Failed to list %s CRs: %v", scope, err))
		return nil
	}

	var crList []string
	for _, item := range list.Items {
		crList = append(crList, item.GetName())
	}
	return crList
}

func deleteNamespaceIfNeeded(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig) {
	if cfg.CRDName == NamespacedCRDName && cfg.Namespace == cfgpkg.DefaultCRNamespace {
		LogInfo(fmt.Sprintf("Deleting namespace %s", cfg.Namespace))
		if err := clients.Clientset.CoreV1().Namespaces().Delete(ctx, cfg.Namespace, metav1.DeleteOptions{}); err != nil {
			LogWarn(fmt.Sprintf("Failed to delete namespace: %v", err))
		}
	}
}

func deleteCRs(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig, gvr schema.GroupVersionResource, crNames []string) *crperf.Metrics {
	metrics := crperf.NewMetrics()
	jobs := make(chan string, cfg.Concurrency*10)
	var wg sync.WaitGroup

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go deleteWorker(ctx, &wg, clients.DynamicClient, gvr, jobs, metrics, cfg)
	}

	done := make(chan bool)
	go deletionProgressReporter(ctx, metrics, len(crNames), done)

	for _, name := range crNames {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			done <- true
			return metrics
		case jobs <- name:
		}
	}

	close(jobs)
	wg.Wait()
	done <- true

	return metrics
}

func deleteWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	jobs <-chan string,
	metrics *crperf.Metrics,
	cfg *cfgpkg.CRBenchmarkConfig,
) {
	defer wg.Done()

	resourceClient := getResourceClient(client, gvr, cfg)

	for name := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		err := resourceClient.Delete(ctx, name, metav1.DeleteOptions{})
		latency := time.Since(start)

		responseSize := int64(100)
		statusCode := 200
		if err != nil {
			statusCode = extractStatusCode(err)
		}

		metrics.Record(latency, responseSize, statusCode, err)

		if err != nil && metrics.GetFailed()%100 == 0 {
			LogWarn(fmt.Sprintf("Error deleting CR %s: %v", name, err))
		}
	}
}

func deletionProgressReporter(ctx context.Context, metrics *crperf.Metrics, total int, done <-chan bool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastDeleted := int64(0)
	lastTime := time.Now()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted := metrics.GetCreated()
			failed := metrics.GetFailed()

			elapsed := time.Since(lastTime).Seconds()
			rate := float64(deleted-lastDeleted) / elapsed

			pct := float64(deleted+failed) / float64(total) * 100
			LogInfo(fmt.Sprintf("[DELETE PROGRESS] %.1f%% | Deleted: %d | Failed: %d | Rate: %.0f/s",
				pct, deleted, failed, rate))

			lastDeleted = deleted
			lastTime = time.Now()
		}
	}
}

func PrintDeletionStats(metrics *crperf.Metrics, cfg *cfgpkg.CRBenchmarkConfig) {
	stats := metrics.GetStats()

	fmt.Println("\n========================================================================")
	LogInfo("🗑️  CR DELETION RESULTS")
	fmt.Println("========================================================================")

	printDeletionBasicStats(stats)
	printDeletionLatencyStats(stats)
	printStatusCodes(stats)
	printErrorBreakdown(stats)

	LogInfo("")
	fmt.Println("========================================================================")
}

func printDeletionBasicStats(stats crperf.Stats) {
	LogInfo("")
	LogInfo("DELETION STATISTICS:")
	LogInfo(fmt.Sprintf("  Total CRs deleted:  %d", stats.Created))
	LogInfo(fmt.Sprintf("  Total failed:       %d", stats.Failed))
	LogInfo(fmt.Sprintf("  Success rate:       %.2f%%", stats.SuccessRate))
	LogInfo(fmt.Sprintf("  Total duration:     %v", stats.Duration))
	LogInfo(fmt.Sprintf("  Overall rate:       %.0f CRs/sec", stats.Rate))
}

func printDeletionLatencyStats(stats crperf.Stats) {
	LogInfo("")
	LogInfo("DELETION LATENCY METRICS:")
	LogInfo(fmt.Sprintf("  Average latency:  %v", stats.AvgLatency))
	LogInfo(fmt.Sprintf("  Min latency:      %v", stats.MinLatency))
	LogInfo(fmt.Sprintf("  Max latency:      %v", stats.MaxLatency))
	LogInfo(fmt.Sprintf("  P50 latency:      %v", stats.P50Latency))
	LogInfo(fmt.Sprintf("  P95 latency:      %v", stats.P95Latency))
	LogInfo(fmt.Sprintf("  P99 latency:      %v", stats.P99Latency))
}

func printStatusCodes(stats crperf.Stats) {
	if len(stats.StatusCodes) == 0 {
		return
	}

	LogInfo("")
	LogInfo("HTTP STATUS CODES:")
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

func printErrorBreakdown(stats crperf.Stats) {
	if len(stats.ErrorCategories) == 0 {
		return
	}

	LogInfo("")
	LogInfo("ERROR BREAKDOWN:")
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