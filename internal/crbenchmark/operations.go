package crbenchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	cfgpkg "kube-inflater/internal/config"
	"kube-inflater/internal/crperf"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func GenerateCR(cfg *cfgpkg.CRBenchmarkConfig, index int) *unstructured.Unstructured {
	kind := ClusterwiseKind
	if cfg.CRDName == NamespacedCRDName {
		kind = NamespacedKind
	}

	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "inflator.com/v1",
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name": fmt.Sprintf("bench-%d", index),
				"labels": map[string]interface{}{
					"app":   "cr-benchmark",
					"batch": "bench",
				},
			},
			"spec": map[string]interface{}{
				"replicas": float64(3),
				"config": map[string]interface{}{
					"enabled": true,
					"value":   "benchmark-value",
				},
			},
		},
	}

	if cfg.CRDName == NamespacedCRDName {
		cr.SetNamespace(cfg.Namespace)
	}

	return cr
}

func CreateCRs(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig, clusterCtx ClusterContext) (*crperf.Metrics, error) {
	gvr := GetGVR(cfg.CRDName)
	metrics := crperf.NewMetrics()

	jobs := make(chan int, cfg.Concurrency*10)
	var wg sync.WaitGroup

	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go createWorker(ctx, &wg, clients.DynamicClient, gvr, jobs, metrics, cfg)
	}

	done := make(chan bool)
	go createProgressReporter(ctx, metrics, cfg.TotalCRs, done)

	startIndex := clusterCtx.MaxCRIndex + 1
	logCreationStart(cfg, startIndex)

	for i := 0; i < cfg.TotalCRs; i++ {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			done <- true
			return metrics, ctx.Err()
		case jobs <- startIndex + i:
		}
	}

	close(jobs)
	wg.Wait()
	done <- true

	return metrics, nil
}

func logCreationStart(cfg *cfgpkg.CRBenchmarkConfig, startIndex int) {
	scope := "namespaced"
	if cfg.CRDName == ClusterwiseCRDName {
		scope = "clusterwide"
	}
	LogInfo(fmt.Sprintf("Creating %d %s CRs with %d workers at %d/sec (starting at index %d)",
		cfg.TotalCRs, scope, cfg.Concurrency, cfg.RateLimit, startIndex))
}

func createWorker(
	ctx context.Context,
	wg *sync.WaitGroup,
	client dynamic.Interface,
	gvr schema.GroupVersionResource,
	jobs <-chan int,
	metrics *crperf.Metrics,
	cfg *cfgpkg.CRBenchmarkConfig,
) {
	defer wg.Done()

	resourceClient := getResourceClient(client, gvr, cfg)

	for idx := range jobs {
		select {
		case <-ctx.Done():
			return
		default:
		}

		cr := GenerateCR(cfg, idx)
		start := time.Now()
		result, err := resourceClient.Create(ctx, cr, metav1.CreateOptions{})
		latency := time.Since(start)

		responseSize, statusCode := calculateResponseMetrics(result, err)
		metrics.Record(latency, responseSize, statusCode, err)

		if err != nil && metrics.GetFailed()%100 == 0 {
			LogWarn(fmt.Sprintf("Error creating CR %d: %v", idx, err))
		}
	}
}

func getResourceClient(client dynamic.Interface, gvr schema.GroupVersionResource, cfg *cfgpkg.CRBenchmarkConfig) dynamic.ResourceInterface {
	if cfg.CRDName == NamespacedCRDName {
		return client.Resource(gvr).Namespace(cfg.Namespace)
	}
	return client.Resource(gvr)
}

func calculateResponseMetrics(result *unstructured.Unstructured, err error) (int64, int) {
	responseSize := int64(0)
	statusCode := 201

	if err == nil && result != nil {
		if data, marshalErr := json.Marshal(result); marshalErr == nil {
			responseSize = int64(len(data))
		}
	} else if err != nil {
		statusCode = extractStatusCode(err)
	}

	return responseSize, statusCode
}

func createProgressReporter(ctx context.Context, metrics *crperf.Metrics, total int, done <-chan bool) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	lastCreated := int64(0)
	lastTime := time.Now()

	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			created := metrics.GetCreated()
			failed := metrics.GetFailed()

			elapsed := time.Since(lastTime).Seconds()
			rate := float64(created-lastCreated) / elapsed

			pct := float64(created+failed) / float64(total) * 100
			LogInfo(fmt.Sprintf("[PROGRESS] %.1f%% | Created: %d | Failed: %d | Rate: %.0f/s",
				pct, created, failed, rate))

			lastCreated = created
			lastTime = time.Now()
		}
	}
}

func extractStatusCode(err error) int {
	if err == nil {
		return 200
	}

	errStr := err.Error()
	statusCodes := []int{429, 409, 400, 403, 404, 500, 503}

	for _, code := range statusCodes {
		if contains(errStr, fmt.Sprintf("%d", code)) {
			return code
		}
	}

	return 500
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr)
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if matchesAt(s, substr, i) {
			return true
		}
	}
	return false
}

func matchesAt(s, substr string, start int) bool {
	for j := 0; j < len(substr); j++ {
		if s[start+j] != substr[j] {
			return false
		}
	}
	return true
}