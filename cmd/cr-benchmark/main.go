package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	cfgpkg "kube-inflater/internal/config"
	"kube-inflater/internal/crbenchmark"
	"kube-inflater/internal/crperf"
)

func main() {
	cfg := crbenchmark.LoadConfigFromFlags()
	logConfiguration(cfg)

	clients, err := crbenchmark.BuildClients(cfg)
	if err != nil {
		crbenchmark.LogErr(fmt.Sprintf("Failed to build clients: %v", err))
		os.Exit(1)
	}

	ctx, cancel := setupSignalHandler()
	defer cancel()

	if cfg.CleanupOnly {
		handleCleanup(ctx, clients, cfg)
		return
	}

	if cfg.PerfOnly {
		handlePerfOnly(ctx, clients, cfg)
		return
	}

	handleFullBenchmark(ctx, clients, cfg)
}

func logConfiguration(cfg *cfgpkg.CRBenchmarkConfig) {
	scope := "namespaced"
	if cfg.CRDName == crbenchmark.ClusterwiseCRDName {
		scope = "clusterwide"
	}
	crbenchmark.LogInfo("🎈 Starting CR benchmark - testing custom resource scalability!")
	crbenchmark.LogInfo(fmt.Sprintf("Configuration: Scope=%s CRD=%s Total=%d Workers=%d",
		scope, cfg.CRDName, cfg.TotalCRs, cfg.Concurrency))
}

func setupSignalHandler() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		crbenchmark.LogInfo("Received shutdown signal")
		cancel()
	}()
	return ctx, cancel
}

func handleCleanup(ctx context.Context, clients *crbenchmark.Clients, cfg *cfgpkg.CRBenchmarkConfig) {
	if err := crbenchmark.CleanupResources(ctx, clients, cfg); err != nil {
		crbenchmark.LogErr(fmt.Sprintf("Cleanup failed: %v", err))
		os.Exit(1)
	}
}

func handlePerfOnly(ctx context.Context, clients *crbenchmark.Clients, cfg *cfgpkg.CRBenchmarkConfig) {
	crbenchmark.LogInfo("🎈 Running in performance-only mode - testing existing CRs")

	clusterCtx := gatherAndLogClusterContext(ctx, clients, cfg)
	if clusterCtx.ExistingCRs == 0 {
		crbenchmark.LogWarn("No existing CRs found! Cannot run performance tests.")
		os.Exit(1)
	}

	readPerfResults := crbenchmark.RunPerformanceTests(ctx, clients, cfg)
	saveReport(cfg, clusterCtx, nil, readPerfResults, "perfonly")
	crbenchmark.LogInfo("🎈 Performance-only benchmark completed successfully!")
}

func handleFullBenchmark(ctx context.Context, clients *crbenchmark.Clients, cfg *cfgpkg.CRBenchmarkConfig) {
	if err := crbenchmark.EnsureCRDExists(ctx, clients, cfg); err != nil {
		crbenchmark.LogErr(fmt.Sprintf("Failed to ensure CRD exists: %v", err))
		os.Exit(1)
	}

	if cfg.CRDName == crbenchmark.NamespacedCRDName {
		if err := crbenchmark.EnsureNamespace(ctx, clients, cfg.Namespace); err != nil {
			crbenchmark.LogErr(fmt.Sprintf("Failed to ensure namespace: %v", err))
			os.Exit(1)
		}
	}

	clusterCtx := gatherAndLogClusterContext(ctx, clients, cfg)
	creationMetrics := createCRsIfNeeded(ctx, clients, cfg, clusterCtx)
	readPerfResults := crbenchmark.RunPerformanceTests(ctx, clients, cfg)

	var creationStats *crperf.Stats
	if creationMetrics != nil {
		stats := creationMetrics.GetStats()
		creationStats = &stats
	}

	saveReport(cfg, clusterCtx, creationStats, readPerfResults, "")
	crbenchmark.LogInfo("🎈 CR benchmark completed successfully!")
}

func gatherAndLogClusterContext(ctx context.Context, clients *crbenchmark.Clients, cfg *cfgpkg.CRBenchmarkConfig) crbenchmark.ClusterContext {
	crbenchmark.LogInfo("Gathering cluster context...")
	clusterCtx := crbenchmark.GatherClusterContext(ctx, clients, cfg)
	crbenchmark.LogInfo(fmt.Sprintf("Cluster state: %d nodes, %d pods, %d existing CRs",
		clusterCtx.NodeCount, clusterCtx.PodCount, clusterCtx.ExistingCRs))
	return clusterCtx
}

func createCRsIfNeeded(ctx context.Context, clients *crbenchmark.Clients, cfg *cfgpkg.CRBenchmarkConfig, clusterCtx crbenchmark.ClusterContext) *crperf.Metrics {
	if clusterCtx.ExistingCRs >= cfg.TotalCRs {
		crbenchmark.LogInfo(fmt.Sprintf("🎈 Cluster already has %d CRs (target: %d) - skipping CR creation",
			clusterCtx.ExistingCRs, cfg.TotalCRs))
		return nil
	}

	crsToCreate := cfg.TotalCRs - clusterCtx.ExistingCRs
	crbenchmark.LogInfo(fmt.Sprintf("Creating %d additional CRs (existing: %d, target: %d)",
		crsToCreate, clusterCtx.ExistingCRs, cfg.TotalCRs))

	createCfg := *cfg
	createCfg.TotalCRs = crsToCreate

	creationMetrics, err := crbenchmark.CreateCRs(ctx, clients, &createCfg, clusterCtx)
	if err != nil {
		crbenchmark.LogErr(fmt.Sprintf("Failed to create CRs: %v", err))
		os.Exit(1)
	}

	crbenchmark.PrintFinalStats(creationMetrics, cfg, clusterCtx)
	return creationMetrics
}

func saveReport(cfg *cfgpkg.CRBenchmarkConfig, clusterCtx crbenchmark.ClusterContext, creationStats *crperf.Stats, readPerfResults []crbenchmark.ReadPerfMetrics, suffix string) {
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	safeCRDName := strings.ReplaceAll(cfg.CRDName, ".", "-")

	filename := fmt.Sprintf("cr-benchmark-%s-%s.md", safeCRDName, timestamp)
	if suffix != "" {
		filename = fmt.Sprintf("cr-benchmark-%s-%s-%s.md", suffix, safeCRDName, timestamp)
	}

	reportPath := filepath.Join(cfg.OutputDir, filename)
	markdownReport := crbenchmark.GenerateMarkdownReport(cfg, clusterCtx, creationStats, nil, readPerfResults)

	if err := os.WriteFile(reportPath, []byte(markdownReport), 0644); err != nil {
		crbenchmark.LogWarn(fmt.Sprintf("Failed to write markdown report: %v", err))
	} else {
		crbenchmark.LogInfo(fmt.Sprintf("📊 Markdown report saved to: %s", reportPath))
	}
}