package crbenchmark

import (
	"context"
	"fmt"
	"time"

	cfgpkg "kube-inflater/internal/config"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ClusterContext holds cluster state information
type ClusterContext struct {
	NodeCount      int
	PodCount       int
	NamespaceCount int
	ExistingCRs    int
	MaxCRIndex     int // Highest CR index found (from bench-N naming)
	TotalCRDs      int
	Timestamp      time.Time
}

// GatherClusterContext collects current cluster state
func GatherClusterContext(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig) ClusterContext {
	context := ClusterContext{
		Timestamp: time.Now(),
	}

	// Get node count
	if nodes, err := clients.Clientset.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err == nil {
		context.NodeCount = len(nodes.Items)
	} else {
		LogErr(fmt.Sprintf("Failed to count nodes: %v", err))
	}

	// Get pod count
	if pods, err := clients.Clientset.CoreV1().Pods("").List(ctx, metav1.ListOptions{}); err == nil {
		context.PodCount = len(pods.Items)
	} else {
		LogErr(fmt.Sprintf("Failed to count pods: %v", err))
	}

	// Get namespace count
	if namespaces, err := clients.Clientset.CoreV1().Namespaces().List(ctx, metav1.ListOptions{}); err == nil {
		context.NamespaceCount = len(namespaces.Items)
	} else {
		LogErr(fmt.Sprintf("Failed to count namespaces: %v", err))
	}

	// Get existing CR count using pagination and find max index
	gvr := GetGVR(cfg.CRDName)
	continueToken := ""
	pageNum := 0
	context.MaxCRIndex = -1 // Initialize to -1 so first CR will be index 0
	
	for {
		pageNum++
		listOpts := metav1.ListOptions{
			Limit:    int64(cfg.PerfChunkSize),
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

		if err != nil {
			LogErr(fmt.Sprintf("Failed to list existing CRs (page %d): %v", pageNum, err))
			break
		}

		if list == nil {
			LogWarn(fmt.Sprintf("Received nil list response (page %d)", pageNum))
			break
		}

		pageItemCount := len(list.Items)
		context.ExistingCRs += pageItemCount
		
		// Find max index from CR names (bench-N pattern)
		for _, item := range list.Items {
			name := item.GetName()
			var index int
			if _, err := fmt.Sscanf(name, "bench-%d", &index); err == nil {
				if index > context.MaxCRIndex {
					context.MaxCRIndex = index
				}
			}
		}

		if pageNum == 1 {
			LogInfo(fmt.Sprintf("Counting existing CRs: found %d in first page", pageItemCount))
		} else if pageItemCount > 0 {
			LogInfo(fmt.Sprintf("Counting existing CRs: page %d has %d items (total so far: %d)",
				pageNum, pageItemCount, context.ExistingCRs))
		}

		continueToken = list.GetContinue()
		if continueToken == "" {
			if pageNum > 1 {
				LogInfo(fmt.Sprintf("Finished counting existing CRs: %d total across %d pages (max index: %d)",
					context.ExistingCRs, pageNum, context.MaxCRIndex))
			} else if context.ExistingCRs > 0 {
				LogInfo(fmt.Sprintf("Found %d existing CRs (max index: %d)", context.ExistingCRs, context.MaxCRIndex))
			}
			break
		}
	}

	// Get total CRD count
	crdGVR := schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}
	if crds, err := clients.DynamicClient.Resource(crdGVR).List(ctx, metav1.ListOptions{}); err == nil {
		context.TotalCRDs = len(crds.Items)
	} else {
		LogErr(fmt.Sprintf("Failed to count CRDs: %v", err))
	}

	return context
}

// EnsureNamespace creates a namespace if it doesn't exist
func EnsureNamespace(ctx context.Context, clients *Clients, namespace string) error {
	_, err := clients.Clientset.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err == nil {
		return nil
	}

	LogInfo(fmt.Sprintf("Creating namespace %s", namespace))
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}
	_, err = clients.Clientset.CoreV1().Namespaces().Create(ctx, ns, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("creating namespace: %w", err)
	}
	return nil
}
