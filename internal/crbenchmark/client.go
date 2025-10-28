package crbenchmark

import (
	"fmt"

	cfgpkg "kube-inflater/internal/config"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/flowcontrol"
)

// Clients holds Kubernetes client interfaces
type Clients struct {
	Clientset     *kubernetes.Clientset
	DynamicClient dynamic.Interface
}

// BuildClients creates Kubernetes clients with appropriate rate limiting
func BuildClients(cfg *cfgpkg.CRBenchmarkConfig) (*Clients, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	overrides := &clientcmd.ConfigOverrides{}
	config := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides)

	restConfig, err := config.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig: %w", err)
	}

	restConfig.QPS = 1000
	restConfig.Burst = 2000
	restConfig.RateLimiter = flowcontrol.NewTokenBucketRateLimiter(
		float32(cfg.RateLimit), cfg.RateLimit*2)

	// Set CBOR content type if requested
	// See: https://kubernetes.io/docs/reference/using-api/api-concepts/#cbor-encoding
	if cfg.UseCBOR {
		// Request CBOR for responses
		restConfig.AcceptContentTypes = "application/cbor"
		// Send requests in CBOR (optional, can still use JSON)
		restConfig.ContentType = "application/cbor"

		restConfig.ContentConfig.AcceptContentTypes = "application/cbor"
		restConfig.ContentConfig.ContentType = "application/cbor"
	}

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("creating clientset: %w", err)
	}

	dynamicClient, err := dynamic.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("creating dynamic client: %w", err)
	}

	// log the content types each client is using
	LogInfo(fmt.Sprintf("Kubernetes Clientset Content-Type: %s, AcceptContentTypes: %s",
		restConfig.ContentType, restConfig.AcceptContentTypes))
	LogInfo(fmt.Sprintf("Kubernetes Dynamic Client Content-Type: %s, AcceptContentTypes: %s",
		restConfig.ContentConfig.ContentType, restConfig.ContentConfig.AcceptContentTypes))

	return &Clients{
		Clientset:     clientset,
		DynamicClient: dynamicClient,
	}, nil
}
