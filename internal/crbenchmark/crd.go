package crbenchmark

import (
	"context"
	"fmt"
	"time"

	cfgpkg "kube-inflater/internal/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

func GetCRDDefinition(crdName string) *unstructured.Unstructured {
	kind, plural, singular, scope := getCRDParams(crdName)

	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apiextensions.k8s.io/v1",
			"kind":       "CustomResourceDefinition",
			"metadata": map[string]interface{}{
				"name": crdName,
			},
			"spec": map[string]interface{}{
				"group": "inflator.com",
				"names": map[string]interface{}{
					"kind":     kind,
					"plural":   plural,
					"singular": singular,
				},
				"scope":    scope,
				"versions": []interface{}{getCRDVersion()},
			},
		},
	}
}

func getCRDParams(crdName string) (kind, plural, singular, scope string) {
	if crdName == NamespacedCRDName {
		return NamespacedKind, NamespacedPlural, NamespacedSingular, "Namespaced"
	}
	return ClusterwiseKind, ClusterwisePlural, ClusterwiseSingular, "Cluster"
}

func getCRDVersion() map[string]interface{} {
	return map[string]interface{}{
		"name":    APIVersion,
		"served":  true,
		"storage": true,
		"schema": map[string]interface{}{
			"openAPIV3Schema": map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"spec": map[string]interface{}{
						"type":                                 "object",
						"x-kubernetes-preserve-unknown-fields": true,
					},
					"status": map[string]interface{}{
						"type":                                 "object",
						"x-kubernetes-preserve-unknown-fields": true,
					},
				},
			},
		},
	}
}

func EnsureCRDExists(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig) error {
	crdGVR := getCRDGVR()
	_, err := clients.DynamicClient.Resource(crdGVR).Get(ctx, cfg.CRDName, metav1.GetOptions{})
	crdExists := err == nil

	if crdExists && !cfg.RecreateCRD {
		LogInfo(fmt.Sprintf("CRD %s already exists, using existing CRD", cfg.CRDName))
		return waitForCRDReady(ctx, clients, cfg)
	}

	if crdExists {
		if err := deleteCRD(ctx, clients.DynamicClient, crdGVR, cfg.CRDName); err != nil {
			return err
		}
	}

	if err := createCRD(ctx, clients.DynamicClient, crdGVR, cfg.CRDName); err != nil {
		return err
	}

	return waitForCRDReady(ctx, clients, cfg)
}

func getCRDGVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{
		Group:    "apiextensions.k8s.io",
		Version:  "v1",
		Resource: "customresourcedefinitions",
	}
}

func deleteCRD(ctx context.Context, client dynamic.Interface, crdGVR schema.GroupVersionResource, crdName string) error {
	LogInfo(fmt.Sprintf("Deleting existing CRD %s (--recreate-crd flag set)...", crdName))
	if err := client.Resource(crdGVR).Delete(ctx, crdName, metav1.DeleteOptions{}); err != nil {
		return fmt.Errorf("deleting CRD: %w", err)
	}
	LogInfo("Waiting for CRD deletion to complete...")
	time.Sleep(2 * time.Second)
	return nil
}

func createCRD(ctx context.Context, client dynamic.Interface, crdGVR schema.GroupVersionResource, crdName string) error {
	LogInfo(fmt.Sprintf("Creating CRD %s...", crdName))
	crd := GetCRDDefinition(crdName)
	if _, err := client.Resource(crdGVR).Create(ctx, crd, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating CRD: %w", err)
	}
	LogInfo(fmt.Sprintf("Successfully created CRD %s", crdName))
	return nil
}

func waitForCRDReady(ctx context.Context, clients *Clients, cfg *cfgpkg.CRBenchmarkConfig) error {
	LogInfo("Waiting for CRD to be established...")
	gvr := GetGVR(cfg.CRDName)
	maxWait := 30 * time.Second
	checkInterval := 500 * time.Millisecond
	deadline := time.Now().Add(maxWait)

	for time.Now().Before(deadline) {
		if isCRDReady(ctx, clients, gvr, cfg) {
			LogInfo("CRD is ready!")
			return nil
		}
		time.Sleep(checkInterval)
	}

	return fmt.Errorf("timeout waiting for CRD to be established after %v", maxWait)
}

func isCRDReady(ctx context.Context, clients *Clients, gvr schema.GroupVersionResource, cfg *cfgpkg.CRBenchmarkConfig) bool {
	var err error
	if cfg.CRDName == NamespacedCRDName {
		_, err = clients.DynamicClient.Resource(gvr).Namespace(cfg.Namespace).List(ctx, metav1.ListOptions{Limit: 1})
	} else {
		_, err = clients.DynamicClient.Resource(gvr).List(ctx, metav1.ListOptions{Limit: 1})
	}
	return err == nil
}

func GetGVR(crdName string) schema.GroupVersionResource {
	resource := ClusterwisePlural
	if crdName == NamespacedCRDName {
		resource = NamespacedPlural
	}

	return schema.GroupVersionResource{
		Group:    "inflator.com",
		Version:  APIVersion,
		Resource: resource,
	}
}