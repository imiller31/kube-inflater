package crspec

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Generator creates CRs from a template
type Generator struct {
	template  *unstructured.Unstructured
	prefix    string
	namespace string
}

// NewGenerator creates a new CR generator
func NewGenerator(template *unstructured.Unstructured, prefix, namespace string) *Generator {
	return &Generator{
		template:  template,
		prefix:    prefix,
		namespace: namespace,
	}
}

// Generate creates a CR for the given index
// Memory efficient - creates one CR at a time, not pre-generating millions
func (g *Generator) Generate(index int) *unstructured.Unstructured {
	cr := g.template.DeepCopy()

	// Set unique name
	name := fmt.Sprintf("%s-%d", g.prefix, index)
	cr.SetName(name)
	cr.SetNamespace(g.namespace)

	// Add labels for cleanup
	labels := cr.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	labels["app"] = "cr-benchmark"
	labels["batch"] = g.prefix
	cr.SetLabels(labels)

	return cr
}

// LoadTemplate loads a CR template from a YAML file
func LoadTemplate(path string) (*unstructured.Unstructured, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading template file: %w", err)
	}

	var obj map[string]interface{}
	if err := yaml.Unmarshal(data, &obj); err != nil {
		return nil, fmt.Errorf("parsing template YAML: %w", err)
	}

	// Convert map[interface{}]interface{} to map[string]interface{} recursively
	// This is required because yaml.v2 creates interface{} keys, but Kubernetes
	// Unstructured.DeepCopy() requires string keys for JSON compatibility
	converted := convertToJSONCompatible(obj).(map[string]interface{})

	return &unstructured.Unstructured{Object: converted}, nil
}

// convertToJSONCompatible recursively converts YAML types to JSON-compatible types
// This fixes panics from k8s DeepCopy which requires:
// - map[string]interface{} instead of map[interface{}]interface{}
// - float64 for numbers instead of int
// - nil, bool, string, []interface{}, map[string]interface{} only
func convertToJSONCompatible(i interface{}) interface{} {
	switch x := i.(type) {
	case map[interface{}]interface{}:
		m2 := map[string]interface{}{}
		for k, v := range x {
			m2[k.(string)] = convertToJSONCompatible(v)
		}
		return m2
	case map[string]interface{}:
		m2 := map[string]interface{}{}
		for k, v := range x {
			m2[k] = convertToJSONCompatible(v)
		}
		return m2
	case []interface{}:
		for i, v := range x {
			x[i] = convertToJSONCompatible(v)
		}
		return x
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case uint:
		return float64(x)
	case uint64:
		return float64(x)
	case uint32:
		return float64(x)
	}
	return i
}