package crbenchmark

import "fmt"

// LogInfo logs informational messages
func LogInfo(msg string) {
	fmt.Printf("[INFO] %s\n", msg)
}

// LogWarn logs warning messages
func LogWarn(msg string) {
	fmt.Printf("[WARN] %s\n", msg)
}

// LogErr logs error messages
func LogErr(msg string) {
	fmt.Printf("[ERROR] %s\n", msg)
}

// LogPerf logs performance metrics
func LogPerf(msg string) {
	fmt.Printf("🎈 [GAUGE] %s\n", msg)
}