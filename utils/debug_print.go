package utils

import (
	"fmt"
	"log"
)

var (
	debugMode bool
)

// SetDebuggingMode sets the debugging mode to a certain status.
// If the status is on, the debug print function can be used.
func SetDebuggingMode(status bool) {
	debugMode = status
}

// PrintDebug checks if the debugging mode is turned on and then prints the contents
func PrintDebug(format string, args ...interface{}) {
	if debugMode {
		// the debugging mode is on, print the things given as arguments
		log.Printf(format, args...)
	}
}

// HumanByteCount returns the amount of bytes in a human readable form.
func HumanByteCount(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}
