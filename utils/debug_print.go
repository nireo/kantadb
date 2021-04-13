package utils

import "log"

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
