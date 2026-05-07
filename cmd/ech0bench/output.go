package main

import (
	"fmt"
	"os"
)

func exitWithError(err error) {
	writeStderr("ech0bench: %v\n", err)
	os.Exit(1)
}

func writeStdout(format string, args ...any) {
	if _, err := fmt.Fprintf(os.Stdout, format, args...); err != nil {
		writeStderr("write stdout: %v\n", err)
	}
}

func writeStderr(format string, args ...any) {
	if _, err := fmt.Fprintf(os.Stderr, format, args...); err != nil {
		return
	}
}
