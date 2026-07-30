package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
)

func main() {
	publishedPath := flag.String("published", "", "path to the publisher's accepted-message file (required)")
	receivedPath := flag.String("received", "", "path to the worker/subscriber's received-message file (required)")
	flag.Parse()

	if *publishedPath == "" || *receivedPath == "" {
		fmt.Fprintln(os.Stderr, "both -published and -received are required")
		os.Exit(1)
	}

	published, err := readLines(*publishedPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read -published file: %v\n", err)
		os.Exit(1)
	}

	received, err := readLines(*receivedPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to read -received file: %v\n", err)
		os.Exit(1)
	}

	receivedSet := make(map[string]struct{}, len(received))
	for _, line := range received {
		receivedSet[line] = struct{}{}
	}

	var missing []string
	for _, msg := range published {
		if _, ok := receivedSet[msg]; !ok {
			missing = append(missing, msg)
		}
	}

	fmt.Printf("published: %d, received (unique): %d, missing: %d\n", len(published), len(receivedSet), len(missing))

	if len(missing) > 0 {
		fmt.Println("missing messages:")
		for _, m := range missing {
			fmt.Println(" -", m)
		}
		os.Exit(1)
	}

	fmt.Println("PASS: every published message was represented at least once")
}

// readLines reads a file into a slice of trimmed, non-empty lines.
func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, scanner.Err()
}
