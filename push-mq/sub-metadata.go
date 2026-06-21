package main

type SubMetadata struct {
	subURL          string // URL to call POST /queue/message on
	numberOfRetries int
	retryPolicy     string // Either fixed delay or exponential, fixed delay by default
	initialDelay    int    // Initial delay in Milliseconds
}
