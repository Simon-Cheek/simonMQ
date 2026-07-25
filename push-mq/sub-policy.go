package main

type SubPolicy struct {
	SubName         string
	SubURL          string // URL to call POST /queue/message on
	NumberOfRetries int
}
