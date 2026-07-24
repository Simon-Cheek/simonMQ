package main

type SubPolicy struct {
	subName         string
	subURL          string // URL to call POST /queue/message on
	numberOfRetries int
}
