package main

import (
	"fmt"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	broker := NewBroker()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.Enqueue("bench-queue", "payload")
	}
}

func BenchmarkEnqueueDequeueMixed(b *testing.B) {
	broker := NewBroker()
	for i := 0; i < 100; i++ {
		broker.Enqueue("bench-queue", "payload")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.Enqueue("bench-queue", "payload")
		broker.Dequeue("bench-queue")
	}
}

func BenchmarkEnqueueParallel(b *testing.B) {
	broker := NewBroker()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			broker.Enqueue("bench-queue", "payload")
		}
	})
}

func BenchmarkEnqueueDequeueParallelMultiQueue(b *testing.B) {
	broker := NewBroker()
	numQueues := 20
	for q := 0; q < numQueues; q++ {
		for i := 0; i < 100; i++ {
			broker.Enqueue(fmt.Sprintf("queue-%d", q), "payload")
		}
	}
	b.ResetTimer()
	var counter int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter++
			qName := fmt.Sprintf("queue-%d", counter%numQueues)
			broker.Enqueue(qName, "payload")
			broker.Dequeue(qName)
		}
	})
}
