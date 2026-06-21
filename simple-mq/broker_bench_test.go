package main

import (
	"fmt"
	"testing"
)

func BenchmarkEnqueue(b *testing.B) {
	broker := NewBroker()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.enqueue("bench-queue", "payload")
	}
}

func BenchmarkEnqueueDequeueMixed(b *testing.B) {
	broker := NewBroker()
	// Pre-seed so dequeues always have something to pop, keeping depth steady.
	for i := 0; i < 100; i++ {
		broker.enqueue("bench-queue", "payload")
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		broker.enqueue("bench-queue", "payload")
		broker.dequeue("bench-queue")
	}
}

func BenchmarkEnqueueParallel(b *testing.B) {
	broker := NewBroker()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			broker.enqueue("bench-queue", "payload")
		}
	})
}

func BenchmarkEnqueueDequeueParallelMultiQueue(b *testing.B) {
	broker := NewBroker()
	numQueues := 20
	// Pre-seed each queue so dequeues have work early on.
	for q := 0; q < numQueues; q++ {
		for i := 0; i < 100; i++ {
			broker.enqueue(fmt.Sprintf("queue-%d", q), "payload")
		}
	}
	b.ResetTimer()
	var counter int
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter++
			qName := fmt.Sprintf("queue-%d", counter%numQueues)
			broker.enqueue(qName, "payload")
			broker.dequeue(qName)
		}
	})
}
