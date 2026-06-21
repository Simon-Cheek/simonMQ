package main

import "fmt"

func main() {

	// Throughput Tests
	//fmt.Println("Starting first test")
	//simple1Res, noContent1 := measureThroughput(10, 500, "8080")
	//fmt.Println("Starting second test")
	//simple2Res, noContent2 := measureThroughput(10, 500, "8081")
	//fmt.Println("simple1Res:", simple1Res)
	//fmt.Println("noContent1:", noContent1)
	//fmt.Println("simple2Res:", simple2Res)
	//fmt.Println("noContent2:", noContent2)

	// Measure fill and drain speed
	//enq, drained := fillThenDrain(10, 5, "8081")
	//fmt.Printf("drained %d items (enqueued %d)\n", drained, enq)

	enq, drainTime := fillThenDrainContention(10, 10, 5, "8081")
	fmt.Println("enqueued:", enq, "drainTime:", drainTime)
}
