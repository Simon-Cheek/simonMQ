package main

import "fmt"

func main() {
	simple1Res, noContent1 := measure(10, 500, "8080")
	simple2Res, noContent2 := measure(10, 500, "8081")
	fmt.Println("simple1Res:", simple1Res)
	fmt.Println("noContent1:", noContent1)
	fmt.Println("simple2Res:", simple2Res)
	fmt.Println("noContent2:", noContent2)
}
