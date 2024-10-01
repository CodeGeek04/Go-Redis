package main

import (
	"fmt"
	"hashtable"
)

func main() {
	ht := hashtable.New()
	time := ht.SetWithTime("k1", "1")
	fmt.Println("time", time)
	all := ht.ListAll()
	fmt.Println(all)
    // Use other methods as needed
}