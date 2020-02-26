package main

import (
	"fmt"
	"time"
)


func statusUpdate() string { return "" }

func main() {
	c := time.Tick(5 * time.Second)
	fmt.Println("start")
	defer fmt.Println("end")
	for now := range c {
		fmt.Printf("%v %s\n", now, statusUpdate())
	}
}

