package main

import (
	"fmt"
	"sync"
	"time"
)

// SafeCounter is safe to use concurrently.
type SafeCounter struct {
	mu      sync.Mutex
	v       int
	channel chan int
}

// // Inc increments the counter for the given key.
// func (c *SafeCounter) Inc(key string) {
// 	c.mu.Lock()
// 	// Lock so only one goroutine at a time can access the map c.v.
// 	c.v[key]++
// 	c.mu.Unlock()
// }

// // Value returns the current value of the counter for the given key.
// func (c *SafeCounter) Value(key string) int {
// 	c.mu.Lock()
// 	// Lock so only one goroutine at a time can access the map c.v.
// 	defer c.mu.Unlock()
// 	return c.v[key]
// }

func (c *SafeCounter) Sleep(key string) {
	c.mu.Lock()
	c.v = 15
	time.Sleep(5 * time.Second) // sleep for 5 seconds
	fmt.Println("gonna release lock")
	c.mu.Unlock()
}

func (c *SafeCounter) UseChannel() {
	valFromChan := <-c.channel
	fmt.Println("valFromChan:", valFromChan)
}

func (c *SafeCounter) GetV() int {
	c.mu.Lock()
	val := c.v
	fmt.Println("v:", val)
	c.mu.Unlock()
	return val
}

func main() {
	myChan := make(chan int)
	c := SafeCounter{v: 5, channel: myChan}
	// for i := 0; i < 1000; i++ {
	// 	go c.Inc("somekey")
	// }

	go c.Sleep("somekey")

	time.Sleep(1 * time.Second)

	go c.UseChannel()
	myChan <- 4

	fmt.Print("c.GetV:", c.GetV())

	// time.Sleep(time.Second)
	// fmt.Println(c.Value("somekey"))
}
