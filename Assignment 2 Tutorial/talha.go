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

const HeartbeatInterval = 1000 * time.Millisecond

func periodicallySendHeartBeats(chanStop chan bool) {

	// resend heartbeats
	fmt.Println("Sending Heartbeats")

	// get timer for a heartbeat interval
	heartbeatTimer := time.NewTimer(HeartbeatInterval)

	exit := false

	for {

		select {

		// if a duration of HeartbeatInterval has passed since last heartbeat:
		case <-time.After(1 * time.Second):

			// resend heartbeats
			fmt.Println("Sending Heartbeats")

			// // restart timer:
			// // 1. stop timer
			if !heartbeatTimer.Stop() {
				// just ensuring that if the channel has value,
				// drain it before restarting (so we dont leak resources
				// for keeping the channel indefinately up)
				fmt.Println("came here 0")
				<-heartbeatTimer.C
				fmt.Println("came here 1")
			}
			// 2. reset timer
			heartbeatTimer.Reset(HeartbeatInterval)

		// main thread is asking us to stop sending heartbeats
		case <-chanStop:
			exit = true

		}

		if exit {
			break
		}
	}
}

func main() {
	endChan := make(chan bool)
	go periodicallySendHeartBeats(endChan)

	time.Sleep(5 * time.Second)
	fmt.Println("ending")
	endChan <- true
	// time.Sleep(5 * time.Second)

}
