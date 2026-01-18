package main

import (
	"flag"
	"fmt"
)

func main() {
	// preberemo argumente iz ukazne vrstice
	sPtr := flag.String("s", "", "server URL")
	pPtr := flag.Int("p", 9876, "port number")
	numServers := flag.Int("n", 1, "number of servers in the chain")
	flag.Parse()

	// zaženemo strežnik ali odjemalca
	if *sPtr == "" {
		// Server mode: start chain of servers
		StartServerChain(*pPtr, *numServers)
	} else {
		// Client mode: connect to first server in chain
		url := fmt.Sprintf("%v:%v", *sPtr, *pPtr)
		Client(url)
	}
}
