package main

import (
	"coelho/receive"
	"coelho/send"
	"flag"
)

var (
	receiveMode = flag.Bool("receive", false, "should run in receive mode?")
	sendMode    = flag.Bool("send", false, "should run in send mode?")
)

func main() {
	flag.Parse()
	if *receiveMode {
		receive.Forever()
	}
	if *sendMode {
		send.BodyFromStdIn()
	}
}
