package main

import (
	"flag"
	"rabbit/receive"
	"rabbit/send"
)

var (
	receiveMode = flag.Bool("receive", true, "should run in receive mode?")
	sendMode    = flag.Bool("send", true, "should run in send mode?")
	msg         = flag.String("msg", "", "messaage to send")
)

func main() {
	flag.Parse()
	if *receiveMode {
		receive.Forever()
	} else {
		send.Msg(*msg)
	}
}
