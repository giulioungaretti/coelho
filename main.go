package main

import (
	"coelho/env"
	"coelho/receive"
	"coelho/send"
)

var (
	envVars env.VARS
)

func init() {
	// init the env variables and flags
	envVars = env.Init()
}

func main() {
	if envVars.Receive {
		receive.Example(&envVars)
	}
	if envVars.Send {
		send.BodyFromStdIn(&envVars)
	}
}
