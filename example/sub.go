// +build ignore
package main

import (
	"bufio"
	"coelho"
	"coelho/env"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"

	"golang.org/x/net/context"
)

var (
	ctx  context.Context
	done context.CancelFunc
)

func init() {
	// start global context
	ctx, done = context.WithCancel(context.Background())
	// controls how to close matilde.
	// it intercepts ^c and instead of sending SIGTERM
	// it makes all the function to return as if they were done with their job.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			if sig == os.Interrupt {
				fmt.Printf("Captured %v, Shutting down gracefully.\n", sig)
				done()
			}
		}
	}()

}

// read is this application's translation to the coelho.Message format, scanning from
// stdin.
func read(r io.Reader) <-chan coelho.Message {
	lines := make(chan coelho.Message)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			lines <- coelho.Message(scan.Bytes())
		}
	}()
	return lines
}

// write is this application's subscriber of application messages, printing to
// stdout.
func write(w io.Writer) chan<- coelho.Message {
	lines := make(chan coelho.Message)
	go func() {
		for line := range lines {
			fmt.Fprintln(w, string(line))
		}
	}()
	return lines
}

func main() {
	e := env.Init()
	r := coelho.Rabbit{}
	r.Exchange = e.Exchange
	r.ExchangeType = e.ExchangeType
	r.Name = e.Name
	r.RK = e.RK
	r.Durable = e.Durable
	r.Delete = e.Delete
	r.Exclusive = e.Exclusive
	r.NoWait = e.NoWait
	flag.Parse()
	go func() {
		r.Subscribe(r.Redial(ctx, e.RabbitMqAddres), write(os.Stdout), done)
		done()
	}()
	<-ctx.Done()
}
