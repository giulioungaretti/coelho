// +build ignore
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"

	"github.com/giulioungaretti/coelho"
	"github.com/giulioungaretti/coelho/env"
	"github.com/streadway/amqp"
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

func body(line []string) []byte {
	s := strings.Join(line[1:], " ")
	return []byte(s)
}

func rk(line []string) string {
	s := line[0]
	return s
}

// read is this application's translation to the coelho.Message format, scanning from
// stdin.
func read(r io.Reader) <-chan coelho.Message {
	lines := make(chan coelho.Message)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		for scan.Scan() {
			msg := amqp.Delivery{}
			line := strings.Split(scan.Text(), " ")
			lines <- coelho.Message{body(line), rk(line), msg}
		}
	}()
	return lines
}

// write is this application's subscriber of application messages, printing to
// stdout.
func write(w io.Writer) chan<- coelho.Message {
	messages := make(chan coelho.Message)
	go func() {
		for msg := range messages {
			fmt.Fprintln(w, string(msg.Body))
		}
	}()
	return messages
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
		r.Publish(r.Redial(ctx, e.RabbitMqAddres), read(os.Stdin), done)
		done()
	}()

	<-ctx.Done()
}
