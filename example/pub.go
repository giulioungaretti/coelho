package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"

	"github.com/giulioungaretti/coelho"
	"github.com/giulioungaretti/coelho/env"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

var (
	ctx    context.Context
	done   context.CancelFunc
	counts uint64
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
func read(r io.Reader) chan []amqp.Delivery {
	lines := make(chan []amqp.Delivery)
	go func() {
		defer close(lines)
		scan := bufio.NewScanner(r)
		var buffer []amqp.Delivery
		i := 0
		for scan.Scan() {
			line := strings.Split(scan.Text(), " ")
			msg := amqp.Delivery{Body: body(line), RoutingKey: rk(line)}
			buffer = append(buffer, msg)
			i++
			if i == 200 {
				lines <- buffer
				buffer = make([]amqp.Delivery, 0)
				i = 0
			}
		}
	}()
	return lines
}

// write is this application's subscriber of application messages, printing to
// stdout.
func write(w io.Writer) chan amqp.Delivery {
	messages := make(chan amqp.Delivery)
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
	r.Durable = e.Durable
	r.Delete = e.Delete
	r.Exclusive = e.Exclusive
	r.NoWait = e.NoWait
	flag.Parse()
	go func() {
		r.Publish(ctx, r.Redial(ctx, e.RabbitMqAddres), read(os.Stdin), &counts)
		done()
	}()
	<-ctx.Done()
	cc := atomic.LoadUint64(&counts)
	fmt.Printf("%v sendt\n", cc)
}
