// +build ignore

package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/codegangsta/cli"
	"github.com/giulioungaretti/coelho"
	"github.com/giulioungaretti/coelho/env"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

var (
	ctx  context.Context
	done context.CancelFunc
	read uint64
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

// ack acks all the messages of teh bunch.
func ack(w io.Writer, ch chan []amqp.Delivery) {
loop:
	for {
		select {
		default:
			continue loop
		case bunch := <-ch:
			bunch[len(bunch)-1].Ack(true)
		}
	}
}

func dispatch(e env.Vars, r coelho.Rabbit, rk string) {
	lines := make(chan amqp.Delivery, r.QoS)
	// set the right routing key
	// monitor
	go func() {
		for {
			log.Infof("message buffer:%v, used:%v", cap(lines), len(lines))
			//dev
			time.Sleep(30 * time.Second)
		}
	}()
	// Subscribe
	go func() {
		for {
			r.Subscribe(ctx, r.Redial(ctx, e.RabbitMqAddres), lines, rk, &read)
		}
		done()
	}()
	// process
	bunchedLines := coelho.Bunch(ctx, lines, 100, 100)
	go ack(os.Stdout, bunchedLines)
	<-ctx.Done()
}

func sub(ctx context.Context, c *cli.Context, queue []string) {
	e := env.Init()
	r := coelho.Rabbit{}
	r.Exchange = e.Exchange
	r.ExchangeType = e.ExchangeType
	r.Durable = e.Durable
	r.Delete = e.Delete
	r.Exclusive = e.Exclusive
	r.NoWait = e.NoWait
	r.QoS = 100
	flag.Parse()
	for _, queue := range queues {
		dispatch(e, r, queue)
	}
	<-ctx.Done()
}

func main() {
	app := cli.NewApp()
	app.Name = "Subscribe"
	app.Version = "0.1"
	app.Usage = "Read msg from queues specified in the arguments, close with ^c"
	app.Action = func(c *cli.Context) {
	}
	app.Run(os.Args)
}
