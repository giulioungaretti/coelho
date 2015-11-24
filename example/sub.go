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
	"github.com/giulioungaretti/coelho"
	"github.com/giulioungaretti/coelho/env"
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

// write is this application's subscriber of application messages, printing to
// stdout.
func bunch(lines chan coelho.Message, done context.CancelFunc) (bufferCh chan []coelho.Message) {
	bufferCh = make(chan []coelho.Message, 3)
	go func() {
		buffer := make([]coelho.Message, cap(lines)/2)
		i := 0
		for {
			select {
			case line := <-lines:
				buffer[i] = line
				i++
			case <-ctx.Done():
				break
			}
			// buffer full ack them all and send bunch
			if i == cap(lines)/2 {
				last := buffer[i-1]
				err := last.Msg.Ack(true)
				i = 0
				if err != nil {
					log.Errorf("Error on ack: %v", err)
				}

				bufferCh <- buffer
			}
		}
	}()
	return bufferCh
}

func write(w io.Writer, end string, ch chan []coelho.Message) {
loop:
	for {
		select {
		default:
			continue loop
		case _ = <-ch:
			continue
		}
	}
}

func dispatch(e env.Vars, r coelho.Rabbit, rk string) {
	lines := make(chan coelho.Message, r.QoS)
	// set the right routing key
	r.RK = rk
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
	linesCh := bunch(lines, done)
	go write(os.Stdout, rk, linesCh)
	<-ctx.Done()
}

func main() {
	e := env.Init()
	r := coelho.Rabbit{}
	r.Exchange = e.Exchange
	r.ExchangeType = e.ExchangeType
	r.Durable = e.Durable
	r.RK = e.RK
	r.Delete = e.Delete
	r.Exclusive = e.Exclusive
	r.NoWait = e.NoWait
	r.QoS = 100
	flag.Parse()
	go dispatch(e, r, "offer.click")
	go dispatch(e, r, "catalog.view")
	<-ctx.Done()
}
