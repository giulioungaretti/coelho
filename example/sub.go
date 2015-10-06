// +build ignore
package main

import (
	"coelho"
	"coelho/env"
	"flag"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"time"
)

var (
	ctx  context.Context
	done context.CancelFunc
	read uint64 = 0
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
func write(w io.Writer, lines chan coelho.Message, done context.CancelFunc) {
	go func() {
		for {
			select {
			default:
				a := rand.Intn(10000000)
				if a == 100 {
					line := <-lines
					err := line.Msg.Reject(true)
					if err != nil {
						log.Errorf("Error: %v on ac", err)
					}
					log.Warn("send it back: %v", string(line.Rk))

					continue
				}
			case line := <-lines:
				err := line.Msg.Ack(false)
				if err != nil {
					log.Errorf("Error: %v on ac", err)
				}
				l := fmt.Sprintf("%v %v", line.Rk, string(line.Body))
				fmt.Fprintln(w, l)
			case <-ctx.Done():
				break
			}
		}
		done()
	}()
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
	lines := make(chan coelho.Message, 1000)
	go func() {
		for {
			log.Infof("message buffer:%v, used:%v", cap(lines), len(lines))
			//dev
			time.Sleep(30 * time.Second)
		}
	}()
	go func() {
		for {
			r.Subscribe(r.Redial(ctx, e.RabbitMqAddres), lines, ctx, done, &read)
		}
		done()
	}()
	write(os.Stdout, lines, done)
	<-ctx.Done()
}
