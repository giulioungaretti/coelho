// Command pubsub is an example of a fanout exchange with dynamic reliable
// membership, reading from stdin, writing to stdout.
//
// This example shows how to implement reconnect logic independent from a
// publish/subscribe loop with bridges to application types.

package coelho

import (
	log "github.com/Sirupsen/logrus"

	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// message is the application type for a message.  This can contain identity,
// or a reference to the recevier chan for further demuxing.
type Message []byte

// Session composes an amqp.Connection with an amqp.Channel
type Session struct {
	*amqp.Connection
	*amqp.Channel
}

// Rabbit hold the details and the Con, Ch, Queue
type Rabbit struct {
	Arguments    map[string]interface{}
	Delete       bool // delete when usused
	Durable      bool
	Exchange     string
	ExchangeType string
	Exclusive    bool
	Knows        bool
	Name         string
	NoWait       bool
	RK           string
}

//DeclareExc decleares an exchange with false auto-delede, and false internal flags.
func (r Rabbit) DeclareExc(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(
		r.Exchange,     // name
		r.ExchangeType, // type
		r.Durable,      // durable
		false,          // auto-deleted
		false,          // internal
		r.NoWait,       // no-wait
		r.Arguments,    // arguments
	)
	return err
}

// Bind the queue to an exchange
func (r Rabbit) Bind(ch *amqp.Channel) error {
	log.Infof("Binding queue %s to exchange %s with routing key %s with %s exchange ",
		r.Name, r.Exchange, r.RK, r.ExchangeType)
	err := ch.QueueBind(
		r.Name,
		r.RK,
		r.Exchange,
		r.NoWait,
		r.Arguments)
	return err
}

// DeclareQueue returns a decleared queue
func (r Rabbit) DeclareQueue(ch *amqp.Channel) (amqp.Queue, error) {
	qd, err := ch.QueueDeclare(
		r.Name,
		r.Durable,
		r.Delete,
		r.Exclusive,
		r.NoWait,
		r.Arguments,
	)
	return qd, err
}

//DieGracefully recovers from a panic, closes connections nicely
// and  panics again.
func DieGracefully(done context.CancelFunc) {
	if r := recover(); r != nil {
		log.Warnf("Recovered in  %v.", r)
		done()
		log.Info("Closing down")
	}
}

// Close tears the connection down, taking the channel with it.
func (s Session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

/*
 *Redial continually connects to the URL, exiting the program when no longer possible
 *==============
 *URL reference
 *amqp://user:pass@host:10000/vhost
 *==============
 */
func (r Rabbit) Redial(ctx context.Context, url string) chan chan Session {
	sessions := make(chan chan Session)

	go func() {
		sess := make(chan Session)
		defer close(sessions)

		for {
			select {
			case sessions <- sess:
			case <-ctx.Done():
				log.Info("shutting down Session factory")
				return
			}

			conn, err := amqp.Dial(url)
			if err != nil {
				log.Fatalf("cannot (re)dial: %v: %q", err, url)
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Fatalf("cannot create channel %v: %v", r.Exchange, err)
			}
			// idempotent declaration
			if err := r.DeclareExc(ch); err != nil {
				log.Fatalf("cannot declare %v exchange: %v", r.ExchangeType, err)
			}

			select {
			case sess <- Session{conn, ch}:
			case <-ctx.Done():
				log.Infof("shutting down new Session")
				return
			}
		}
	}()

	return sessions
}
