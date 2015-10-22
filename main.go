// package coelho offers  pub/sub functions to send/recieve messages from rabbitMQ.
// The idea is to have a simple exported Subscribe and Publish functions.
// Sub
package coelho

import (
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
	"time"
)

// Message mirrors a rabbitMQ message
type Message struct {
	Body []byte
	Rk   string
	Msg  amqp.Delivery
}

// Session composes an amqp.Connection with an amqp.Channel
type Session struct {
	*amqp.Connection
	*amqp.Channel
}

// Rabbit holds the details of the Con, Ch, Queue
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

// DeclareExc decleares an exchange with false auto-delete, and false internal flags.
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

// Close tears the connection down, taking the channel with it.
func (s Session) Close() error {
	if s.Connection == nil {
		return nil
	}
	return s.Connection.Close()
}

/*
 *Redial continually connects to the URL, returns no longer possible
 * no guarantee on the number of sessions returned on close.
 *==============
 *URL reference
 *amqp://user:pass@host:10000/vhost
 *==============
 */
func (r Rabbit) Redial(ctx context.Context, url string) chan Session {
	sessions := make(chan Session)
	go func() {
		defer close(sessions)
		for {
			select {
			default:
				log.Info("Dialing")
			case <-ctx.Done():
				log.Info("shutting down Session factory")
				return
			}
			conn, err := amqp.Dial(url)
			if err != nil {
				log.Warnf("Can't dial. Waiting 10 seconds...")
				time.Sleep(10 * time.Second)
				conn, err = amqp.Dial(url)
				if err != nil {
					log.Errorf("cannot (re)dial: %v: %q", err, url)
					return
				}
			}

			ch, err := conn.Channel()
			if err != nil {
				log.Errorf("cannot create channel %v: %v", r.Exchange, err)
				return
			}
			// idempotent declaration
			if err := r.DeclareExc(ch); err != nil {
				log.Errorf("cannot declare %v exchange: %v", r.ExchangeType, err)
				return
			}

			select {
			// this will block here if the subscriber is not using the session
			case sessions <- Session{conn, ch}:
				log.Info("New session init.")
			case <-ctx.Done():
				log.Infof("shutting down new Session")
				return
			}
		}
	}()
	return sessions
}
