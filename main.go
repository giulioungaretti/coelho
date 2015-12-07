// Package coelho offers  pub/sub functions to send/recieve messages from rabbitMQ.
// The idea is to have a simple exported Subscribe and Publish functions.
// Sub
package coelho

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// Session composes an amqp.Connection with an amqp.Channel
type Session struct {
	*amqp.Connection
	*amqp.Channel
}

// Rabbit holds the details of the Con, Ch
type Rabbit struct {
	Address      string
	Arguments    map[string]interface{}
	AutoAck      bool
	Delete       bool // delete when usused
	Durable      bool
	Exchange     string
	ExchangeType string
	Exclusive    bool
	Internal     bool
	NoWait       bool
	Passive      bool
	QoS          int
}

// DeclareExc decleares an exchange with false auto-delete, and false internal flags.
func (r Rabbit) DeclareExc(ch *amqp.Channel) error {
	if r.Passive {
		err := ch.ExchangeDeclarePassive(
			r.Exchange,     // name
			r.ExchangeType, // type
			r.Durable,      // durable
			r.Delete,       // auto-deleted
			r.Internal,     // internal
			r.NoWait,       // no-wait
			r.Arguments,    // arguments
		)
		return err
	}
	err := ch.ExchangeDeclare(
		r.Exchange,     // name
		r.ExchangeType, // type
		r.Durable,      // durable
		r.Delete,       // auto-deleted
		r.Internal,     // internal
		r.NoWait,       // no-wait
		r.Arguments,    // arguments
	)
	return err
}

// Bind the queue to an exchange
func (r Rabbit) Bind(ch *amqp.Channel, queueName string, rk string) error {
	log.Infof("Binding queue %s to exchange %s with routing key %s with %s exchange ",
		queueName, r.Exchange, rk, r.ExchangeType)
	err := ch.QueueBind(
		queueName,
		rk,
		r.Exchange,
		r.NoWait,
		r.Arguments)
	return err
}

// DeclareQueue returns a decleared queue
func (r Rabbit) DeclareQueue(ch *amqp.Channel, queueName string) (amqp.Queue, error) {
	qd, err := ch.QueueDeclare(
		queueName,
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

/*Redial continually connects to the URL, returns no longer possible
 *no guarantee on the number of sessions returned on close.
 *==============
 *URL reference
 *amqp://user:pass@host:port/vhost
 *a different URL-structure will not work
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
				log.Infof("Shutting down session factory")
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
			//Deliveries on the returned chan will be buffered indefinitely.  To limit memory
			//of this buffer, use the Channel.Qos method to limit the amount of
			//unacknowledged/buffered deliveries the server will deliver on this Channel.
			if r.QoS != 0 {
				err = ch.Qos(
					r.QoS, // prefetch count
					0,     // prefetch size
					false) // global
				if err != nil {
					log.Errorf("Error setting Qos %v", err)
				}
			}
			select {
			// this will block here if the subscriber is not using the session
			case sessions <- Session{conn, ch}:
				log.Info("New session has been initialized.")
			case <-ctx.Done():
				log.Infof("Shutting down session")
				return
			}
		}
	}()
	return sessions
}
