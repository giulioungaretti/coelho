package coelho

import (
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// Publish publishes messages to a reconnecting Session to an exchange.
// It receives from the application specific source of messages.
// Keeps a pending channel in case of small timeouts.
// If the queues are not declared messages go in the rabbit-hole.
func (r Rabbit) Publish(ctx context.Context, sessions chan Session, messages <-chan Message, queueName string) {
	var (
		running bool
		reading = messages
		pending = make(chan Message, 1)
	)

	for pub := range sessions {
		log.Infof("[x] publishing")
		for {
			var msg Message
			select {
			default:
				reading = messages
			case msg = <-pending:
				// exchange, key string, mandatory, immediate bool, msg Publishing
				err := pub.Publish(r.Exchange, msg.Rk, false, false, amqp.Publishing{
					Body: msg.Body,
				})
				// Retry failed delivery on the next Session
				if err != nil {
					pending <- msg
					pub.Close()
					break
				}
			case msg, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- msg
				reading = nil
			case <-ctx.Done():
				return
			}
		}
	}
}
