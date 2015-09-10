package coelho

import (
	log "github.com/Sirupsen/logrus"

	"golang.org/x/net/context"

	"github.com/streadway/amqp"
)

// publish publishes messages to a reconnecting Session to a fanout exchange.
// It receives from the application specific source of messages.
// Keeps a pending channel in case of small timeouts.
func (r Rabbit) Publish(sessions chan Session, messages <-chan Message, done context.CancelFunc) {
	defer DieGracefully(done)
	var (
		running bool
		reading = messages
		pending = make(chan Message, 1)
	)

	for pub := range sessions {
		if _, err := r.DeclareQueue(pub.Channel); err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", r.Name, err)
			return
		}
		log.Printf("[x] publishing")
		for {
			var msg Message
			select {
			default:
				reading = messages
			case msg = <-pending:
				//TODO pass info here
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
			}
		}
	}
}
