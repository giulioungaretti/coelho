package coelho

import (
	log "github.com/Sirupsen/logrus"

	"golang.org/x/net/context"

	"github.com/streadway/amqp"
)

// publish publishes messages to a reconnecting Session to a fanout exchange.
// It receives from the application specific source of messages.
func (r Rabbit) Publish(sessions chan chan Session, messages <-chan Message, done context.CancelFunc) {
	defer DieGracefully(done)
	var (
		running bool
		reading = messages
		pending = make(chan Message, 1)
	)

	for Session := range sessions {
		pub := <-Session
		// declaere quque
		if _, err := r.DeclareQueue(pub.Channel); err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", r.Name, err)
			return
		}
		log.Printf("[x] publishing")
		for {
			var body Message
			select {
			default:
				reading = messages
				panic("FOO")
			case body = <-pending:
				//TODO pass info here
				// exchange, key string, mandatory, immediate bool, msg Publishing
				err := pub.Publish(r.Exchange, r.RK, false, false, amqp.Publishing{
					Body: body,
				})
				// Retry failed delivery on the next Session
				if err != nil {
					pending <- body
					pub.Close()
					break
				}

			case body, running = <-reading:
				// all messages consumed
				if !running {
					return
				}
				// work on pending delivery until ack'd
				pending <- body
				reading = nil
			}
		}
	}
}
