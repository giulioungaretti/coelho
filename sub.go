package coelho

import (
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// subscribe consumes deliveries from an exclusive queue from a fanout exchange and sends to the application specific messages chan.
func (r Rabbit) Subscribe(sessions chan chan Session, messages chan<- Message, done context.CancelFunc) {
	DieGracefully(done)
	queue := r.Name
	for Session := range sessions {
		sub := <-Session
		// declaere quque
		if _, err := r.DeclareQueue(sub.Channel); err != nil {
			log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
			return
		}

		if err := r.Bind(sub.Channel); err != nil {
			log.Printf("cannot consume without a binding to exchange: %+v, %v", r, err)
			return
		}

		deliveries, err := sub.Consume(queue, "", false, true, false, false, nil)
		if err != nil {
			log.Printf("cannot consume from: %q, %v", queue, err)
			return
		}

		log.Printf("subscribed...")

		for msg := range deliveries {
			messages <- Message(msg.Body)
			sub.Ack(msg.DeliveryTag, false)
		}
	}
}
