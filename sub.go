package coelho

import (
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// subscribe consumes deliveries from an exclusive queue from a fanout exchange and sends to the application specific messages chan.
func (r Rabbit) Subscribe(sessions chan Session, messages chan<- Message, done context.CancelFunc) {
	DieGracefully(done)
	queue := r.Name
	// subscribe forever
	for {
		for sub := range sessions {
			//sub := <-Session
			// declaere quque
			if _, err := r.DeclareQueue(sub.Channel); err != nil {
				log.Printf("cannot consume from exclusive queue: %q, %v", queue, err)
				return
			}

			if err := r.Bind(sub.Channel); err != nil {
				log.Printf("cannot consume without a binding to exchange: %+v, %v", r, err)
				return
			}
			autoAck := true
			nowait := true
			deliveries, err := sub.Consume(queue, "", autoAck, false, false, nowait, nil)
			if err != nil {
				log.Printf("cannot consume from: %q, %v", queue, err)
				return
			}
			log.Printf("subscribed...")
			// connection close
			// channel down
			// sub closed
			i := 0
			for msg := range deliveries {
				i++
				// test breaking, and thus restarting
				if a := i % 100; a == 0 {
					messages <- Message(msg.Body)
					log.Printf("breaking")
					break
				}
			}
			sub.Close()
		}
	}
}
