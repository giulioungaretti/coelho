package coelho

import (
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// Subscribe consumes deliveries from an exclusive queue from an exchange and sends to the application specific messages chan.
// handles shutting down gracefully in case of sig-int. Or disconnects.
func (r Rabbit) Subscribe(sessions chan Session, messages chan<- Message, ctx context.Context, done context.CancelFunc) {
	DieGracefully(done)
	queue := r.Name
	// subscribe forever
	for sub := range sessions {
		// declaere quque
		if _, err := r.DeclareQueue(sub.Channel); err != nil {
			log.Errorf("cannot consume from exclusive queue: %q, %v", queue, err)
			// try again
			continue
		}

		if err := r.Bind(sub.Channel); err != nil {
			log.Errorf("cannot consume without a binding to exchange: %+v, %v", r, err)
			continue
		}
		autoAck := true
		nowait := true
		deliveries, err := sub.Consume(queue, "", autoAck, false, false, nowait, nil)
		if err != nil {
			log.Errorf("cannot consume from: %q, %v", queue, err)
			// try again
			continue
		}
		for msg := range deliveries {
			//this will never end because deliveries is closed
			// only on connection/amqp-channel errors.
			select {
			case messages <- Message{msg.Body, msg.RoutingKey}:
			case <-ctx.Done():
				log.Infof("Closed session.")
				sub.Close()
				done()
				return
			}
		}
	}
}
