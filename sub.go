package coelho

import (
	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
	"sync/atomic"
	"time"
)

// Subscribe consumes deliveries from an exclusive queue from an exchange and
// sends to the application specific messages chan.  Handles shutting down
// gracefully in case of sig-int. Or disconnects.
func (r Rabbit) Subscribe(sessions chan Session, messages chan<- Message, ctx context.Context, done context.CancelFunc, counts *uint64) {
	DieGracefully(done)
	queue := r.Name
	// subscribe forever
	for sub := range sessions {
		// declaere quque
		if _, err := r.DeclareQueue(sub.Channel); err != nil {
			log.Errorf("cannot consume from queue: %q, %v", queue, err)
			// try again
			continue
		}

		if err := r.Bind(sub.Channel); err != nil {
			log.Errorf("cannot consume without a binding to exchange: %+v, %v", r, err)
			continue
		}
		autoAck := false
		nowait := true
		//Deliveries on the returned chan will be buffered indefinitely.  To limit memory
		deliveries, err := sub.Consume(queue, "", autoAck, false, false, nowait, nil)
		if err != nil {
			log.Errorf("cannot consume from: %q, %v", queue, err)
			// try again
			continue
		}

		for msg := range deliveries {
			atomic.AddUint64(counts, 1)
			//this will never end because deliveries is closed
			// only on connection/amqp-channel errors.
			select {
			case <-time.After(10 * time.Millisecond):
				// if we wait more than 10 * Millisecond to send trhough the
				// channel it means  that the reciever is blocked so we just
				// exit and avoid losing too much messages
				log.Warnf("Timeout")
				return
			case messages <- Message{msg.Body, msg.RoutingKey}:
				msg.Ack(false)
			case <-ctx.Done():
				log.Infof("Closed session.")
				sub.Close()
				done()
				return
			}
		}
	}
}
