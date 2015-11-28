package coelho

import (
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// Subscribe consumes deliveries from an exclusive queue from an exchange and
// sends to the application specific messages chan.
//******** NOTE that mesasges are not acked.**************
// it's the client responsability  to ack the message.
//Handles shutting down gracefully in case of sig-int. Or disconnects.
func (r Rabbit) Subscribe(ctx context.Context, sessions chan Session, messages chan []amqp.Delivery, queueName string, counts *uint64) {
	// subscribe forever
	for sub := range sessions {
		// declaere quque
		var q amqp.Queue
		var err error
		if q, err = r.DeclareQueue(sub.Channel, queueName); err != nil {
			log.Errorf("cannot consume from queue: %q. Error: %v", queueName, err)
			// try again
			continue
		}
		if err := r.Bind(sub.Channel, q.Name, q.Name); err != nil {
			log.Errorf("cannot consume without a binding to exchange: %+v. Erorr: %v", r, err)
			continue
		}
		autoAck := false
		deliveries, err := sub.Consume(q.Name, "", autoAck, false, true, false, nil)
		if err != nil {
			log.Errorf("cannot consume from: %q, %v", q.Name, err)
			// try again
			continue
		}
		var buffer []amqp.Delivery
		lenMsg := r.QoS
		var i int
		select {
		default:
		loop:
			for {
				select {
				case msg := <-deliveries:
					buffer = append(buffer, msg)
					atomic.AddUint64(counts, 1)
					i++
				case <-ctx.Done():
					break loop
				default:
				}
				if i == lenMsg {
					messages <- buffer
					buffer = make([]amqp.Delivery, 0)
					i = 0
				}
			}
			log.Infof("Closed session.")
			sub.Close()
		case <-ctx.Done():
			log.Infof("Closed session.")
			sub.Close()
			return
		}
	}
}
