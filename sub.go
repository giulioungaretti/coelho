package coelho

import (
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// Subscribe consumes deliveries from an exclusive queue from an exchange and
// sends to the application specific messages chan.
//******** NOTE that mesasges are not acked.**************
// it's the client responsability  to ack the message.
//Handles shutting down gracefully in case of sig-int. Or disconnects.
func (r Rabbit) Subscribe(ctx context.Context, sessions chan Session, messages chan<- Message, queueName string, counts *uint64) {
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
		if err := r.Bind(sub.Channel, q.Name); err != nil {
			log.Errorf("cannot consume without a binding to exchange: %+v. Erorr: %v", r, err)
			continue
		}
		autoAck := false
		nowait := true
		err = sub.Channel.Qos(
			//TODO benchamrk
			r.QoS, // prefetch count
			0,     // prefetch size
			false) // global
		if err != nil {
			log.Errorf("Error setting Qos %v", err)
		}
		//Deliveries on the returned chan will be buffered indefinitely.  To limit memory
		//of this buffer, use the Channel.Qos method to limit the amount of
		//unacknowledged/buffered deliveries the server will deliver on this Channel.
		deliveries, err := sub.Consume(q.Name, "", autoAck, false, false, nowait, nil)
		if err != nil {
			log.Errorf("cannot consume from: %q, %v", q.Name, err)
			// try again
			continue
		}
		select {
		default:
			for msg := range deliveries {
				//this will never end because deliveries is closed
				// only on connection/amqp-channel errors.
				select {
				case <-time.After(1 * time.Second):
					// if we wait more than 1 * Second to send trhough the
					// channel it means  that the reciever is blocked so we just
					// exit and avoid losing too much messages
					log.Warnf("Timeout")
					mutliple := true
					requeue := true
					msg.Nack(mutliple, requeue)
					continue
				case messages <- Message{
					Body: msg.Body,
					Rk:   msg.RoutingKey,
					Msg:  msg,
				}:
					// msg is not acked
					atomic.AddUint64(counts, 1)
				case <-ctx.Done():
					log.Infof("Closed session.")
					sub.Close()
					return
				}
			}
		case <-ctx.Done():
			log.Infof("Closed session.")
			sub.Close()
			return
		}
	}
}
