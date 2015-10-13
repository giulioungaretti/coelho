package coelho

import (
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
	"golang.org/x/net/context"
)

// Subscribe consumes deliveries from an exclusive queue from an exchange and
// sends to the application specific messages chan.
//******** NOTE that mesasges are not acked.**************
// it's the client responsability  to ack the message.
//Handles shutting down gracefully in case of sig-int. Or disconnects.
func (r Rabbit) Subscribe(sessions chan Session, messages chan<- Message, ctx context.Context, counts *uint64) {
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
		err := sub.Channel.Qos(
			//TODO benchamrk
			10,    // prefetch count
			0,     // prefetch size
			false) // global
		if err != nil {
			log.Errorf("Error setting Qos", err)
			continue
		}
		//Deliveries on the returned chan will be buffered indefinitely.  To limit memory
		//of this buffer, use the Channel.Qos method to limit the amount of
		//unacknowledged/buffered deliveries the server will deliver on this Channel.
		deliveries, err := sub.Consume(queue, "", autoAck, false, false, nowait, nil)
		if err != nil {
			log.Errorf("cannot consume from: %q, %v", queue, err)
			// try again
			continue
		}
		select {
		default:
			for msg := range deliveries {
				atomic.AddUint64(counts, 1)
				//this will never end because deliveries is closed
				// only on connection/amqp-channel errors.
				select {
				case <-time.After(1 * time.Second):
					// if we wait more than 10 * Millisecond to send trhough the
					// channel it means  that the reciever is blocked so we just
					// exit and avoid losing too much messages
					log.Warnf("Timeout")
					// NOTE should i reject the message here ?
					msg.Reject(true)
					continue
				case messages <- Message{
					Body: msg.Body,
					Rk:   msg.RoutingKey,
					Msg:  msg,
				}:
					// msg is not acked
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
