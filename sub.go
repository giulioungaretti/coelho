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
func (r Rabbit) Subscribe(ctx context.Context, sessions chan Session, messages chan<- amqp.Delivery, queueName string, counts *uint64) {
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
		deliveries, err := sub.Consume(q.Name, "", r.AutoAck, r.Exclusive, false, r.NoWait, nil)
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
					// if we wait more than 1 * Second to send thorough the
					// channel it means  that the reciever is blocked so we just
					// wait a bit  and avoid losing too much messages
					log.Warnf("Timeout")
					mutliple := true
					requeue := true
					msg.Nack(mutliple, requeue)
					time.Sleep(3 * time.Second)
					log.Warnf("end Timeout")
				case messages <- msg:
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

// Bunch goes through a channel containing amqp deliveries and bunches them up in arrays of size
// bunchLen and then forwards onto a channel with a buffer chanBuf.
// Bunch never stops sending unless the context done channel is closed.
func Bunch(ctx context.Context, messages chan amqp.Delivery, bunchLen, chanBuff int) (bufferCh chan []amqp.Delivery) {
	bufferCh = make(chan []amqp.Delivery, chanBuff)
	go func() {
		var buffer []amqp.Delivery
		i := 0
		t0 := time.Now()
	loop:
		for {
			select {
			case msg := <-messages:
				buffer = append(buffer, msg)
				i++
			case <-ctx.Done():
				break loop
			default:
			}
			if i == bunchLen {
				bufferCh <- buffer
				buffer = make([]amqp.Delivery, 0)
				i = 0
				t0 = time.Now()
			}
			// clear messages after 120 seconds of inactivity
			if time.Since(t0).Seconds() > 120 {
				bufferCh <- buffer
				buffer = make([]amqp.Delivery, 0)
				i = 0
				t0 = time.Now()
			}
		}
	}()
	return bufferCh
}
