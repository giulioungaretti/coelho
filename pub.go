package coelho

import (
	"sync/atomic"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"golang.org/x/net/context"
)

// Publish publishes messages to a reconnecting Session to an exchange.
// It receives from the application specific source of messages.
// Keeps a pending channel in case of small timeouts.
// If the queues are not declared messages go in the rabbit-hole.
func (r Rabbit) Publish(ctx context.Context, sessions chan Session, messages chan []amqp.Delivery, counts *uint64) {
	//var (
	//pending = make([]Message, maxPending)
	//)
	for pub := range sessions {
		log.Debug("[x] publishing")
	msgloop:
		for {
			select {
			case msg := <-messages:
				for _, m := range msg {
					// exchange, key string, mandatory, immediate bool, msg Publishing
					err := pub.Publish(r.Exchange, m.RoutingKey, false, false, amqp.Publishing{
						Body: m.Body,
					})
					// Retry failed delivery on the next Session
					atomic.AddUint64(counts, 1)
					if err != nil {
						log.Errorf("Failed sending message%v", msg)
						pub.Close()
						break msgloop
					}
				}
			case <-ctx.Done():
				pub.Close()
				return
			default:

			}
		}
	}
}
