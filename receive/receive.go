package receive

import (
	"coelho/core"
	"coelho/env"
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

type Consumer struct {
	Queue         core.Rabbit
	QueueName     string
	Consumer      string
	AutoAck       bool
	Exclusive     bool
	NoLocal       bool
	NoWait        bool
	Arguments     map[string]interface{}
	PrefetchCount int
	PrefetchSize  int
	Global        bool
	IsSetup       bool
}

func (c *Consumer) SetUp() (msgs <-chan amqp.Delivery) {
	// set fair dispatch
	err := c.Queue.Ch.Qos(
		c.PrefetchCount, // prefetch count
		c.PrefetchSize,  // prefetch size
		c.Global,        // global
	)
	failOnError(err, "Failed to set QoS")
	// set up consuming
	msgs, err = c.Queue.Ch.Consume(
		c.QueueName,
		c.Consumer,
		c.AutoAck,
		c.Exclusive,
		c.NoLocal,
		c.NoWait,
		c.Arguments,
	)
	failOnError(err, "Failed to register a consumer")
	c.IsSetup = true
	return msgs
}

// Action is the function we want to perform on
// every incoming action. Pay attention to
// ack-ing / nack-ing
type Action func(amqp.Delivery)

/*
 * Forever reads forever from the quque specified
 * in VAERS, with a Consumer configuration  that
 * has to be speficied
 *Example
*     queue := core.Rabbit{}
*	  err := queue.Connect(e)
*     failOnError(err, "Failed to register a consumer")
*	  defer queue.Con.Close()
*	  defer queue.Ch.Close()
*    // set up consumer
*    c := Consumer{
*        QueueName:     queue.Name, //
*        Consumer:      "",         // consumer
*        AutoAck:       false,      // auto-ack
*        Exclusive:     false,      // exclusive
*        NoLocal:       false,      // no-local
*        NoWait:        false,      // no-wait
*        Arguments:     nil,        // args
*        PrefetchSize:  0,
*        PrefetchCount: 1,
*        Global:        false,
*    }
*
*    msgs := c.SetUp()
*/
func Forever(msgs <-chan amqp.Delivery, action Action) {
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			action(d)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

// ExampleAction is an example function that acts on the messages/deliveries
// from a consumer
func ExampleAction(d amqp.Delivery) {
	log.Printf("Received a message: %s", d.RoutingKey)
	log.Printf("Received a message: %s", d.Body)
	d.Ack(false) // multiple = false
}

// Example shows how to how to read Forever
func Example(e *env.VARS) {
	queue := core.Rabbit{}
	err := queue.Connect(e)
	failOnError(err, "Failed to register a consumer")
	defer queue.Con.Close()
	defer queue.Ch.Close()
	// set up consumer
	c := Consumer{
		Queue:         queue,
		QueueName:     queue.Name, //
		Consumer:      "",         // consumer
		AutoAck:       false,      // auto-ack
		Exclusive:     false,      // exclusive
		NoLocal:       false,      // no-local
		NoWait:        false,      // no-wait
		Arguments:     nil,        // args
		PrefetchSize:  0,
		PrefetchCount: 1,
		Global:        false,
	}
	msgs := c.SetUp()
	Forever(msgs, ExampleAction)
}
