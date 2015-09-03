package receive

import (
	"coelho/core"
	"fmt"
	"log"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func Forever() {
	queue := core.Rabbit{}
	err := queue.Connect()
	failOnError(err, "Failed to register a consumer")
	defer queue.Con.Close()
	defer queue.Ch.Close()
	// set fair dispatch
	err = queue.Ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "Failed to set QoS")
	// set up consuming
	msgs, err := queue.Ch.Consume(
		queue.Name, // queue
		"",         // consumer
		false,      // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	failOnError(err, "Failed to register a consumer")
	forever := make(chan bool)
	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.RoutingKey)
			log.Printf("Received a message: %s", d.Body)
			d.Ack(false)
		}
	}()
	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
