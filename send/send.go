package send

import (
	"fmt"
	"log"
	"rabbit/core"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func Msg(body string) {
	queue, err := core.Connect()
	failOnError(err, "Failed to declare a queue")
	defer queue.Con.Close()
	defer queue.Ch.Close()
	err = queue.Ch.Publish(
		"",         // exchange
		queue.Name, // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	log.Printf(" [x] Sent %s", body)
	failOnError(err, "Failed to publish a message")
}
