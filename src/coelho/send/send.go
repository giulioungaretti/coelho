package send

import (
	"bufio"
	"coelho/core"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

/*
 * TODO
 *- write a goroutine which keeps the connection up and working all the time
 *- buffered channel to send to it, so if the connection is down, then you can't send
 */
func Msg(r core.Rabbit, body string, rk string) {
	err := r.Ch.Publish(
		"events", // exchange
		rk,       // routing key
		false,    // mandatory
		false,    // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func BodyFromStdIn() {
	queue := core.Rabbit{}
	err := queue.Connect()
	failOnError(err, "Failed to declare a queue")
	defer queue.Con.Close()
	defer queue.Ch.Close()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		text := strings.Split(scanner.Text(), " ")
		if len(text) > 1 {
			Msg(queue, body(text), rk(text))
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func body(args []string) string {
	s := strings.Join(args[1:], " ")
	return s
}

func rk(args []string) string {
	s := args[0]
	return s
}
