package core

import (
	"fmt"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

// RabbitMQConnection holds the details of a connection
type RabbitMQConnection struct {
	addr string
	port string
}

// format returns the formattet string connection to dial
func (r *RabbitMQConnection) format() (s string) {
	s = fmt.Sprintf("amqp://guest:guest@%s:%s/", r.addr, r.port)
	return
}

// RabbitMQQueue hold the details of a queue
type RabbitMQQueue struct {
	Con       *amqp.Connection
	Ch        *amqp.Channel // channel to which the queue is attached
	Queue     amqp.Queue
	Name      string
	Durable   bool
	Delete    bool // delete when usused
	Exclusive bool
	NoWait    bool
	Arguments map[string]interface{}
}

// decleare returns a decleared queue
func (q *RabbitMQQueue) declare() (err error) {
	qd, err := q.Ch.QueueDeclare(
		q.Name,
		q.Durable,
		q.Delete,
		q.Exclusive,
		q.NoWait,
		q.Arguments,
	)
	if err != nil {
		q.Queue = qd
		return nil
	}
	return err
}

// Connect starts an amqp connection and returns an initialized queue.
// TODO this should read from env, or json or something alike
func Connect() (RabbitMQQueue, error) {
	c := RabbitMQConnection{}
	c.addr = "192.168.56.10"
	c.port = "5672"
	conn, err := amqp.Dial(c.format())
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q := RabbitMQQueue{}
	q.Con = conn
	q.Ch = ch
	q.Name = "task"
	// mimimal guarantee to not lose message.
	q.Durable = true
	q.Delete = false
	q.Exclusive = false
	q.NoWait = false
	q.Arguments = nil
	err = q.declare()
	if err != nil {
		return q, nil
	}
	return q, err
}
