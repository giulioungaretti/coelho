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

// Rabbit hold the details and the Con, Ch, Queue
type Rabbit struct {
	Con       *amqp.Connection
	Ch        *amqp.Channel // channel to which the queue is attached
	Queue     *amqp.Queue
	Name      string
	Durable   bool
	Delete    bool // delete when usused
	Exclusive bool
	NoWait    bool
	RK        string
	Exchange  string
	Arguments map[string]interface{}
}

func (q *Rabbit) declareExc() {
	err := q.Ch.ExchangeDeclare(
		q.Exchange, // name
		"topic",    // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

// bind the queue to an exchange
func (r *Rabbit) bind() {
	log.Printf("Binding queue %s to exchange %s with routing key %s",
		r.Queue.Name, r.Exchange, r.RK)
	err := r.Ch.QueueBind(
		r.Queue.Name, // queue name
		r.RK,         // routing key
		r.Exchange,   // exchange
		false,
		nil)
	failOnError(err, "Failed to bind a queue")
}

// decleare returns a decleared queue
func (q *Rabbit) declareQueue() {
	qd, err := q.Ch.QueueDeclare(
		q.Name,
		q.Durable,
		q.Delete,
		q.Exclusive,
		q.NoWait,
		q.Arguments,
	)
	q.Queue = &qd
	failOnError(err, "Failed to declare a queue")
}

// Connect starts an amqp connection and returns an initialized queue.
// TODO this should read from env, or json or something alike
func (q *Rabbit) Connect() error {
	c := RabbitMQConnection{}
	c.addr = "192.168.56.10"
	c.port = "5672"
	conn, err := amqp.Dial(c.format())
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q.Con = conn
	q.Ch = ch
	q.Name = "events"
	q.Exchange = "events"
	q.RK = "#"
	// mimimal guarantee to not lose messages.
	q.Durable = true
	q.Delete = false
	q.Exclusive = false
	q.NoWait = false
	q.Arguments = nil
	q.declareExc()
	q.declareQueue()
	q.bind()
	return err
}
