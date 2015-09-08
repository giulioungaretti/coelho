package core

import (
	"coelho/env"
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
type rabbitMQConnection struct {
	addr string
	port string
}

// format returns the formattet string connection to dial
func (r *rabbitMQConnection) format() (s string) {
	s = fmt.Sprintf("amqp://guest:guest@%s:%s/", r.addr, r.port)
	return
}

// Rabbit hold the details and the Con, Ch, Queue
type Rabbit struct {
	Arguments    map[string]interface{}
	Ch           *amqp.Channel // channel to which the queue is attached
	Con          *amqp.Connection
	Delete       bool // delete when usused
	Durable      bool
	Exchange     string
	Exclusive    bool
	Name         string
	NoWait       bool
	Queue        *amqp.Queue
	RK           string
	ExchangeType string
	Knows        bool
}

//DeclareExc decleares an exchange with false auto-delede, and false internal flags.
func (q *Rabbit) DeclareExc() {
	err := q.Ch.ExchangeDeclare(
		q.Exchange,     // name
		q.ExchangeType, // type
		q.Durable,      // durable
		false,          // auto-deleted
		false,          // internal
		q.NoWait,       // no-wait
		q.Arguments,    // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

// Bind the queue to an exchange
func (r *Rabbit) Bind() {
	log.Infof("Binding queue %s to exchange %s with routing key %s with %s exchange ",
		r.Queue.Name, r.Exchange, r.RK, r.ExchangeType)
	err := r.Ch.QueueBind(
		r.Queue.Name,
		r.RK,
		r.Exchange,
		r.NoWait,
		r.Arguments)
	failOnError(err, "Failed to bind a queue")
}

// DeclareQueue returns a decleared queue
func (q *Rabbit) DeclareQueue() {
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

// ConnectRaw connects a rabbit. The rabbit must know what to do.  So one has
// to manually  intialize the struct.  The rabbit will try to connect, create
// channel, exchange, queue and bind it to the exchange
//Example:
//q.Name = "events"
//q.RK = "#"
//// mimimal guarantee to not lose messages.
//q.Durable = true
//q.Delete = false
//q.Exclusive = false
//q.NoWait = false
//q.Arguments = nil
func (q *Rabbit) ConnectRaw() error {
	if !q.Knows {
		log.Fatalf("Poor rabbit does not know what to do, rember to tell him")
	}
	c := rabbitMQConnection{}
	c.addr = "192.168.56.10"
	c.port = "5672"
	conn, err := amqp.Dial(c.format())
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	q.Con = conn
	q.Ch = ch
	q.DeclareExc()
	q.DeclareQueue()
	q.Bind()
	return err
}

// ConnectRaw connects a Rabbit. The rabbit know what to do based on the env.Vars.
func (q *Rabbit) Connect(e *env.VARS) error {
	// init empty rabbit
	log.SetLevel(log.DebugLevel)
	c := rabbitMQConnection{}
	c.port = e.RabbitMqPort
	c.addr = e.RabbitMqAddres
	conn, err := amqp.Dial(c.format())
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	// now update the rabbit
	q.Con = conn
	q.Ch = ch
	q.Exchange = e.Exchange
	q.ExchangeType = e.ExchangeType
	q.Name = e.Name
	q.RK = e.RK
	q.Durable = e.Durable
	q.Delete = e.Delete
	q.Exclusive = e.Exclusive
	q.NoWait = e.NoWait
	// TODO  nil for now, we won't need them anyway.
	q.Arguments = nil
	// exc-queue-bind
	q.DeclareExc()
	q.DeclareQueue()
	q.Bind()
	return err
}
