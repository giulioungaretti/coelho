package core

import (
	"flag"
	"fmt"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
)

var (
	rabbitMqAddres string
	rabbitMqPort   string
	rabbitMqUsr    string
	rabbitMqPwd    string
)

func init() {
	// load env variables
	// crahses if they don't exist
	rabbitMqAddres = os.Getenv("rabbitMqAddres")
	if rabbitMqAddres == "" {
		log.Fatalf("RabbitMQ cluster addres was not found in the enviroment")
	}
	rabbitMqPort = os.Getenv("rabbitMqPort")
	if rabbitMqPort == "" {
		log.Fatalf("RabbitMQ cluster port was not found in the enviroment")
	}
	rabbitMqPwd = os.Getenv("rabbitMqPwd")
	if rabbitMqPwd == "" {
		log.Fatalf("RabbitMQ cluster pwd was not found in the enviroment")
	}
	rabbitMqUsr = os.Getenv("rabbitMqUsr")
	if rabbitMqUsr == "" {
		log.Fatalf("RabbitMQ cluster usr was not found in the enviroment")
	}
}

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
	Arguments map[string]interface{}
	Ch        *amqp.Channel // channel to which the queue is attached
	Con       *amqp.Connection
	Delete    bool // delete when usused
	Durable   bool
	Exchange  string
	Exclusive bool
	Name      string
	NoWait    bool
	Queue     *amqp.Queue
	RK        string
	Type      string
}

//DeclareExc decleares an exchange with false auto-delede, and false internal flags.
func (q *Rabbit) DeclareExc() {
	err := q.Ch.ExchangeDeclare(
		q.Exchange,  // name
		q.Type,      // type
		q.Durable,   // durable
		false,       // auto-deleted
		false,       // internal
		q.NoWait,    // no-wait
		q.Arguments, // arguments
	)
	failOnError(err, "Failed to declare an exchange")
}

// Bind the queue to an exchange
func (r *Rabbit) Bind() {
	log.Printf("Binding queue %s to exchange %s with routing key %s",
		r.Queue.Name, r.Exchange, r.RK)
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

// ConnectRaw connects a Rabbit. The rabbit know what to do based on flags.
func (q *Rabbit) Connect() error {
	// init empty rabbit
	c := rabbitMQConnection{}
	c.port = rabbitMqPort
	c.addr = rabbitMqAddres
	conn, err := amqp.Dial(c.format())
	failOnError(err, "Failed to connect to RabbitMQ")
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	// parse flags
	exch := flag.String("exchange", "events", "name of the rabbit exchange")
	name := flag.String("name", "events", "name of rabbit queue")
	rk := flag.String("rk", "#", "routing key to bind to")
	durable := flag.Bool("durable", true, "make the queue durable")
	nowait := flag.Bool("nowait", false, "make the queue nowait")
	delete := flag.Bool("delete", false, "make the queue delete")
	exclusive := flag.Bool("exclusive", false, "make the queue exclusive")
	flag.Parse()

	// now update the rabbitt
	q.Con = conn
	q.Ch = ch
	q.Exchange = *exch
	q.Name = *name
	q.RK = *rk
	q.Durable = *durable
	q.Delete = *delete
	q.Exclusive = *exclusive
	q.NoWait = *nowait
	// TODO  nil for now, we won't need them anyway.
	q.Arguments = nil

	// exc-queue-bind
	q.DeclareExc()
	q.DeclareQueue()
	q.Bind()
	return err
}
