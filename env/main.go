// Package env takes care of setting up the environment.
// If a the local development environment variable is not set to true // it loads from the environment else it defaults to the values
// defined in the const.
package env

import (
	"errors"
	"flag"
	"os"
	"runtime"
	"strconv"

	log "github.com/Sirupsen/logrus"
)

const (
	CPU = 16
)

var (
	//env
	rabbitMqAddres   string
	rabbitMqPort     string
	rabbitMqUsr      string
	rabbitMqPwd      string
	CHOELO_MAX_CORES int
	// flags
	logLevel    = flag.String("log_level", "warn", "pick log level (error|warn|debug)")
	delete      = flag.Bool("delete", false, "make the queue delete")
	durable     = flag.Bool("durable", true, "make the queue durable")
	exch        = flag.String("exchange", "events", "name of the rabbit exchange")
	exclusive   = flag.Bool("exclusive", false, "make the queue exclusive")
	name        = flag.String("name", "events", "name of rabbit queue")
	nowait      = flag.Bool("nowait", false, "make the queue nowait")
	rk          = flag.String("rk", "#", "routing key to bind to")
	exchType    = flag.String("tpye", "topic", "type of the exchange")
	receiveMode = flag.Bool("receive", false, "should run in receive mode?")
	sendMode    = flag.Bool("send", true, "should run in send mode?")
)

func convStringToInt(s string) int {
	if s == "" {
		return 0
	}
	res, err := strconv.ParseInt(s, 10, 8)
	if err != nil {
		return 0
	}
	return int(res)
}

// parseLogLevel parses and returns the desired log level
func parseLogLevel() (log.Level, error) {
	switch *logLevel {
	case "debug":
		return log.DebugLevel, nil
	case "warn":
		return log.WarnLevel, nil
	case "error":
		return log.ErrorLevel, nil
	}

	return log.ErrorLevel, errors.New("Incorrect log-level setting")
}
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
	CHOELO_MAX_CORES = convStringToInt(os.Getenv("CHOELO_MAX_CORES"))
	if CHOELO_MAX_CORES == 0 {
		log.Warnf("Using %v cores.", CPU)
		CHOELO_MAX_CORES = CPU
	}
	flag.Parse()
	level, err := parseLogLevel()
	if err != nil {
		log.Fatalf("Logging error: %v", err)
	}
	log.SetLevel(level)
	runtime.GOMAXPROCS(CPU)
}

type VARS struct {
	Delete         bool // delete when usused
	Durable        bool
	Exchange       string
	Exclusive      bool
	Name           string
	NoWait         bool
	RK             string
	ExchangeType   string
	RabbitMqAddres string
	RabbitMqPort   string
	RabbitMqUsr    string
	RabbitMqPwd    string
	Send           bool
	Receive        bool
}

// Init initializes the environment
func Init() VARS {
	v := &VARS{
		Exchange:       *exch,
		ExchangeType:   *exchType,
		Name:           *name,
		RK:             *rk,
		Durable:        *durable,
		Delete:         *delete,
		Exclusive:      *exclusive,
		NoWait:         *nowait,
		RabbitMqAddres: rabbitMqAddres,
		RabbitMqPort:   rabbitMqPort,
		RabbitMqUsr:    rabbitMqUsr,
		RabbitMqPwd:    rabbitMqPwd,
		Receive:        *receiveMode,
		Send:           *sendMode,
	}
	return *v
}
