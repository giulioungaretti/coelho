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

var (
	rabbitMqAddres string
	choeloMaxCores int
	// flags
	logLevel  = flag.String("log_level", "debug", "pick log level (error|warn|debug)")
	delete    = flag.Bool("delete", false, "make the queue delete")
	durable   = flag.Bool("durable", false, "make the queue durable")
	exch      = flag.String("exchange", "events", "name of the rabbit exchange")
	exclusive = flag.Bool("exclusive", false, "make the queue exclusive")
	nowait    = flag.Bool("nowait", false, "make the queue nowait")
	exchType  = flag.String("tpye", "direct", "type of the exchange")
	sendMode  = flag.Bool("send", true, "should run in send mode?")
	qos       = flag.Int("QoS", 100, "Number of prefechted msssages to get from rabbitMQ")
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
	flag.Parse()
	level, err := parseLogLevel()
	if err != nil {
		log.Fatalf("Logging error: %v", err)
	}
	log.SetLevel(level)
	cpus := convStringToInt(os.Getenv("cpus"))
	if cpus == 0 {
		cpus = runtime.NumCPU()
		log.Infof("Using %v cores.", cpus)
	}
	runtime.GOMAXPROCS(cpus)
}

// Vars contains the enviroment variables we need
// they can be changed by the user.
// RabbitMqAddres  **must** be in the env, otherwise
// choelo will exit with an error.
type Vars struct {
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
	QoS            int
}

// Init initializes the environment
func Init() Vars {
	v := &Vars{
		Exchange:       *exch,
		ExchangeType:   *exchType,
		Durable:        *durable,
		Delete:         *delete,
		Exclusive:      *exclusive,
		NoWait:         *nowait,
		RabbitMqAddres: rabbitMqAddres,
		Send:           *sendMode,
		QoS:            *qos,
	}
	return *v
}
