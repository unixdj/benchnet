// main
package main

import (
	"errors"
	"fmt"
	"github.com/unixdj/conf"
	"log/syslog"
	"math/rand"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"
)

var (
	log              *syslog.Writer
	conffile         = "bench.conf"
	dbfile           = "bench.db"
	serverAddr       = "localhost"
	clientId, nodeId uint64
	networkKey       []byte
	netKeyRE         = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)
)

type netKeyValue []byte

func (key *netKeyValue) Set(s string) error {
	if !netKeyRE.MatchString(s) {
		return errors.New("invalid key (must be 64 hexadecimal digits)")
	}
	fmt.Sscanf(s, "%x", key) // will succeed
	return nil
}

//func (key *netKeyValue) String() string { return fmt.Sprintf("%x", *key) }

func readConf() error {
	f, err := os.Open(conffile)
	if err != nil {
		return err
	}
	defer f.Close()
	return conf.Parse(f, conffile, []conf.Var{
		{
			Name: "db",
			Val:  (*conf.StringValue)(&dbfile),
		},
		{
			Name:     "clientid",
			Val:      (*conf.Uint64Value)(&clientId),
			Required: true,
		},
		{
			Name:     "nodeid",
			Val:      (*conf.Uint64Value)(&nodeId),
			Required: true,
		},
		{
			Name:     "key",
			Val:      (*netKeyValue)(&networkKey),
			Required: true,
		},
	})
}

const (
	reconnectTime = time.Hour
	reconnectFuzz = time.Minute * 10
	retryTime     = time.Minute * 10
	retryFuzz     = time.Minute * 2
)

func durFuzz(dur time.Duration, fuzz time.Duration) time.Duration {
	return dur - fuzz + time.Duration(rand.Int63n(int64(fuzz)*2))
}

func netLoop(headShot <-chan bool, done chan<- bool) {
	defer func() {
		done <- true
	}()
	rand.Seed(int64(time.Now().UnixNano()))
	var dur time.Duration
	for {
		ok := talk() // connect to server immediately
		if ok {
			dur = durFuzz(reconnectTime, reconnectFuzz)
		} else {
			dur = durFuzz(retryTime, retryFuzz)
		}
		log.Debug(fmt.Sprintf("next connection in %v", dur))
		t := time.NewTimer(dur)
		select {
		case <-headShot:
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func main() {
	var err error
	log, err = syslog.New(syslog.LOG_INFO,
		fmt.Sprintf("benchnet.node[%d]", os.Getpid()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't connect to syslog: %v", err)
		os.Exit(1)
	}
	defer log.Close()

	killme := make(chan os.Signal, 5)
	signal.Notify(killme, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT,
		syscall.SIGPIPE, syscall.SIGTERM)

	if err = readConf(); err != nil {
		log.Crit(err.Error())
		os.Exit(1)
	}

	err = dbOpen()
	if dbc == nil {
		log.Crit("can't open database: " + err.Error())
		os.Exit(1)
	}
	defer dbc.Close()
	if err != nil {
		dbc.Close()
		log.Crit("can't init database: " + err.Error())
		os.Exit(1)
	}

	if err = loadJobs(); err != nil {
		dbc.Close()
		log.Crit("error while loading jobs from database: " + err.Error())
		os.Exit(1)
	}
	defer killJobs()

	killNet := make(chan bool)
	netDone := make(chan bool)
	go netLoop(killNet, netDone)
	defer func() {
		killNet <- true
		<-netDone
	}()

	log.Notice("RUNNING")

	log.Notice("EXIT: " + (<-killme).String())
}
