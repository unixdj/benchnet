// Benchnet
//
// Copyright 2012 Vadim Vygonets
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	conffile         = "benchnode.conf"
	dbfile           = "benchnode.db"
	serverAddr       = "klaipeda.startunit.com"
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
			Name: "server",
			Val:  (*conf.StringValue)(&serverAddr),
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

func netLoop(headShot <-chan bool) {
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
		select {
		case <-headShot:
			log.Debug("net loop done")
			return
		case <-time.After(dur):
		}
	}
}

func main() {
	var err error
	log, err = syslog.New(syslog.LOG_DAEMON,
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
		log.Err(err.Error())
		os.Exit(1)
	}

	err = dbOpen()
	if dbc == nil {
		log.Err("can't open database: " + err.Error())
		os.Exit(1)
	}
	defer dbc.Close()
	if err != nil {
		dbc.Close()
		log.Err("can't init database: " + err.Error())
		os.Exit(1)
	}

	if err = loadJobs(); err != nil {
		dbc.Close()
		log.Err("error while loading jobs from database: " + err.Error())
		os.Exit(1)
	}
	killNet := make(chan bool)
	go netLoop(killNet)
	defer func() {
		netDone := make(chan bool)
		go func() {
			killNet <- true
			netDone <- true
		}()
		killJobs()
		<-netDone
	}()

	log.Info("RUNNING")

	log.Info("EXIT: " + (<-killme).String())
}
