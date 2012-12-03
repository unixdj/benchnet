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

package main

import (
	"fmt"
	"github.com/unixdj/benchnet/lib/conn"
	"log/syslog"
	"net"
	"os"
	"os/signal"
	"syscall"
)

var log *syslog.Writer
var dying bool

func netLoop(l net.Listener, handler func(net.Conn), name string) {
	for {
		c, err := l.Accept()
		if err != nil {
			if dying {
				log.Debug(name + " loop killed")
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Notice("accept: " + ne.Error())
				continue
			}
			log.Notice("accept: " + err.Error())
			break
		}
		log.Info(fmt.Sprintf("accept %s connection from %s",
			name, c.RemoteAddr()))
		go handler(c)
	}
}

func main() {
	var err error
	log, err = syslog.New(syslog.LOG_DAEMON,
		fmt.Sprintf("benchnet.server[%d]", os.Getpid()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't connect to syslog: %v\n", err)
		os.Exit(1)
	}
	defer log.Close()

	killme := make(chan os.Signal, 5)
	signal.Notify(killme, syscall.SIGHUP, syscall.SIGINT, syscall.SIGQUIT,
		syscall.SIGPIPE, syscall.SIGTERM)

	initDone := make(chan error)
	killData := make(chan bool, 1) // async
	dataDone := make(chan bool)
	go dataLoop(initDone, killData, dataDone)
	// wait for data loop to initialize
	if err := <-initDone; err != nil {
		log.Err(err.Error())
		return
	}
	defer func() {
		killData <- true
		<-dataDone
	}()

	l, err := net.Listen("tcp", conn.Port)
	if err != nil {
		log.Err("FATAL: " + err.Error())
		return
	}
	defer l.Close()
	go netLoop(l, handle, "client")

	m, err := net.Listen("tcp", "127.0.0.1:25197") // "bm" for benchmgmt
	if err != nil {
		log.Err("FATAL: " + err.Error())
		return
	}
	defer m.Close()
	go netLoop(m, mgmtHandle, "management")

	log.Info("RUNNING")

	log.Info("EXIT: " + (<-killme).String())
	dying = true
}
