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
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"github.com/unixdj/benchnet/lib/conn"
	"io"
	"time"
)

var (
	errProto  = errors.New("protocol error")
	errFuture = errors.New("timestamp in the future")
)

type step func(*conn.Conn) (step, error)

func recvGreet(s *conn.Conn) (step, error) {
	buf := make([]byte, len(conn.Greet))
	_, err := io.ReadFull(s, buf)
	if err != nil {
		return nil, err
	}
	if bytes.Compare(buf[:len(conn.Greet)], []byte(conn.Greet)) != 0 {
		return nil, conn.ErrProto
	}
	return auth, s.ReceiveChallenge()
}

func auth(s *conn.Conn) (step, error) {
	s.Reset()
	buf := make([]byte, 16, 16+2*conn.KeySize)
	binary.BigEndian.PutUint64(buf, clientId)
	binary.BigEndian.PutUint64(buf[8:], nodeId)
	buf = s.Sign(buf)
	return sendLogs, s.SendChallenge(buf)
}

func sendLogs(s *conn.Conn) (step, error) {
	var buf [8]byte
	if _, err := io.ReadFull(s, buf[:]); err != nil {
		return nil, err
	}
	then := binary.BigEndian.Uint64(buf[:])
	now := uint64(time.Now().UnixNano())
	if then > now {
		return nil, errFuture
	}
	if err := s.CheckSig(); err != nil {
		return nil, err
	}
	if ra, err := loadResults(then); err != nil {
		return nil, err
	} else {
		log.Debug(fmt.Sprintf("sending %d results", len(ra)))
		if err = gob.NewEncoder(s).Encode(ra); err != nil {
			return nil, err
		}
	}
	if then > now-uint64(time.Hour)*2 {
		then = now - uint64(time.Hour)*2
	}
	deleteResults(then)
	return recvJobs, s.SendSig()
}

func recvJobs(s *conn.Conn) (step, error) {
	var newjobs jobList
	if err := gob.NewDecoder(s).Decode(&newjobs); err != nil {
		return nil, err
	}
	log.Debug(fmt.Sprintf("received %d jobs", len(newjobs)))
	if err := s.CheckSig(); err != nil {
		return nil, err
	}
	mergeJobs(newjobs)
	return sendBye, nil
}

func sendBye(s *conn.Conn) (step, error) {
	if _, err := s.Write([]byte{0}); err != nil {
		return nil, err
	}
	return nil, s.SendSig()
}

func talk() (ok bool) {
	log.Info("connecting to server " + serverAddr + conn.Port)
	s, err := conn.Dial("tcp", "localhost"+conn.Port, networkKey)
	if err != nil {
		log.Err(err.Error())
		return false
	}
	defer s.Close()
	f, err := recvGreet(s)
	for f != nil && err == nil {
		f, err = f(s)
	}
	if err != nil {
		log.Err(err.Error())
		return false
	}
	log.Info("conection completed")
	return true
}
