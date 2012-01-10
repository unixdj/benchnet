package main

import (
	"benchnet/lib/conn"
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	errProto  = errors.New("protocol error")
	errFuture = errors.New("timestamp in the future")
)

var client = conn.Node{0, 0, 0,
	[]byte("\x00\x01\x02\x03\x04\x05\x06\x07" +
		"\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f" +
		"\x10\x11\x12\x13\x14\x15\x16\x17" +
		"\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f"),
}

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
	binary.BigEndian.PutUint64(buf, client.ClientId)
	binary.BigEndian.PutUint64(buf[8:], client.NodeId)
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
	if ra, err := loadResults(then, now); err != nil {
		return nil, err
	} else {
		fmt.Printf("sending %d results\n", len(ra))
		if err = gob.NewEncoder(s).Encode(ra); err != nil {
			return nil, err
		}
	}
	return recvJobs, s.SendSig()
}

func recvJobs(s *conn.Conn) (step, error) {
	var newjobs jobList
	if err := gob.NewDecoder(s).Decode(&newjobs); err != nil {
		return nil, err
	}
	fmt.Printf("newjobs: %v\n", newjobs)
	mergeJobs(newjobs) // XXX: before CheckSig()!!!
	return nil, s.CheckSig()
}

func talk() error {
	s, err := conn.Dial("tcp", "localhost"+conn.Port, client.Key)
	if err != nil {
		return err
	}
	defer s.Close()
	f, err := recvGreet(s)
	for f != nil && err == nil {
		f, err = f(s)
	}
	return err
}
