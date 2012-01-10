package main

import (
	"benchnet/lib/check"
	"benchnet/lib/conn"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"net"
)

type jobDesc struct {
	Id            int
	Period, Start int
	Check         []string
}

type jobList []jobDesc

/*
var jobs = jobList{
	{0, 3, 3, []string{"dns", "not.found"}},
	{1, 7, 3, []string{"dns", "foo.bar"}},
	{3, 20, 10, []string{"http", "get", "http://foo.bar/"}},
}
*/

const clientLastTime = 1324105000000000000 // or so
var (
	dbfile    = "srv.db"
	clientKey = []byte("" +
		"\x00\x01\x02\x03\x04\x05\x06\x07" +
		"\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f" +
		"\x10\x11\x12\x13\x14\x15\x16\x17" +
		"\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f")
)

type step func(*conn.Conn, *node) (step, error)

func sendGreet(s *conn.Conn, n *node) (step, error) {
	greets := make([]byte, len(conn.Greet))
	copy(greets, conn.Greet)
	return authClient, s.SendChallenge(greets)
}

func authClient(s *conn.Conn, n *node) (step, error) {
	buf := make([]byte, 16)
	_, err := io.ReadFull(s, buf)
	if err != nil {
		return nil, err
	}
	clientId := binary.BigEndian.Uint64(buf)
	nodeId := binary.BigEndian.Uint64(buf[8:])
	if clientId != nodeId {
		panic("test")
	}
	log.Printf("node id %d\n", nodeId)
	*n, err = selectNode(nodeId)
	log.Printf("node key %x\n", n.key)
	if err != nil {
		return nil, err
	}
	s.SetKey(n.key)
	s.WriteToHash(buf)
	if err = s.CheckSig(); err != nil {
		return nil, err
	}
	log.Printf("sig ok\n")
	return recvLogs, s.ReceiveChallenge()
}

func recvLogs(s *conn.Conn, n *node) (step, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n.lastSeen)
	if _, err := s.Write(buf[:]); err != nil {
		return nil, err
	}
	if err := s.SendSig(); err != nil {
		return nil, err
	}
	var results []check.Result
	if err := gob.NewDecoder(s).Decode(&results); err != nil {
		return nil, err
	}
	log.Printf("%v\n", results)
	return nil, nil
}

func handle(c net.Conn) {
	s, err := conn.New(c)
	if err != nil {
		c.Close()
		log.Printf("handle: %v\n", err)
		return
	}
	defer s.Close()
	log.Printf("handle\n")
	var n node
	f, err := sendGreet(s, &n)
	for f != nil && err == nil {
		log.Printf("step\n")
		f, err = f(s, &n)
	}
	if err != nil {
		log.Printf("handle: %v\n", err)
	} else {
		log.Printf("ok\n")
	}
}

func main() {
	err := dbOpen()
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	if err = loadDB(); err != nil {
		log.Printf("%v\n", err)
		return
	}
	l, err := net.Listen("tcp", conn.Port)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer l.Close()
	for {
		c, err := l.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				continue
			}
			return
		}
		go handle(c)
	}
}
