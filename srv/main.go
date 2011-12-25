package main

import (
	"benchnet/lib/conn"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"io"
	"log"
	"net"
)

const (
	keySize = sha256.Size
)

type Result struct {
	JobId int
	Flags int
	Errs  string
	Start int64
	RT    int64
	S     []string
}

type jobDesc struct {
	Id            int
	Period, Start int
	Check         []string
}

type jobList []jobDesc

var jobs = jobList{
	{0, 3, 3, []string{"dns", "not.found"}},
	{1, 7, 3, []string{"dns", "foo.bar"}},
	{3, 20, 10, []string{"http", "get", "http://foo.bar/"}},
}

const clientLastTime = 1324105000000000000 // or so
var (
	clientKey = []byte("" +
		"\x00\x01\x02\x03\x04\x05\x06\x07" +
		"\x08\x09\x0a\x0b\x0c\x0d\x0e\x0f" +
		"\x10\x11\x12\x13\x14\x15\x16\x17" +
		"\x18\x19\x1a\x1b\x1c\x1d\x1e\x1f")
)

func decodeUint64(buf []byte) uint64 {
	return uint64(buf[0])<<56 | uint64(buf[1])<<48 |
		uint64(buf[2])<<40 | uint64(buf[3])<<32 |
		uint64(buf[4])<<24 | uint64(buf[5])<<16 |
		uint64(buf[6])<<8 | uint64(buf[7])
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
	greets := make([]byte, len(conn.Greet))
	copy(greets, conn.Greet)
	if err = s.SendChallenge(greets); err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	buf := make([]byte, 16)
	_, err = io.ReadFull(s, buf)
	if err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	clientId := binary.BigEndian.Uint64(buf)
	nodeId := binary.BigEndian.Uint64(buf[8:])
	if clientId != nodeId {
		panic("test")
	}
	s.SetKey(clientKey)
	s.WriteToHash(buf)
	err = s.CheckSig()
	if err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	err = s.ReceiveChallenge()
	if err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	buf = buf[:8]
	binary.BigEndian.PutUint64(buf, clientLastTime)
	if _, err = s.Write(buf); err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	if err = s.SendSig(); err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	enc := gob.NewEncoder(s)
	if err = enc.Encode(&jobs); err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	if err = s.SendSig(); err != nil {
		log.Printf("handle: %v\n", err)
		return
	}
	log.Printf("ok\n")
}

func main() {
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
