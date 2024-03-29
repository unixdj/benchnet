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

/*
	File proto.go implements the node-server protocol.
*/

package main

import (
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/unixdj/benchnet/lib/conn"
	"io"
	"net"
	"time"
)

type (
	connData struct {
		n *node
		r []result
	}
	step func(*conn.Conn, *connData) (step, error)
)

func sendGreet(c *conn.Conn, d *connData) (step, error) {
	greets := make([]byte, len(conn.Greet))
	copy(greets, conn.Greet)
	return authClient, c.SendChallenge(greets)
}

func authClient(c *conn.Conn, d *connData) (step, error) {
	var buf [16]byte
	_, err := io.ReadFull(c, buf[:])
	if err != nil {
		return nil, err
	}
	//clientId := binary.BigEndian.Uint64(buf[:8])
	id := binary.BigEndian.Uint64(buf[8:])
	d.n = getNode(id)
	if d.n == nil {
		return nil, nodeNotFoundError(id)
	}
	c.SetKey(d.n.key)
	c.WriteToHash(buf[:])
	if err = c.CheckSig(); err != nil {
		return nil, err
	}
	log.Info(fmt.Sprintf("client %s: authenticated node %d",
		c.RemoteAddr(), id))
	return recvLogs, c.ReceiveChallenge()
}

func recvLogs(c *conn.Conn, d *connData) (step, error) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], d.n.lastSeen)
	_, err := c.Write(buf[:])
	if err != nil {
		return nil, err
	}
	if err = c.SendSig(); err != nil {
		return nil, err
	}
	d.n.lastSeen = uint64(time.Now().UnixNano())
	if err = gob.NewDecoder(c).Decode(&d.r); err != nil {
		return nil, err
	}
	for i := range d.r {
		d.r[i].nodeId = d.n.id
	}
	return sendJobs, c.CheckSig()
}

func sendJobs(c *conn.Conn, d *connData) (step, error) {
	if err := gob.NewEncoder(c).Encode(d.n.jobs); err != nil {
		return nil, err
	}
	return recvBye, c.SendSig()
}

func recvBye(c *conn.Conn, d *connData) (step, error) {
	b, err := c.ReadByte()
	if err != nil {
		return nil, err
	}
	if b != 0 {
		return nil, conn.ErrProto
	}
	return nil, c.CheckSig()
}

func handle(nc net.Conn) {
	client := "client " + nc.RemoteAddr().String()
	cc, err := conn.New(nc)
	if err != nil {
		nc.Close()
		log.Notice(client + ": handle: " + err.Error())
		return
	}
	defer cc.Close()
	var d connData
	f, err := sendGreet(cc, &d)
	for f != nil && err == nil {
		f, err = f(cc, &d)
	}
	if err != nil {
		log.Notice(client + ": handle: " + err.Error())
		return
	}
	log.Info(client + ": connection completed")
	nodeSeen(d.n)
	addResults(d.r)
	requestCommit()
}
