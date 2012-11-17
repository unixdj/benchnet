package main

import (
	"benchnet/lib/conn"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log/syslog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var log *syslog.Writer
var dying bool

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
	var buf [1]byte
	_, err := io.ReadFull(c, buf[:])
	if err != nil {
		return nil, err
	}
	if buf[0] != 0 {
		return nil, conn.ErrProto
	}
	return nil, c.CheckSig()
}

func handle(nc net.Conn) {
	client := "client " + nc.RemoteAddr().String()
	cc, err := conn.New(nc)
	if err != nil {
		nc.Close()
		log.Err(client + ": handle: " + err.Error())
		return
	}
	defer cc.Close()
	var d connData
	f, err := sendGreet(cc, &d)
	for f != nil && err == nil {
		f, err = f(cc, &d)
	}
	if err != nil {
		log.Err(client + ": handle: " + err.Error())
		return
	}
	log.Notice(client + ": connection completed")
	nodeSeen(d.n)
	addResults(d.r)
}

func (n *node) String() string {
	s := fmt.Sprintf("Node %v\nlastSeen %v\n"+
		"capacity %v, used %v\ngeolocation %v\nkey %x\njobs:",
		n.id, time.Unix(0, int64(n.lastSeen)),
		n.capa, n.used, n.loc, n.key)
	for _, j := range n.jobs {
		s += fmt.Sprintf(" %v", j.Id)
	}
	return s + "\n\n"
}

func (j *job) String() string {
	return fmt.Sprintf("Job %v\nperiod %vs, start %v\ncapacity %v\n"+
		"check %+q\nnodes %v (%v/%v)\n\n",
		j.Id, j.Period, j.Start, j.capa,
		j.Check, j.nodes, len(j.nodes), cap(j.nodes))
}

func (n nlist) String() string {
	var s string
	for _, v := range n {
		s += v.String()
	}
	return s
}

func (j jlist) String() string {
	var s string
	for _, v := range j {
		s += v.String()
	}
	return s
}

func netLoop(l net.Listener, handler func(net.Conn), name string) {
	for {
		c, err := l.Accept()
		if err != nil {
			if dying {
				log.Debug(name + " loop killed")
				return
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Warning("accept: " + ne.Error())
				continue
			}
			log.Err("accept: " + err.Error())
			break
		}
		log.Notice(fmt.Sprintf("accept %s connection from %s",
			name, c.RemoteAddr()))
		go handler(c)
	}
}

func main() {
	var err error
	log, err = syslog.New(syslog.LOG_INFO,
		fmt.Sprintf("benchnet[%d]", os.Getpid()))
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
		log.Crit(err.Error())
		return
	}
	defer func() {
		killData <- true
		<-dataDone
	}()
	//log.Debug(fmt.Sprintf("%v\n%v\n", nodes, jobs))
	l, err := net.Listen("tcp", conn.Port)
	if err != nil {
		log.Crit("FATAL: " + err.Error())
		return
	}
	defer l.Close()
	go netLoop(l, handle, "client")
	m, err := net.Listen("tcp", "127.0.0.1:25197") // "bm" for benchmgmt
	if err != nil {
		log.Crit("FATAL: " + err.Error())
		return
	}
	defer m.Close()
	go netLoop(m, mgmtHandle, "management")
	log.Notice("RUNNING")
	log.Notice("EXIT: " + (<-killme).String())
	dying = true
}
