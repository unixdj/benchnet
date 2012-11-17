// main
package main

import (
	"errors"
	"fmt"
	"github.com/unixdj/conf"
	"log/syslog"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"
)

var (
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

/*
// crude parser for now
func interact() error {
	in := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("bench:node> ")
		s, err := in.ReadString('\n')
		if err != nil {
			return err
		}
		f := strings.Fields(s)
		if len(f) == 0 {
			continue
		}
		switch f[0] {
		case "q":
			return nil
		case "c":
			if err := talk(); err != nil {
				fmt.Printf("%s\n", err)
			}
		case "l":
			for _, v := range jobs {
				var status string
				if v.s == nil {
					status = " (not running)"
				}
				fmt.Printf("id %d, every %ds, check %s%s\n",
					v.Id, v.Period, v.Check, status)
			}
		case "k":
			if len(f) != 2 {
				fmt.Printf("usage: k <jobid>\n")
				continue
			}
			n, err := strconv.ParseInt(f[1], 10, 0)
			if err != nil {
				fmt.Printf("%s not a number: %s\n", f[1], err)
				continue
			}
			if !killJob(uint64(n)) {
				fmt.Printf("no such job: %d\n", int(n))
			}
			if err = deleteJob(int(n)); err != nil {
				fmt.Printf("Exec: %s\n", err)
			}
		case "a":
			if len(f) < 4 {
				fmt.Printf(`usage: a <jobid> <period> <check>
e.g.:
a 0 60 dns foo.bar
a 1 86400 http get http://foo.bar/
`)
				continue
			}
			id, err := strconv.ParseUint(f[1], 10, 0)
			if err != nil {
				fmt.Printf("%s not a positive number: %s\n", s[1], err)
				continue
			}
			p, err := strconv.ParseInt(f[2], 10, 0)
			if err != nil || p < 3 || p > 86400 {
				fmt.Printf("%s should be a number between 3 and 86400\n", f[2])
				continue
			}
			j := jobDesc{
				Id:     uint64(id),
				Period: int(p),
				Start:  int(p) / 2,
				Check:  f[3:],
			}
			if !addJob(&j, true) {
				fmt.Printf("invalid job %d: %v\n", j.Id, j.Check)
				continue
			}
			if err = insertJob(&j); err != nil {
				fmt.Printf("Exec: %s\n", err)
			}
			fmt.Printf("id %d started\n", j.Id)
		default:
			fmt.Printf("say what?\n")
		}
	}
	// NOTREACHED
	return nil
}
*/

var log *syslog.Writer

func openLog() error {
	var err error
	log, err = syslog.New(syslog.LOG_INFO,
		fmt.Sprintf("benchnet.node[%d]", os.Getpid()))
	return err
}

func closeLog() {
	log.Close()
}

func netLoop() {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		talk() // connect to server immediately
		<-t.C
	}
}

func main() {
	err := openLog()
	if err != nil {
		fmt.Fprintf(os.Stderr, "can't connect to syslog: %v", err)
		os.Exit(1)
	}
	defer closeLog()
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
	go netLoop()
	log.Notice("RUNNING")
	log.Notice("EXIT: " + (<-killme).String())
}
