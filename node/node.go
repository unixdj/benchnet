// main
package main

import (
	"benchnet/lib/conf"
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const conffile = "bench.conf"

var (
	dbfile           = "bench.db"
	clientId, nodeId uint64
	networkKey       []byte
)

var connKeyRE = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

type connKeyValue []byte

func (key *connKeyValue) Set(s string) error {
	if !connKeyRE.MatchString(s) {
		return errors.New("invalid key (must be 64 hexadecimal digits)")
	}
	fmt.Sscanf(s, "%x", key) // will succeed
	return nil
}

func readConf() error {
	f, err := os.Open(conffile)
	if err != nil {
		return err
	}
	defer f.Close()
	err = conf.Parse(f, conffile, []conf.Var{
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
			Val:      (*connKeyValue)(&networkKey),
			Required: true,
		},
	})
	if err != nil {
		return err
	}
	fmt.Printf("%x", networkKey) // will succeed
	return nil
}

// crude parser for now
func interact() error {
	in := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("bench:node> ")
		b, ispref, err := in.ReadLine()
		if err != nil {
			return err
		}
		if ispref {
			fmt.Printf("A line longer than 4K, srsly?  Get out.\n")
			for ispref {
				_, ispref, err = in.ReadLine()
				if err != nil {
					return err
				}
			}
			continue
		}
		s := strings.Fields(string(b))
		if len(s) == 0 {
			continue
		}
		switch s[0] {
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
			if len(s) != 2 {
				fmt.Printf("usage: k <jobid>\n")
				continue
			}
			n, err := strconv.ParseInt(s[1], 10, 0)
			if err != nil {
				fmt.Printf("%s not a number: %s\n", s[1], err)
				continue
			}
			if !killJob(int(n)) {
				fmt.Printf("no such job: %d\n", int(n))
			}
			if err = deleteJob(int(n)); err != nil {
				fmt.Printf("Exec: %s\n", err)
			}
		case "a":
			if len(s) < 4 {
				fmt.Printf(`usage: a <jobid> <period> <check>
e.g.:
a 0 60 dns foo.bar
a 1 86400 http get http://foo.bar/
`)
				continue
			}
			id, err := strconv.ParseUint(s[1], 10, 0)
			if err != nil {
				fmt.Printf("%s not a positive number: %s\n", s[1], err)
				continue
			}
			p, err := strconv.ParseInt(s[2], 10, 0)
			if err != nil || p < 3 || p > 86400 {
				fmt.Printf("%s should be a number between 3 and 86400\n", s[2])
				continue
			}
			j := jobDesc{
				Id:     int(id),
				Period: int(p),
				Start:  int(p) / 2,
				Check:  s[3:],
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

func main() {
	err := readConf()
	if err != nil {
		fmt.Printf("%s\n", err)
	}
	err = dbOpen()
	if dbc == nil {
		fmt.Printf("can't open database: %s\n", err)
		return
	}
	defer dbc.Close()
	if err != nil {
		fmt.Printf("can't init database: %s\n", err)
		return
	}
	if err = loadJobs(); err != nil {
		fmt.Printf("error while loading jobs from database: %s\n", err)
	}
	defer killJobs()
	if err = interact(); err != nil {
		fmt.Printf("%s\n", err)
	}
}
