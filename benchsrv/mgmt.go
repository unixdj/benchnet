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
	"crypto/rand"
	"fmt"
	"github.com/unixdj/smtplike"
	"io"
	"net"
	"regexp" // i'm so lazy
	"strconv"
)

var netKeyRE = regexp.MustCompile(`^[0-9a-fA-F]{64}$`)

func mgmtGreet(args []string, c *smtplike.Conn) (int, string) {
	return smtplike.Hello, "benchnet-management-0 hello"
}

func mgmtAddJob(args []string, c *smtplike.Conn) (int, string) {
	if len(args) < 6 {
		return 501, "invalid syntax"
	}
	var (
		j   job
		tmp int64
		err error
	)
	if j.Id, err = strconv.ParseUint(args[0], 0, 64); err != nil {
		return 501, args[0] + ": " + err.Error()
	}
	if tmp, err = strconv.ParseInt(args[1], 0, 32); err != nil {
		return 501, args[1] + ": " + err.Error()
	}
	j.Period = int(tmp)
	if tmp, err = strconv.ParseInt(args[2], 0, 32); err != nil {
		return 501, args[2] + ": " + err.Error()
	}
	j.Start = int(tmp)
	if tmp, err = strconv.ParseInt(args[3], 0, 32); err != nil {
		return 501, args[3] + ": " + err.Error()
	}
	j.capa = int(tmp)
	if tmp, err = strconv.ParseInt(args[4], 0, 32); err != nil {
		return 501, args[4] + ": " + err.Error()
	}
	j.nodes = make([]uint64, 0, int(tmp))
	j.Check = args[5:]
	if jp := getJob(j.Id); jp != nil {
		return 550, "job already exists"
	}
	addJob(&j)
	return 200, "ok"
}

func mgmtRmJob(args []string, c *smtplike.Conn) (int, string) {
	if len(args) != 1 {
		return 501, "invalid syntax"
	}
	id, err := strconv.ParseUint(args[0], 0, 64)
	if err != nil {
		return 501, args[0] + ": " + err.Error()
	}
	if j := getJob(id); j != nil {
		rmJob(j)
	} else {
		return 550, "job does not exist"
	}
	return 200, "ok"
}

func mgmtAddNode(args []string, c *smtplike.Conn) (int, string) {
	if len(args) < 3 || len(args) > 4 {
		return 501, "invalid syntax"
	}
	var (
		n   node
		err error
	)
	if n.id, err = strconv.ParseUint(args[0], 0, 64); err != nil {
		return 501, args[0] + ": " + err.(*strconv.NumError).Err.Error()
	}
	if tmp, err := strconv.ParseInt(args[1], 0, 32); err != nil {
		return 501, args[1] + ": " + err.Error()
	} else {
		n.capa = int(tmp)
	}
	if tmp, err := strconv.ParseUint(args[2], 0, 64); err != nil {
		return 501, args[2] + ": " + err.Error()
	} else {
		n.loc = geoloc(tmp)
	}
	n.key = make([]byte, 32)
	if len(args) == 4 {
		l, err := io.ReadFull(rand.Reader, n.key)
		if l != len(n.key) || err != nil {
			return 501, "rand: " + err.Error()
		}
	} else {
		if !netKeyRE.MatchString(args[3]) {
			return 501, args[3] + ": must be 64 hexadecimal digits"
		}
		fmt.Sscanf(args[3], "%x", n.key)
	}
	if np := getJob(n.id); np != nil {
		return 550, "node already exists"
	}
	addNode(&n)
	return 200, "ok"
}

func mgmtRmNode(args []string, c *smtplike.Conn) (int, string) {
	if len(args) != 1 {
		return 501, "invalid syntax"
	}
	id, err := strconv.ParseUint(args[0], 0, 64)
	if err != nil {
		return 501, args[0] + ": " + err.Error()
	}
	if n := getNode(id); n != nil {
		rmNode(n)
	} else {
		return 550, "node does not exist"
	}
	return 200, "ok"
}

func mgmtList(args []string, c *smtplike.Conn) (int, string) {
	if len(args) != 0 {
		return 501, "invalid syntax"
	}
	s := nodes.String() + jobs.String()
	if len(s) >= 2 {
		s = s[:len(s)-2]
	}
	return 210, s
}

func mgmtSched(args []string, c *smtplike.Conn) (code int, msg string) {
	if len(args) != 0 {
		return 501, "invalid syntax"
	}
	requestSchedule()
	return 210, "ok"
}

func mgmtCommit(args []string, c *smtplike.Conn) (code int, msg string) {
	if len(args) != 0 {
		return 501, "invalid syntax"
	}
	requestCommit()
	return 210, "ok"
}

func mgmtHelp(args []string, c *smtplike.Conn) (code int, msg string) {
	if len(args) != 0 {
		return 501, "invalid syntax"
	}
	return 214, `commands:
commit
    commit changes to database
h|help
    help
job <id> <period> <start> <capacity> <times> <check>...
    add job
list
    list nodes and jobs
node <id> <capacity> <geoloc> [<key>]
    add node
quit
    quit
rmjob <id>
    remove job
rmnode <id>
    remove node
sched
    run scheduler and commit changes to database`
}

func mgmtQuit(args []string, c *smtplike.Conn) (code int, msg string) {
	if len(args) != 0 {
		return 501, "invalid syntax"
	}
	return smtplike.Goodbye, "bye"
}

var mgmt = smtplike.Proto{
	{"", mgmtGreet},
	{"h", mgmtHelp},
	{"help", mgmtHelp},
	{"job", mgmtAddJob},
	{"list", mgmtList},
	{"node", mgmtAddNode},
	{"rmjob", mgmtRmJob},
	{"rmnode", mgmtRmNode},
	{"sched", mgmtSched},
	{"commit", mgmtCommit},
	{"quit", mgmtQuit},
}

func mgmtHandle(c net.Conn) {
	if err := mgmt.Run(c, nil); err != nil {
		log.Err("management connection terminated: " + err.Error())
		return
	}
	log.Notice("management connection completed")
}
