package main

import (
	"../lib/conf"
	"./check"
	"./db"
	"bufio"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const conffile = "bench.conf"

var (
	dbfile           = "bench.db"
	dbc              *db.DB
	clientId, nodeId uint64
	networkKey       []byte
)

// database stuff

// database schema:
// table jobs:
//     id     job id
//     period period in seconds
//     start  offset in seconds; jobs run at Unix time N*period+start
//     cmd    the check to run (space-separated string)
// table results:
//     id       job id that generated the result
//     start    time when the run started, nanoseconds since Unix epoch
//     duration overall time for this run, in nanoseconds
//     flags    see constants below
//     err      error, if any
//     result   encoded ("%+q") string array of results
const (
	// SHOUT SQL IN CAPITAL LETTERS SO THE DATABASE WILL HEAR YA!!!
	dbCreate1          = "CREATE TABLE IF NOT EXISTS jobs (id INTEGER PRIMARY KEY, period INTEGER, start INTEGER, cmd TEXT)"
	dbCreate2          = "CREATE TABLE IF NOT EXISTS results (id INTEGER, start INTEGER, duration INTEGER, flags INTEGER, err TEXT, result TEXT)"
	dbInsertJob        = "INSERT OR REPLACE INTO jobs (id, period, start, cmd) VALUES (?, ?, ?, ?)"
	dbSelectJobs       = "SELECT id, period, start, cmd FROM jobs"
	dbDeleteJob        = "DELETE FROM jobs WHERE id = ?"
	dbInsertResult     = "INSERT OR REPLACE INTO results (id, start, duration, flags, err, result) VALUES (?, ?, ?, ?, ?, ?)"
	dbSelectResults    = "SELECT id, start, duration, flags, err, result FROM results WHERE start BETWEEN ? AND ?"
	dbDeleteJobResults = "DELETE FROM results WHERE id = ?"
)

func dbOpen() error {
	var err error
	dbc, err = db.Open("sqlite3", dbfile)
	if err != nil {
		return err
	}
	for _, v := range []string{dbCreate1, dbCreate2} {
		if _, err = dbc.Exec(v); err != nil {
			return err
		}
	}
	return nil
}

func insertJob(j *jobDesc) error {
	_, err := dbc.Exec(dbInsertJob, j.Id, j.Period, j.Start, strings.Join(j.Check, " "))
	return err
}

func deleteJob(id int) error {
	_, err := dbc.Exec(dbDeleteJob, id)
	return err
}

func replaceJobs(oldjobs, newjobs jobList) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // nop if committed
	// Maybe we should prepare statements instead of Exec()ing?
	// Delete running jobs
	for _, v := range oldjobs {
		if v.s != nil {
			if _, err := tx.Exec(dbDeleteJob, v.Id); err != nil {
				return err
			}
			if _, err := tx.Exec(dbDeleteJobResults, v.Id); err != nil {
				return err
			}
		}
	}
	// Insert idle jobs
	for _, v := range newjobs {
		if v.s == nil {
			_, err = tx.Exec(dbInsertJob, v.Id, v.Period, v.Start,
				strings.Join(v.Check, " "))
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// loads jobs from db and schedules them
func loadJobs() error {
	rows, err := dbc.Query(dbSelectJobs)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var j jobDesc
		var s string
		if err := rows.Scan(&j.Id, &j.Period, &j.Start, &s); err != nil {
			return err
		}
		j.Check = strings.Fields(s)
		if !addJob(&j, false) {
			fmt.Printf("invalid job %d: %v\n", j.Id, j.Check)
			continue
		}
	}
	startJobs()
	return nil
}

func insertResult(r *check.Result) error {
	_, err := dbc.Exec(dbInsertResult, r.JobId, r.Start, r.RT, r.Flags,
		r.Errs, fmt.Sprintf("%+q", r.S))
	return err
}

var errSyntax = errors.New("syntax error")

// Parse quoted strings of form: ["one" "two\r\n\xcc" "three"]
func parseStringArray(s string) ([]string, error) {
	a := make([]string, 0, 4)
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, errSyntax
	}
	s = s[1 : len(s)-1]
	for len(s) > 0 {
		if s[0] != '"' {
			return nil, errSyntax
		}
		var (
			end    int
			escape bool
			err    error
		)
		for i, r := range s[1:] {
			if escape {
				escape = false
				continue
			}
			if r == '\\' {
				escape = true
				continue
			}
			if r == '"' {
				end = i + 2
				break
			}
		}
		t := s[:end]
		if end != len(s) {
			if s[end] != ' ' {
				return nil, errSyntax
			}
			end++
		}
		s = s[end:]
		if t, err = strconv.Unquote(t); err != nil {
			return nil, err
		}
		a = append(a, t)
	}
	return a, nil
}

func loadResults(from, till uint64) ([]*check.Result, error) {
	fmt.Printf("loadResults: from %d to %d\n", from, till)
	rows, err := dbc.Query(dbSelectResults, from, till)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ra := make([]*check.Result, 0, 16) // how much?
	for rows.Next() {
		var s string
		r := &check.Result{}
		err = rows.Scan(&r.JobId, &r.Start, &r.RT, &r.Flags, &r.Errs, &s)
		if err != nil {
			return nil, err
		}
		if r.S, err = parseStringArray(s); err != nil {
			return nil, err
		}
		if oldl := len(ra); oldl == cap(ra) {
			newl := oldl
			if newl < 8192 {
				newl <<= 1
			} else {
				newl += 8192
			}
			newra := make([]*check.Result, oldl, newl)
			copy(newra, ra)
			ra = newra
		}
		ra = append(ra, r)
	}
	fmt.Printf("loadResults: %d\n", len(ra))
	return ra, nil
}

func readConf() error {
	f, err := os.Open(conffile)
	if err != nil {
		return err
	}
	defer f.Close()
	var key string
	err = conf.Parse(f, conffile, []conf.Var{
		{
			Name: "db",
			Sval: &dbfile,
		},
		{
			Name:     "clientid",
			Typ:      conf.Unsigned,
			Required: true,
			Unval:    &clientId,
		},
		{
			Name:     "nodeid",
			Typ:      conf.Unsigned,
			Required: true,
			Unval:    &nodeId,
		},
		{
			Name:     "key",
			RE:       regexp.MustCompile(`^[0-9a-fA-F]{64}$`),
			Required: true,
			Sval:     &key,
		},
	})
	if err != nil {
		return err
	}
	fmt.Sscanf(key, "%x", &networkKey) // will succeed
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
