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
	"benchnet/lib/stdb"
	"benchnet/node/check"
	"errors"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"strconv"
	"strings"
)

var dbc *stdb.DB

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
	dbSelectResults    = "SELECT id, start, duration, flags, err, result FROM results WHERE start >= ?"
	dbDeleteResults    = "DELETE FROM results WHERE start < ?"
	dbDeleteJobResults = "DELETE FROM results WHERE id = ?"
)

func dbOpen() error {
	var err error
	dbc, err = stdb.Open("sqlite3", dbfile)
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
			/*
				if _, err := tx.Exec(dbDeleteJobResults, v.Id); err != nil {
					return err
				}
			*/
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
			log.Err(fmt.Sprintf("invalid job %d: %v",
				j.Id, j.Check))
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

// Parse quoted strings of the form:
//	["one" "two\r\n\xcc" "three"]
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
		if end == 0 {
			return nil, errSyntax
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

func loadResults(from uint64) ([]*check.Result, error) {
	//log.Debug(fmt.Sprintf("loadResults: from %d to %d", from, till))
	rows, err := dbc.Query(dbSelectResults, from)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ra := make([]*check.Result, 0, 16)
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
		ra = append(ra, r)
	}
	//log.Debug(fmt.Sprintf("loadResults: %d", len(ra)))
	return ra, nil
}

func deleteResults(till uint64) error {
	_, err := dbc.Exec(dbDeleteResults, till)
	return err
}
