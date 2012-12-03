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
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/unixdj/benchnet/lib/stdb"
	"sort"
	"strings"
)

/*
database schema:

table nodes:
	id	node id
	last	time when node connected last, nanoseconds since Unix epoch
	capa	total capacity of jobs the node is prepared to run
	loc	geolocation
	key	network key

table jobs:
	id	job id
	period	period in seconds
	start	offset in seconds; jobs run at Unix time N*period+start
	capa	capacity (exact meaning TBD)
	want	number of desired copies
	cmd	the check to run (space-separated string)

table running:
	job	job id
	node	node id

table results:
	node	 id of node that ran the job
	job	 id of job that generated the result
	start	 time when the run started, nanoseconds since Unix epoch
	duration overall time for this run, in nanoseconds
	flags	 1 for error, mostly
	result	 encoded ("%+q") string array of results
*/
const (
	dbfile        = "benchsrv.db"
	dbCreateNodes = `CREATE TABLE IF NOT EXISTS nodes
		(id integer primary key, last integer, capa integer,
		loc integer, key blob[32])`
	dbCreateJobs = `CREATE TABLE IF NOT EXISTS jobs
		(id integer primary key, period integer, start integer,
		capa integer, want integer, cmd string)`
	dbCreateRunning = `CREATE TABLE IF NOT EXISTS running
		(job integer, node integer)`
	dbCreateResults = `CREATE TABLE IF NOT EXISTS results
		(node integer, job integer, start integer, duration integer,
		flags integer, err text, result text)`
	dbSelectNodes   = "SELECT id, last, capa, loc, key FROM nodes"
	dbInsertNode    = "INSERT OR REPLACE INTO nodes (id, last, capa, loc, key) VALUES (?, ?, ?, ?, ?)"
	dbDeleteNode    = "DELETE FROM nodes WHERE id=?"
	dbSelectJobs    = "SELECT id, period, start, capa, want, cmd FROM jobs"
	dbInsertJob     = "INSERT OR REPLACE INTO jobs (id, period, start, capa, want, cmd) VALUES (?, ?, ?, ?, ?, ?)"
	dbDeleteJob     = "DELETE FROM jobs WHERE id=?"
	dbSelectRunning = "SELECT job, node FROM running"
	dbInsertRunning = "INSERT OR REPLACE INTO running (job, node) VALUES (?, ?)"
	dbDeleteRunning = "DELETE FROM running WHERE job=? AND node=?"
	dbInsertResult  = "INSERT OR REPLACE INTO results (node, job, start, duration, flags, err, result) VALUES (?, ?, ?, ?, ?, ?, ?)"
)

type (
	jobNotFoundError  uint64
	nodeNotFoundError uint64
)

var dbc *stdb.DB

func (e jobNotFoundError) Error() string {
	return fmt.Sprintf("job %d not found", e)
}

func (e nodeNotFoundError) Error() string {
	return fmt.Sprintf("node %d not found", e)
}

func dbOpen() error {
	var err error
	dbc, err = stdb.Open("sqlite3", dbfile)
	if err != nil {
		return err
	}
	for _, v := range []string{
		dbCreateJobs,
		dbCreateNodes,
		dbCreateRunning,
		dbCreateResults,
	} {
		if _, err = dbc.Exec(v); err != nil {
			return err
		}
	}
	return nil
}

func dbLoad() error {
	for _, f := range []func() error{loadNodes, loadJobs, loadRunning} {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}

func dbClose() error {
	if dbc != nil {
		return dbc.Close()
	}
	return nil
}

func loadNodes() error {
	rows, err := dbc.Query(dbSelectNodes)
	if err != nil {
		return err
	}
	defer rows.Close()
	nodes = make([]*node, 0, 16)
	for rows.Next() {
		var (
			n node
		)
		if err := rows.Scan(&n.id, &n.lastSeen, &n.capa, &n.loc, &n.key); err != nil {
			return err
		}
		if nlen := len(nodes); nlen == cap(nodes) {
			if nlen < 1<<13 { // 8*1024
				nlen <<= 1
			} else {
				nlen += 1 << 13
			}
			nodes = append(make([]*node, 0, nlen), nodes...)
		}
		nodes = append(nodes, &n)
	}
	sort.Sort(nodes)
	return nil
}

func loadJobs() error {
	rows, err := dbc.Query(dbSelectJobs)
	if err != nil {
		return err
	}
	defer rows.Close()
	jobs = make([]*job, 0, 16)
	for rows.Next() {
		var (
			j    job
			want int
			s    string
		)
		if err := rows.Scan(&j.Id, &j.Period, &j.Start, &j.capa,
			&want, &s); err != nil {
			return err
		}
		j.Check = strings.Fields(s)
		j.nodes = make([]uint64, 0, want)
		jobs = append(jobs, &j)
	}
	sort.Sort(jobs)
	return nil
}

func loadRunning() error {
	rows, err := dbc.Query(dbSelectRunning)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var jid, nid uint64
		if err := rows.Scan(&jid, &nid); err != nil {
			return err
		}
		j := jobs.find(jid)
		if j == nil {
			return jobNotFoundError(jid)
		}
		n := nodes.find(nid)
		if n == nil {
			return nodeNotFoundError(nid)
		}
		n.doAddJob(j)
	}
	return nil
}

func dbCommit(diffs difflist, results reslist, done chan<- bool) {
	log.Debug("commit starting")
	defer func() {
		log.Debug("commit done")
		done <- true
	}()
	tx, err := dbc.Begin()
	if err != nil {
		log.Notice("sql.Begin: " + err.Error())
		return
	}
	for _, v := range diffs {
		switch v.op {
		case opAddLink:
			_, err = tx.Exec(dbInsertRunning, v.jobId, v.nodeId)
		case opRmLink:
			_, err = tx.Exec(dbDeleteRunning, v.jobId, v.nodeId)
		case opAddNode:
			_, err = tx.Exec(dbInsertNode, v.n.id, v.n.lastSeen,
				v.n.capa, v.n.loc, []byte(v.n.key))
		case opRmNode:
			_, err = tx.Exec(dbDeleteNode, v.nodeId)
		case opAddJob:
			_, err = tx.Exec(dbInsertJob, v.j.Id, v.j.Period,
				v.j.Start, v.j.capa, cap(v.j.nodes),
				strings.Join(v.j.Check, " "))
		case opRmJob:
			_, err = tx.Exec(dbDeleteJob, v.jobId)
		default:
			log.Warning(fmt.Sprintf("interal error: invalid database operation %d", v.op))
		}
		if err != nil {
			log.Notice("sql.Exec: %v" + err.Error())
			if err = tx.Rollback(); err != nil {
				log.Notice("sql.Rollback: " + err.Error())
			}
			return
		}
	}
	for _, v := range results {
		_, err := tx.Exec(dbInsertResult, v.nodeId, v.JobId, v.Start,
			v.RT, v.Flags, v.Errs, fmt.Sprintf("%+q", v.S))
		if err != nil {
			log.Notice("sql.Exec: " + err.Error())
			if err = tx.Rollback(); err != nil {
				log.Notice("sql.Rollback: " + err.Error())
			}
			return
		}
	}
	if err = tx.Commit(); err != nil {
		log.Notice("sql.Commit: " + err.Error())
	}
}
