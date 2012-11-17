package main

import (
	"benchnet/lib/stdb"
	//"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"sort"
	"strings"
)

/*
sqlite> .schema
CREATE TABLE jobs (id integer primary key,
		period integer, start integer, capa integer, want integer,
		cmd string);
CREATE TABLE nodes (id integer primary key,
		last integer, capa integer, loc integer,
		key blob[32]);
CREATE TABLE running
		(job integer, node integer);
CREATE TABLE results
		(node integer, job integer, start integer, duration integer,
		flags integer, err text, result text);
*/

const (
	dbfile          = "srv.db"
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
	return err
}

func dbClose() error {
	if dbc != nil {
		return dbc.Close()
	}
	return nil
}

/*
func selectNode(id uint64) (n node, err error) {
	n = node{id: id}
	// Can't use QueryRow():
	// http://code.google.com/p/go/issues/detail?id=2622
	rows, err := dbc.Query(dbSelectNode, id)
	if err != nil {
		return
	}
	defer rows.Close()
	if !rows.Next() {
		return n, sql.ErrNoRows
	}
	var key []byte
	if err = rows.Scan(&key); err != nil {
		return
	}
	n.key = append([]byte{}, key...)
	return n, err
}
*/

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

func doCommit(diffs difflist, results reslist, done chan<- bool) {
	log.Debug("commit starting")
	defer func() {
		log.Debug("commit done")
		done <- true
	}()
	tx, err := dbc.Begin()
	if err != nil {
		log.Err(fmt.Sprintf("sql.Begin:", err))
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
			log.Crit(fmt.Sprintf("interal error: invalid database operation %d", v.op))
		}
		if err != nil {
			log.Err(fmt.Sprintf("sql.Exec: %v", err))
			if err = tx.Rollback(); err != nil {
				log.Err(fmt.Sprintf("sql.Rollback:", err))
			}
			return
		}
	}
	for _, v := range results {
		_, err := tx.Exec(dbInsertResult, v.nodeId, v.JobId, v.Start,
			v.RT, v.Flags, v.Errs, fmt.Sprintf("%+q", v.S))
		if err != nil {
			log.Err(fmt.Sprintf("sql.Exec: %v", err))
			if err = tx.Rollback(); err != nil {
				log.Err(fmt.Sprintf("sql.Rollback:", err))
			}
			return
		}
	}
	if err = tx.Commit(); err != nil {
		log.Err(fmt.Sprintf("sql.Commit:", err))
	}
}
