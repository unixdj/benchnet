package main

import (
	"benchnet/lib/stdb"
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"strings"
)

/*
sqlite>
create table nodes (id integer primary key, last integer, capa integer,
	used integer, loc integer, key blob[32], jobs blob);
insert or replace into nodes (id, last, capa, used, loc, key, jobs)
	values (0, 1324105000000000000, 10, 5, 0
-- XXX How to organize job <-> node links?
*/

const (
	dbSelectNode    = "SELECT last, capa, used, loc, key, jobs FROM nodes WHERE id = ?"
	dbSelectNodes   = "SELECT id, last, capa, used, loc, key, jobs FROM nodes"
	dbSelectJobs    = "SELECT id, period, start, capa, want, cmd, nodes FROM jobs"
	dbSelectRunning = "SELECT id, period, start, capa, want, cmd, nodes FROM jobs"
)

var dbc *stdb.DB

func dbOpen() error {
	var err error
	dbc, err = stdb.Open("sqlite3", dbfile)
	if err != nil {
		return err
	}
	return err
}

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

func loadNodes() error {
	rows, err := dbc.Query(dbSelectNodes)
	if err != nil {
		return err
	}
	defer rows.Close()
	if len(nodes) != 0 {
		panic("node list not empty")
	}
	nodes = make([]*node, 0, 16)
	for rows.Next() {
		var (
			n    node
			jobs blob
		)
		if err := rows.Scan(&n.id, &n.lastSeen, &n.capa, &n.used,
			&n.loc, &n.key, &jobs); err != nil {
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
			n    blob
		)
		if err := rows.Scan(&j.d.Id, &j.d.Period, &j.d.Start,
			&j.capa, &want, &s, &n); err != nil {
			return err
		}
		j.d.Check = strings.Fields(s)
		j.nodes = make([]uint64, 0, want)
		// XXX: j.nodes
		if jlen := len(jobs); jlen == cap(jobs) {
			if jlen < 1<<13 { // 8*1024
				jlen <<= 1
			} else {
				jlen += 1 << 13
			}
			jobs = append(make([]*job, 0, jlen), jobs...)
		}
		jobs = append(jobs, &j)
	}
	return nil
}

/*
func loadRunning() error {
	rows, err := dbc.Query(dbSelectRunning)
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
			n    blob
		)
		if err := rows.Scan(&j.d.Id, &j.d.Period, &j.d.Start,
			&j.capa, &want, &s, &n); err != nil {
			return err
		}
		j.d.Check = strings.Fields(s)
		j.nodes = make([]uint64, 0, want)
		// XXX: j.nodes
		if jlen := len(jobs); jlen == cap(jobs) {
			if jlen < 1<<13 { // 8*1024
				jlen <<= 1
			} else {
				jlen += 1 << 13
			}
			jobs = append(make([]*job, 0, jlen), jobs...)
		}
		jobs = append(jobs, &j)
	}
	return nil
}
*/
