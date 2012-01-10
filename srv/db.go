package main

import (
	"benchnet/lib/stdb"
	"exp/sql"
	_ "github.com/mattn/go-sqlite3"
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
	dbSelectNode  = "SELECT last, capa, used, loc, key, jobs FROM nodes WHERE id = ?"
	dbSelectNodes = "SELECT id, last, capa, used, loc, key, jobs FROM nodes"
	dbSelectJobs  = "SELECT id, period, start, cmd, capa, nodes FROM jobs"
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
	if nodeHead != nil {
		panic("node list not empty")
	}
	cur := &nodeHead
	for rows.Next() {
		var (
			n    node
			jobs blob
		)
		if err := rows.Scan(&n.id, &n.lastSeen, &n.capa, &n.used,
			&n.loc, &n.key, &jobs); err != nil {
			return err
		}
		*cur = &n
		cur = &n.next
	}
	return nil
}

func loadJobs() error {
	rows, err := dbc.Query(dbSelectJobs)
	if err != nil {
		return err
	}
	defer rows.Close()
	jobs = make([]job, 0, 16)
	for rows.Next() {
		var (
			j job
			n blob
		)
		if err := rows.Scan(&j.d.Id, &j.d.Period, &j.d.Start,
			&j.d.Check, &j.capa, &n); err != nil {
			return err
		}
		// XXX: j.nodes
		if jlen := len(jobs); jlen == cap(jobs) {
			if jlen < 1<<13 { // 8*1024
				jlen <<= 1
			} else {
				jlen += 1 << 13
			}
			jobs = append(make([]job, 0, jlen), jobs...)
		}
	}
	return nil
}
