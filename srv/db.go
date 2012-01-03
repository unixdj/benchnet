package main

import (
	"benchnet/lib/conn" // for conn.Node
	//"benchnet/lib/stdb"
	"exp/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

// sqlite> create table nodes (id integer primary key, last integer, key blob[32]);

const (
	dbSelectNode = "SELECT key FROM nodes WHERE id = ?"
)

var dbc *sql.DB

func dbOpen() error {
	var err error
	dbc, err = sql.Open("sqlite3", dbfile)
	if err != nil {
		return err
	}
	return err
}

func selectNode(id uint64) (*conn.Node, error) {
	n := &conn.Node{NodeId: id}
	// Can't use QueryRow():
	// http://code.google.com/p/go/issues/detail?id=2622
	rows, err := dbc.Query(dbSelectNode, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, sql.ErrNoRows
	}
	var key []byte
	if err = rows.Scan(&key); err != nil {
		return nil, err
	}
	n.Key = append([]byte{}, key...)
	return n, err
}
