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

/*
Package stdb is a single threaded database wrapper around
database/sql.

The underlying sql.DB is accessed via a single goroutine that
gets requests and sends responses via channels, thus serializing
database access.

The API is like that of database/sql, except that some parts are
missing.

After Begin() is called, the connection is locked onto the
returned *Tx until either (*Tx).Commit() or (Tx).Rollback()
is called.  After Query() the connection locked to *Rows until
either (*Rows).Next() returns false or (*Rows).Close() is called.
QueryRow() locks to *Row until (*Row).Scan() is issued.

Justification for this exercise can be found at:
	https://gist.github.com/4184712
*/
package stdb

import (
	"database/sql"
	"errors"
)

// operations
const (
	opClose = iota
	opExec
	opQuery
	opQueryRow
	opScan
	opNext
	opBegin
	opCommit
	opRollback
)

// DB is a database handle.
type DB struct {
	dbc *sql.DB  // backend
	c   chan req // request channel
}

// Rows is the result of calling QueryRows.
type Rows struct {
	c      chan req // request channel for query
	closed bool
}

// Row is the result of calling QueryRow to select a single row.
type Row struct {
	c      chan req // request channel for query
	closed bool
}

// Tx represents a database transaction.
type Tx struct {
	c      chan req
	closed bool
}

// response
type res struct {
	result sql.Result // for Exec()
	rs     *Rows      // query context
	rw     *Row       // query row context
	tx     *Tx        // transaction
	err    error
}

// request
type req struct {
	op   int           // const above (opSomethingOrOther)
	cmd  string        // SQL command
	args []interface{} // arguments for Exec(), Scan(), Query() etc.
	c    chan res      // channel for response
}

// thread

// Query loop: set up sql.Rows and loop until !Next() || Close() || input error
func (db *DB) handleQuery(r *req) {
	rows, err := db.dbc.Query(r.cmd, r.args...)
	if err != nil {
		r.c <- res{err: err}
		return
	}
	rs := &Rows{c: make(chan req)}
	r.c <- res{rs: rs}
	defer rows.Close()
	for {
		qr := <-rs.c
		switch qr.op {
		case opScan:
			qr.c <- res{err: rows.Scan(qr.args...)}
		case opNext:
			if rows.Next() {
				qr.c <- res{}
			} else {
				// if marking closed, do it before sending
				rs.closed = true
				qr.c <- res{err: sql.ErrNoRows} // arbitrary
				return
			}
		case opClose:
			rs.closed = true
			qr.c <- res{}
			return
		default:
			rs.closed = true
			qr.c <- res{err: errors.New("invalid db.Rows operation")}
			return
		}
	}
}

// QueryRow loop: set up sql.Row and process one Scan()
func (db *DB) handleQueryRow(r *req) {
	row := db.dbc.QueryRow(r.cmd, r.args...)
	rw := &Row{c: make(chan req)}
	r.c <- res{rw: rw}
	qr := <-rw.c
	rw.closed = true
	if qr.op == opScan {
		qr.c <- res{err: row.Scan(qr.args...)}
	} else {
		qr.c <- res{err: errors.New("invalid db.Row operation")}
	}
}

// Tx loop: set up sql.Tx and loop until Commit() || Rollback() || input error
func (db *DB) handleTx(r *req) {
	tx, err := db.dbc.Begin()
	if err != nil {
		r.c <- res{err: err}
		return
	}
	ctx := &Tx{c: make(chan req)}
	r.c <- res{tx: ctx}
	for {
		txr := <-ctx.c
		switch txr.op {
		case opExec:
			result, err := tx.Exec(txr.cmd, txr.args...)
			txr.c <- res{result: result, err: err}
		case opCommit:
			ctx.closed = true
			txr.c <- res{err: tx.Commit()}
			return
		case opRollback:
			ctx.closed = true
			txr.c <- res{err: tx.Rollback()}
			return
		case opQuery:
			db.handleQuery(&txr)
		default:
			ctx.closed = true
			txr.c <- res{err: errors.New("invalid db.Tx operation")}
			return
		}
	}
}

// main loop
func (db *DB) thread(driverName, dataSourceName string, c chan<- error) {
	var err error
	db.dbc, err = sql.Open(driverName, dataSourceName)
	c <- err
	close(c)
	if err != nil {
		return
	}
	for {
		r := <-db.c
		switch r.op {
		case opClose:
			r.c <- res{err: db.dbc.Close()}
			return
		case opExec:
			result, err := db.dbc.Exec(r.cmd, r.args...)
			r.c <- res{result: result, err: err}
		case opQuery:
			db.handleQuery(&r)
		case opQueryRow:
			db.handleQueryRow(&r)
		case opBegin:
			db.handleTx(&r)
		default:
			r.c <- res{err: errors.New("invalid db operation")}
		}
	}
}

// -- front end

// Open opens a database connection and starts the worker goroutine.
func Open(driverName, dataSourceName string) (*DB, error) {
	db := &DB{nil, make(chan req)}
	c := make(chan error)
	go db.thread(driverName, dataSourceName, c)
	if err := <-c; err != nil {
		return nil, err
	}
	return db, nil
}

// Close closes the database connection and terminates the
// worker goroutine.
func (db *DB) Close() error {
	c := make(chan res)
	db.c <- req{op: opClose, c: c}
	return (<-c).err
}

// Exec executes a query that returns no rows.
func (db *DB) Exec(s string, args ...interface{}) (sql.Result, error) {
	c := make(chan res)
	db.c <- req{op: opExec, cmd: s, args: args, c: c}
	r := <-c
	return r.result, r.err
}

// -- Query() / Rows

// Query executes a query that returns rows.
func (db *DB) Query(s string, args ...interface{}) (*Rows, error) {
	c := make(chan res)
	db.c <- req{op: opQuery, cmd: s, args: args, c: c}
	r := <-c
	return r.rs, r.err
}

func (rs *Rows) Next() bool {
	if rs.closed {
		return false
	}
	c := make(chan res)
	rs.c <- req{op: opNext, c: c}
	return (<-c).err == nil
}

func (rs *Rows) Scan(args ...interface{}) error {
	if rs.closed {
		return errors.New("sql: Rows closed")
	}
	c := make(chan res)
	rs.c <- req{op: opScan, args: args, c: c}
	return (<-c).err
}

func (rs *Rows) Close() error {
	if rs.closed {
		return nil
	}
	c := make(chan res)
	rs.c <- req{op: opClose, c: c}
	return (<-c).err
}

// -- QueryRow() / Row

// QueryRow executes a query that returns one row.
func (db *DB) QueryRow(s string, args ...interface{}) *Row {
	c := make(chan res)
	db.c <- req{op: opQueryRow, cmd: s, args: args, c: c}
	return (<-c).rw
}

func (rw *Row) Scan(args ...interface{}) error {
	if rw.closed {
		return errors.New("sql: Row closed")
	}
	c := make(chan res)
	rw.c <- req{op: opScan, args: args, c: c}
	return (<-c).err
}

// -- Begin() / Tx

// Begin starts a transaction.
func (db *DB) Begin() (*Tx, error) {
	c := make(chan res)
	db.c <- req{op: opBegin, c: c}
	r := <-c
	return r.tx, r.err
}

func (tx *Tx) Exec(s string, args ...interface{}) (sql.Result, error) {
	if tx.closed {
		return nil, sql.ErrTxDone
	}
	c := make(chan res)
	tx.c <- req{op: opExec, cmd: s, args: args, c: c}
	r := <-c
	return r.result, r.err
}

func (tx *Tx) Commit() error {
	if tx.closed {
		return sql.ErrTxDone
	}
	c := make(chan res)
	tx.c <- req{op: opCommit, c: c}
	return (<-c).err
}

func (tx *Tx) Rollback() error {
	if tx.closed {
		return sql.ErrTxDone
	}
	c := make(chan res)
	tx.c <- req{op: opRollback, c: c}
	return (<-c).err
}
