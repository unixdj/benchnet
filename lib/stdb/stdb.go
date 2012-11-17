// Package stdb is a single threaded database wrapper for exp/sql.
//
// API like exp/sql
//
// After Query(), the thread is locked to the client until either
// (*Rows).Next() returns false or (*Rows).Close() is called.
// So do it fast and close it good.
//
// Same goes for QueryRow() after which one (*Row).Scan() should
// be issued, and Begin(), which should be finished off with
// (*Tx).Commit() or (*Tx).Rollback().
package stdb

import (
	"database/sql"
	"errors"
	"io"
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

type DB struct {
	dbc *sql.DB  // backend
	c   chan req // request channel
}

type Rows struct {
	rows   *sql.Rows
	c      chan req // request channel for query
	closed bool
}

type Row struct {
	row    *sql.Row
	c      chan req // request channel for query
	closed bool
}

type Tx struct {
	tx     *sql.Tx
	c      chan req
	closed bool
}

// response
type res struct {
	result sql.Result // for Exec()
	qctx   *Rows      // query context
	qrctx  *Row       // query row context
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

// query loop: set up sql.Rows and loop until !Next() || Close() || input error
func (db *DB) handleQuery(r *req) {
	rows, err := db.dbc.Query(r.cmd, r.args...)
	if err != nil {
		r.c <- res{err: err}
		return
	}
	qctx := &Rows{rows: rows, c: make(chan req)}
	r.c <- res{qctx: qctx}
	defer qctx.rows.Close()
	for {
		qr := <-qctx.c
		switch qr.op {
		case opScan:
			qr.c <- res{err: qctx.rows.Scan(qr.args...)}
		case opNext:
			if qctx.rows.Next() {
				qr.c <- res{}
			} else {
				// if marking closed, do it before sending
				qctx.closed = true
				qr.c <- res{err: io.EOF}
				return
			}
		case opClose:
			qctx.closed = true
			qr.c <- res{}
			return
		default:
			qctx.closed = true
			qr.c <- res{err: errors.New("invalid db.Rows operation")}
			return
		}
	}
}

// queryRow loop: set up sql.Row and process one Scan()
func (db *DB) handleQueryRow(r *req) {
	row := db.dbc.QueryRow(r.cmd, r.args...)
	qrctx := &Row{row: row, c: make(chan req)}
	r.c <- res{qrctx: qrctx}
	qr := <-qrctx.c
	qrctx.closed = true
	if qr.op == opScan {
		qr.c <- res{err: qrctx.row.Scan(qr.args...)}
	} else {
		qr.c <- res{err: errors.New("invalid db.Row operation")}
	}
}

// tx loop: set up sql.Tx and loop until Commit() || Rollback() || input error
func (db *DB) handleTx(r *req) {
	tx, err := db.dbc.Begin()
	if err != nil {
		r.c <- res{err: err}
		return
	}
	ctx := &Tx{tx: tx, c: make(chan req)}
	r.c <- res{tx: ctx}
	for {
		txr := <-ctx.c
		switch txr.op {
		case opExec:
			result, err := ctx.tx.Exec(txr.cmd, txr.args...)
			txr.c <- res{result: result, err: err}
		case opCommit:
			ctx.closed = true
			txr.c <- res{err: ctx.tx.Commit()}
			return
		case opRollback:
			ctx.closed = true
			txr.c <- res{err: ctx.tx.Rollback()}
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

// Open(): start thread
func Open(driverName, dataSourceName string) (*DB, error) {
	db := &DB{nil, make(chan req)}
	c := make(chan error)
	go db.thread(driverName, dataSourceName, c)
	if err := <-c; err != nil {
		return nil, err
	}
	return db, nil
}

// frontend: communication with thread

func (db *DB) Close() error {
	c := make(chan res)
	db.c <- req{op: opClose, c: c}
	return (<-c).err
}

func (db *DB) Exec(s string, args ...interface{}) (sql.Result, error) {
	c := make(chan res)
	db.c <- req{op: opExec, cmd: s, args: args, c: c}
	r := <-c
	return r.result, r.err
}

// -- Query() / Rows

func (db *DB) Query(s string, args ...interface{}) (*Rows, error) {
	c := make(chan res)
	db.c <- req{op: opQuery, cmd: s, args: args, c: c}
	r := <-c
	return r.qctx, r.err
}

func (qctx *Rows) Next() bool {
	if qctx.closed {
		return false
	}
	c := make(chan res)
	qctx.c <- req{op: opNext, c: c}
	return (<-c).err == nil
}

func (qctx *Rows) Scan(args ...interface{}) error {
	if qctx.closed {
		return io.EOF
	}
	c := make(chan res)
	qctx.c <- req{op: opScan, args: args, c: c}
	return (<-c).err
}

func (qctx *Rows) Close() bool {
	if qctx.closed {
		return false
	}
	c := make(chan res)
	qctx.c <- req{op: opClose, c: c}
	return (<-c).err == nil
}

// -- QueryRow() / Row

func (db *DB) QueryRow(s string, args ...interface{}) *Row {
	c := make(chan res)
	db.c <- req{op: opQueryRow, cmd: s, args: args, c: c}
	return (<-c).qrctx
}

func (qrctx *Row) Scan(args ...interface{}) error {
	if qrctx.closed {
		return io.EOF
	}
	c := make(chan res)
	qrctx.c <- req{op: opScan, args: args, c: c}
	return (<-c).err
}

// -- Begin() / Tx

func (db *DB) Begin() (*Tx, error) {
	c := make(chan res)
	db.c <- req{op: opBegin, c: c}
	r := <-c
	return r.tx, r.err
}

func (tx *Tx) Exec(s string, args ...interface{}) (sql.Result, error) {
	if tx.closed {
		return nil, io.EOF
	}
	c := make(chan res)
	tx.c <- req{op: opExec, cmd: s, args: args, c: c}
	r := <-c
	return r.result, r.err
}

func (tx *Tx) Commit() error {
	if tx.closed {
		return io.EOF
	}
	c := make(chan res)
	tx.c <- req{op: opCommit, c: c}
	return (<-c).err
}

func (tx *Tx) Rollback() error {
	if tx.closed {
		return io.EOF
	}
	c := make(chan res)
	tx.c <- req{op: opRollback, c: c}
	return (<-c).err
}
