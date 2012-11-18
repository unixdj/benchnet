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
	This file deals with data structures and access.

	To ensure consistency, all data access should happen via
	interface functions at the end of file.  Operations are
	dispatched in the dataLoop() function which runs in a
	dedicated coroutine.
*/

package main

import (
	"benchnet/lib/check"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type (
	geoloc uint64 // Geolocation
	blob   []byte // kinda-nullable blob for db access

	// job description for node array
	jobDesc struct {
		Id            uint64
		Period, Start int
		Check         []string
	}

	jobList []jobDesc

	// job
	job struct {
		jobDesc          // desc
		capa    int      // capacity of one job instance
		nodes   []uint64 // node IDs running the job (len == have, cap == want), unsorted
	}

	// Node
	node struct {
		id         uint64  // id
		lastSeen   uint64  // Time last connected
		capa, used int     // capacity
		loc        geoloc  // location
		key        blob    // Network key
		jobs       jobList // jobs we want on this node, sorted by id
	}

	// Result
	result struct {
		check.Result
		nodeId uint64
	}

	jobRequest struct {
		id uint64
		c  chan *job
	}

	nodeRequest struct {
		id uint64
		c  chan *node
	}
)

var (
	jobReqChan    = make(chan jobRequest, 5)  // async
	nodeReqChan   = make(chan nodeRequest, 5) // async
	schedReqChan  = make(chan bool, 2)        // async
	commitReqChan = make(chan bool, 2)        // async
)

const (
	opAddLink = iota
	opRmLink
	opAddNode
	opRmNode
	opAddJob
	opRmJob
	opNodeSeen
	opAddResults
)

type opRequest struct {
	op int
	j  *job
	n  *node
	r  []result
}

var opChan = make(chan opRequest) // synchronous

type dataDiff struct {
	op     int
	jobId  uint64 // opAddLink, opRmLink, opRmJob
	nodeId uint64 // opAddLink, opRmLink, opRmNode
	j      *job   // opAddJob
	n      *node  // opAddNode
}

// On sufficiently large data sets binary search becomes slow
// due to cache misses.  The data types below may have to be
// changed to maps.
// http://www.pvk.ca/Blog/2012/07/30/binary-search-is-a-pathological-case-for-caches/
type (
	jlist    []*job
	nlist    []*node
	difflist []dataDiff
	reslist  []result
)

var (
	jobs    jlist    // list of jobs (sorted by geo?)
	nodes   nlist    // list of nodes
	diffs   difflist // list of operations to perform on db
	results reslist  // list of results to commit to db
)

var errDataType = errors.New("wrong data type")

func (b *blob) Scan(value interface{}) error {
	switch v := value.(type) {
	case []byte:
		*b = append(make([]byte, 0, len(v)), v...)
	case nil:
		*b = []byte{}
	default:
		return errDataType
	}
	return nil
}

func (n *node) String() string {
	s := fmt.Sprintf("Node %v\nlastSeen %v\n"+
		"capacity %v, used %v\ngeolocation %v\nkey %x\njobs:",
		n.id, time.Unix(0, int64(n.lastSeen)),
		n.capa, n.used, n.loc, n.key)
	for _, j := range n.jobs {
		s += fmt.Sprintf(" %v", j.Id)
	}
	return s + "\n\n"
}

func (j *job) String() string {
	return fmt.Sprintf("Job %v\nperiod %vs, start %v\ncapacity %v\n"+
		"check %+q\nnodes %v (%v/%v)\n\n",
		j.Id, j.Period, j.Start, j.capa,
		j.Check, j.nodes, len(j.nodes), cap(j.nodes))
}

func (n nlist) String() string {
	var s string
	for _, v := range n {
		s += v.String()
	}
	return s
}

func (j jlist) String() string {
	var s string
	for _, v := range j {
		s += v.String()
	}
	return s
}

func (l jlist) Len() int           { return len(l) }
func (l jlist) Less(i, j int) bool { return l[i].Id < l[j].Id }
func (l jlist) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// index returns index in l where job with given id is or should be.
func (l jlist) index(id uint64) int {
	return sort.Search(len(l), func(i int) bool { return l[i].Id >= id })
}

// find retrieves a job from l by id.
func (l jlist) find(id uint64) *job {
	i := l.index(id)
	if i == len(l) || l[i].Id != id {
		return nil
	}
	return l[i]
}

func (l nlist) Len() int           { return len(l) }
func (l nlist) Less(i, j int) bool { return l[i].id < l[j].id }
func (l nlist) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// index returns index in l where node with given id is or should be.
func (l nlist) index(id uint64) int {
	return sort.Search(len(l), func(i int) bool { return l[i].id >= id })
}

// find retrieves a node from l by id.
func (l nlist) find(id uint64) *node {
	i := l.index(id)
	if i == len(l) || l[i].id != id {
		return nil
	}
	return l[i]
}

// index returns index in l where job with given id is or should be.
func (l jobList) index(id uint64) int {
	return sort.Search(len(l), func(i int) bool { return l[i].Id >= id })
}

// in checks if j is in l.
func (j *job) in(l jobList) bool {
	i := l.index(j.Id)
	return i < len(l) && l[i].Id == j.Id
}

// runnable checks if j wants to run more times.
func (j *job) runnable() bool {
	return len(j.nodes) < cap(j.nodes)
}

// canRun checks if n wants to run j.
func (n *node) canRun(j *job) bool {
	return j.capa <= n.capa-n.used && !j.in(n.jobs)
}

// doAddNode adds n to nodes.
func doAddNode(n *node) {
	i := nodes.index(n.id)
	if i < len(nodes) && nodes[i].id == n.id {
		nodes[i] = n
	} else {
		nodes = append(nodes[:i], append(nlist{n}, nodes[i:]...)...)
	}
}

// doRmNode removes n from nodes.
func doRmNode(n *node) {
	i := nodes.index(n.id)
	if i < len(nodes) && nodes[i].id == n.id {
		nodes = append(nodes[:i], nodes[i+1:]...)
	}
}

// doAddJob adds j to jobs.
func doAddJob(j *job) {
	i := jobs.index(j.Id)
	if i < len(jobs) && jobs[i].Id == j.Id {
		jobs[i] = j
	} else {
		jobs = append(jobs[:i], append(jlist{j}, jobs[i:]...)...)
	}
}

// doRmJob removes j from jobs.
func doRmJob(j *job) {
	i := jobs.index(j.Id)
	if i < len(jobs) && jobs[i].Id == j.Id {
		jobs = append(jobs[:i], jobs[i+1:]...)
	}
}

// doAddJob adds j to n's job list without recording the change.
// For use while loading the database.
func (n *node) doAddJob(j *job) {
	i := n.jobs.index(j.Id)
	n.jobs = append(n.jobs[:i], append(jobList{j.jobDesc}, n.jobs[i:]...)...)
	j.nodes = append(j.nodes, n.id)
	n.used += j.capa
}

// doRmJob removes j from n's job list without recording the change.
func (n *node) doRmJob(j *job) {
	n = nodes.find(n.id)
	j = jobs.find(j.Id)
	if n == nil || j == nil {
		return
	}
	i := n.jobs.index(j.Id)
	n.jobs = append(n.jobs[:i], n.jobs[i+1:]...)
	for i, v := range j.nodes {
		if v == n.id {
			j.nodes = append(j.nodes[:i], j.nodes[i+1:]...)
			break
		}
	}
	n.used -= j.capa
}

// doOp performs an operation and adds a record to dataDiff list.
func doOp(r opRequest) {
	switch r.op {
	case opAddLink, opRmLink:
		if r.op == opAddLink {
			r.n.doAddJob(r.j)
		} else {
			r.n.doRmJob(r.j)
		}
		var (
			l    = dataDiff{op: r.op, jobId: r.j.Id, nodeId: r.n.id}
			notl = dataDiff{op: r.op ^ 1, jobId: r.j.Id, nodeId: r.n.id}
		)
		for i, v := range diffs {
			switch v {
			case notl:
				diffs = append(diffs[:i], diffs[i+1:]...)
				return
			case l:
				return // XXX panic?
			}
		}
		diffs = append(diffs, l)
	case opNodeSeen:
		tmp := nodes.find(r.n.id)
		if tmp == nil {
			return
		}
		tmp.lastSeen = r.n.lastSeen
		r.n = tmp
		fallthrough
	case opAddNode:
		if r.op == opAddNode { // opNodeSeen modifies node in place
			doAddNode(r.n)
		}
		for i, v := range diffs {
			switch {
			case v.op == opAddNode && v.n.id == r.n.id:
				diffs[i].n = copyNode(r.n)
				return
			case v.op == opRmNode && v.nodeId == r.n.id:
				diffs = append(diffs[:i], diffs[i+1:]...)
			}
		}
		diffs = append(diffs, dataDiff{op: opAddNode, n: copyNode(r.n)})
	case opRmNode:
		tmpn := nodes.find(r.n.id)
		for _, v := range tmpn.jobs {
			doOp(opRequest{
				op: opRmLink,
				j:  &job{jobDesc: v},
				n:  tmpn,
			})
		}
		doRmNode(r.n)
		for i, v := range diffs {
			switch {
			case v.op == opRmNode && v.nodeId == r.n.id:
				return
			case v.op == opAddNode && v.n.id == r.n.id:
				diffs = append(diffs[:i], diffs[i+1:]...)
				return
			}
		}
		diffs = append(diffs, dataDiff{op: r.op, nodeId: r.n.id})
	case opAddJob:
		doAddJob(r.j)
		for i, v := range diffs {
			switch {
			case v.op == opAddJob && v.j.Id == r.j.Id:
				diffs[i].j = copyJob(r.j)
				return
			case v.op == opRmJob && v.jobId == r.j.Id:
				diffs = append(diffs[:i], diffs[i+1:]...)
			}
		}
		diffs = append(diffs, dataDiff{op: r.op, j: copyJob(r.j)})
	case opRmJob:
		tmpj := jobs.find(r.j.Id)
		for _, v := range tmpj.nodes {
			doOp(opRequest{
				op: opRmLink,
				j:  tmpj,
				n:  &node{id: v},
			})
		}
		doRmJob(r.j)
		for i, v := range diffs {
			switch {
			case v.op == opRmJob && v.nodeId == r.j.Id:
				return
			case v.op == opAddJob && v.j.Id == r.j.Id:
				diffs = append(diffs[:i], diffs[i+1:]...)
				return
			}
		}
		diffs = append(diffs, dataDiff{op: r.op, jobId: r.j.Id})
	case opAddResults:
		results = append(results, r.r...)
	}
}

// addJob adds j to n's job list.
func (n *node) addJob(j *job) {
	doOp(opRequest{op: opAddLink, j: j, n: n})
}

// perm returns a pseudo-randomly permuted copy of n.
func perm(n nlist) nlist {
	t := make([]*node, len(n))
	p := rand.Perm(len(n))
	for i, v := range p {
		t[i] = n[v]
	}
	return t
}

// schedule attempts to schedule uscheduled jobs.
// TODO: make scheduler concurrent.
func schedule() {
	if len(nodes) == 0 || len(jobs) == 0 {
		return
	}
	var (
		lastmod = -1   // last modified
		cand    = jobs // runnable jobs
	)
	// XXX: sort jobs?
	for !cand[0].runnable() {
		cand = cand[1:]
		if len(cand) == 0 {
			return
		}
	}
	log.Debug("scheduler starting")
	defer log.Debug("scheduler done")
	tmpnodes := perm(nodes)
	// i == lastmod means we did the whole loop without scheduling any job.
	// len(cand) == 0 means no jobs left to schedule.
	for {
		for i, n := range tmpnodes {
			if i == lastmod {
				return
			}
			// add one job to n
			for _, v := range cand {
				if v.runnable() && n.canRun(v) {
					n.addJob(v)
					lastmod = i
					for !cand[0].runnable() {
						cand = cand[1:]
						if len(cand) == 0 {
							return
						}
					}
					break
				}
			}
		}
		// catch the case when no job is added after first loop
		if lastmod == -1 {
			return
		}
	}
}

// copyJob makes a deep copy of job jp.
func copyJob(jp *job) *job {
	j := *jp
	j.nodes = make([]uint64, len(jp.nodes), cap(jp.nodes))
	copy(j.nodes, jp.nodes)
	return &j
}

// doGetJob retrieves a deep copy of job specified by id.
// You probably want to call getJob() instead.
func doGetJob(id uint64) *job {
	jp := jobs.find(id)
	if jp == nil {
		return nil
	}
	return copyJob(jp)
}

// copyNode makes a deep copy of node np.
func copyNode(np *node) *node {
	n := *np
	n.jobs = make(jobList, len(np.jobs))
	copy(n.jobs, np.jobs)
	return &n
}

// doGetNode retrieves a deep copy of node specified by id.
// You probably want to call getNode() instead.
func doGetNode(id uint64) *node {
	np := nodes.find(id)
	if np == nil {
		return nil
	}
	return copyNode(np)
}

func dataInit() error {
	err := dbOpen()
	if dbc == nil {
		return errors.New("can't open database: " + err.Error())
	}
	if err != nil {
		dbc.Close()
		return errors.New("can't init database: " + err.Error())
	}
	if err = dbLoad(); err != nil {
		dbClose()
		return errors.New("can't load database: " + err.Error())
	}
	log.Debug("database loaded")
	return nil
}

func commit(done chan<- bool) {
	if len(diffs) == 0 && len(results) == 0 {
		done <- false
		return
	}
	d, r := diffs, results
	if len(diffs) != 0 {
		diffs = make(difflist, 0, 16)
	}
	if len(results) != 0 {
		results = make(reslist, 0, 16)
	}
	go dbCommit(d, r, done)
}

func dataLoop(initDone chan<- error, headShot <-chan bool, done chan<- bool) {
	defer func() {
		log.Debug("data loop done")
		done <- true
	}()
	err := dataInit()
	initDone <- err
	close(initDone)
	if err != nil {
		return
	}
	var (
		committing bool
		commitDone = make(chan bool, 2)
		t          = time.NewTicker(10 * time.Minute)
	)
	defer func() {
		if err := recover(); err != nil {
			log.Crit(fmt.Sprintf("data loop: panic %v", err))
		}
		t.Stop()
		if committing {
			<-commitDone
			committing = false
		}
		// final commit
		commit(commitDone)
		<-commitDone
		dbClose()
	}()
	schedReqChan <- true
	for {
		select {
		case <-headShot:
			log.Debug("data loop: headshot")
			return
		case <-t.C:
			requestSchedule()
		case <-schedReqChan:
			schedule()
			requestCommit()
		case <-commitReqChan:
			if !committing {
				commit(commitDone)
				committing = true
			}
		case r := <-commitDone:
			if !committing {
				log.Crit("dataLoop(): commit done while not committing")
			} else if r {
				log.Debug("data loop: commit done")
			} else {
				log.Debug("data loop: nothing to commit")
			}
			committing = false
		case r := <-jobReqChan:
			log.Debug("data loop: job request")
			r.c <- doGetJob(r.id)
		case r := <-nodeReqChan:
			log.Debug("data loop: node request")
			r.c <- doGetNode(r.id)
		case r := <-opChan:
			log.Debug(fmt.Sprintf("data loop: add op %d", r.op))
			doOp(r)
		}
	}
}

// getJob fetches a deep copy of the job specified by id.
func getJob(id uint64) *job {
	c := make(chan *job)
	jobReqChan <- jobRequest{id, c}
	return <-c
}

// getNode fetches a deep copy of the node specified by id.
func getNode(id uint64) *node {
	c := make(chan *node)
	nodeReqChan <- nodeRequest{id, c}
	return <-c
}

func addLink(j *job, n *node) { opChan <- opRequest{op: opAddLink, j: j, n: n} }
func rmLink(j *job, n *node)  { opChan <- opRequest{op: opRmLink, j: j, n: n} }
func addNode(n *node)         { opChan <- opRequest{op: opAddNode, n: n} }
func rmNode(n *node)          { opChan <- opRequest{op: opRmNode, n: n} }
func addJob(j *job)           { opChan <- opRequest{op: opAddJob, j: j} }
func rmJob(j *job)            { opChan <- opRequest{op: opRmJob, j: j} }
func nodeSeen(n *node)        { opChan <- opRequest{op: opNodeSeen, n: n} }
func addResults(r []result)   { opChan <- opRequest{op: opAddResults, r: r} }

func requestSchedule() {
	if len(schedReqChan) == 0 {
		schedReqChan <- true
	}
}

func requestCommit() {
	if len(commitReqChan) == 0 {
		commitReqChan <- true
	}
}
