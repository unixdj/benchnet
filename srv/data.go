package main

import (
	"errors"
	"math/rand"
	"sort"
)

// Geolocation
type geoloc uint64 // for now

// kinda-nullable blob for db access
type blob []byte

// Node
type node struct {
	next       *node   // next
	id         uint64  // id
	lastSeen   uint64  // Time last connected
	capa, used int     // capacity
	loc        geoloc  // location
	key        blob    // Network key
	jobs       jobList // jobs we want on this node, sorted by id
	dirty      bool    // relatively to db
}

type job struct {
	//next  *job     // next
	d     jobDesc  // desc
	capa  int      // capacity of one job instance
	nodes []uint64 // node IDs running the job (len == have, cap == want), unsorted
	dirty bool     // relatively to db
}

type jlist []*job
type nlist []*node

var (
	jobs  jlist // list of jobs (sorted by geo?)
	nodes nlist // list of nodes
)

var errDataType = errors.New("wrong data type")

func (b *blob) ScanInto(value interface{}) error {
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

// index returns index in l where job with given id is or should be.
func (l jobList) index(id int) int {
	return sort.Search(len(l), func(i int) bool { return l[i].Id >= id })
}

// in checks if j is in l.
func (j *job) in(l jobList) bool {
	i := l.index(j.d.Id)
	return i < len(l) && l[i].Id == j.d.Id
}

// runnable checks if job wants to run more times.
func (j *job) runnable() bool {
	return len(j.nodes) < cap(j.nodes)
}

// canRun checks if n wants to run j.
func (n *node) canRun(j *job) bool {
	return !j.in(n.jobs) && j.capa <= n.capa-n.used
}

func crossLink(j *job, n *node) {
	i := n.jobs.index(j.d.Id)
	n.jobs = append(n.jobs[:i], append(jobList{j.d}, n.jobs[i:]...)...)
	j.nodes = append(j.nodes, n.id)
}

// addJob adds j to n's job list.
func (n *node) addJob(j *job) {
	crossLink(j, n)
	n.used += j.capa
	n.dirty = true
	j.dirty = true
}

// addJob tries to add all jobs in list l to n's job list.
func (n *node) addJobs(l []job) {
	for i := range l {
		if (&l[i]).runnable() && n.canRun(&l[i]) {
			n.addJob(&l[i])
		}
	}
}

// perm permutes n pseudo-randomly.
func perm(n []*node) {
	t := make([]*node, len(n))
	p := rand.Perm(len(n))
	for i, v := range p {
		t[i] = n[v] // pun not intended
	}
	copy(n, t)
}

func schedule() {
	if len(nodes) == 0 || len(jobs) == 0 {
		return
	}
	perm(nodes)
	var (
		lastmod = -1   // last modified
		cand    = jobs // runnable jobs
	)
	// i == lastmod means we did the whole loop without scheduling any job.
	// len(cand) == 0 means no jobs left to schedule.
	for {
		for i, n := range nodes {
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
					// TODO: move jobs to another list
					// instead of the above?
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

func loadDB() error {
	if err := loadNodes(); err != nil {
		return err
	}
	return loadJobs()
}
