package main

import (
	"./check"
	"./sched"
	"fmt"
	"sort"
	"time"
)

type jobDesc struct {
	Id            int
	Period, Start int
	Check         []string
	s             *sched.Sched
}

type jobList []jobDesc

var jobs jobList

// util

func int2dur(i int) time.Duration {
	return time.Duration(i) * time.Second
}

// list management as per sort.Interface

func (l jobList) Len() int           { return len(l) }
func (l jobList) Less(i, j int) bool { return l[i].Id < l[j].Id }
func (l jobList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }

// job control

// findJob:
func findJob(id int) (i int, found bool) {
	i = sort.Search(len(jobs), func(i int) bool { return jobs[i].Id >= id })
	return i, i < len(jobs) && jobs[i].Id == id
}

// kill jobs and wait for them to die in parallel
func killJobs() {
	var k int
	c := make(chan bool)
	for i, v := range jobs {
		if v.s == nil {
			continue
		}
		go func(s *sched.Sched) {
			s.Stop()
			c <- true
		}(v.s)
		jobs[i].s = nil // mark as not running
		k++
	}
	for k > 0 {
		<-c
		k--
	}
}

func killJob(id int) bool {
	i, ok := findJob(id)
	if !ok {
		return false
	}
	jobs[i].s.Stop()
	jobs = append(jobs[0:i], jobs[i+1:]...) // delete from list
	return true
}

func scheduleJob(j *jobDesc) {
	j.s = sched.New(int2dur(j.Period), int2dur(j.Start), func() {
		//fmt.Printf("%d: %s (start)\n", id, t)
		r := check.Run(j.Id, j.Check)
		s := "ok"
		if r.Flags&check.ResFail != 0 {
			s = r.Errs
		}
		fmt.Printf("%d: %s %+.24q\n", j.Id, s, r.S)
		//fmt.Printf("%d: %s\n%s\n", id, t, r)
		if err := insertResult(r); err != nil {
			fmt.Printf("Exec: %s\n", err)
		}
	})
	fmt.Printf("start job %d: period %d, start %d, check %v\n",
		j.Id, j.Period, j.Start, j.Check)
}

func addJob(j *jobDesc, start bool) bool {
	if !check.IsValid(j.Check) {
		return false
	}
	i, found := findJob(j.Id)
	if found {
		jobs[i].s.Stop()
		fmt.Printf("killed job %d\n", j.Id)
		jobs[i] = *j
	} else {
		jobs = append(jobs[:i], append(jobList{*j}, jobs[i:]...)...)
	}
	if start {
		scheduleJob(&jobs[i])
	}
	return true
}

func startJobs() {
	for i := range jobs {
		if jobs[i].s == nil {
			scheduleJob(&jobs[i])
		}
	}
}

func jobsEqual(a, b *jobDesc) bool {
	if a.Id != b.Id || a.Period != b.Period || a.Start != b.Start ||
		len(a.Check) != len(b.Check) {
		return false
	}
	for i, v := range a.Check {
		if v != b.Check[i] {
			return false
		}
	}
	return true
}

func mergeJobs(newjobs jobList) (status []bool, err error) {
	sort.Sort(newjobs)
	updated := false
	i, j := 0, 0
	for i < len(jobs) && j < len(newjobs) {
		switch {
		case jobs[i].Id == jobs[j].Id:
			if jobsEqual(&jobs[i], &newjobs[j]) {
				newjobs[j].s, jobs[i].s = jobs[i].s, nil
			} else {
				status[j] = check.IsValid(newjobs[j].Check)
				updated = true
			}
			i++
			j++
		case jobs[i].Id < newjobs[j].Id:
			i++
			updated = true
		default:
			status[j] = check.IsValid(newjobs[j].Check)
			j++
			updated = true
		}
	}
	if i < len(jobs) || j < len(newjobs) {
		updated = true
	}
	j = 0
	for _, v := range status {
		if v {
			j++
		} else {
			newjobs = append(newjobs[0:j], newjobs[j+1:]...)
		}
	}
	if updated {
		if err = replaceJobs(jobs, newjobs); err != nil {
			return
		}
	}
	killJobs()
	jobs = newjobs
	startJobs()
	return
}
