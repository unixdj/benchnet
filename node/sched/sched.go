// Package sched implements a simple scheduler.
package sched

import (
	"time"
)

/*
const (
	minPeriod = time.Minute
	maxPeriod = time.Hour * 24 * 14
)
*/

type Sched struct {
	headShot chan bool
}

// Stop stops the scheduler.  If f is currently running, Stop will
// not return until it's finished.
func (s *Sched) Stop() {
	s.headShot <- true
}

func (s *Sched) thread(period time.Duration, start time.Duration, f func()) {
	timer := time.NewTimer(start)
	select {
	case <-s.headShot:
		timer.Stop()
		return
	case <-timer.C:
	}
	f()
	ticker := time.NewTicker(period)
	for {
		select {
		case <-s.headShot:
			ticker.Stop()
			return
		case <-ticker.C:
		}
		f()
	}
}

// New starts a new scheduler running f each period, at Unix time
// N*period+offset where N is natural.  No more than one instance
// of f will run at any given moment.
func New(period time.Duration, offset time.Duration, f func()) *Sched {
	// This will break after Fri Apr 11 23:47:16 +0000 UTC 2262
	nanonow := time.Duration(time.Now().UnixNano())
	start := period - (nanonow-offset)%period
	if start < time.Millisecond {
		start += period
	}
	s := Sched{headShot: make(chan bool)}
	go s.thread(period, start, f)
	return &s
}
