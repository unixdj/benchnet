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

// Package sched implements a simple scheduler.
package sched

import "time"

// Sched represets a scheduler instance.
type Sched struct {
	headShot chan bool
}

// Stop stops the scheduler s.  If f is currently running, Stop
// will not return until it's finished.  If called twice, Stop
// will hang forever.
func (s *Sched) Stop() {
	s.headShot <- true
}

func (s *Sched) thread(period time.Duration, start time.Duration, f func()) {
	select {
	case <-s.headShot:
		return
	case <-time.After(start):
	}
	ticker := time.NewTicker(period)
	for {
		f()
		select {
		case <-s.headShot:
			ticker.Stop()
			return
		case <-ticker.C:
		}
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
