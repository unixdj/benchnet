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

// Package check runs Benchnet checks (tests).  The checks are
// represented by string arrays, e.g., ["http" "get" "http://foo.bar/"].
package check

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"strings"
	"time"
)

// Flags for Result
const (
	ResFail = 1 << iota // Check failed (e.g. response not 200 for HTTP)
)

// Result represents the result of the check.
type Result struct {
	JobId uint64   // Id of job that started the check
	Flags int      // Flags (failure)
	Start int64    // Time the check ran, nanoseconds since Unix epoch
	RT    int64    // Run Time of the check, nanoseconds
	Errs  string   // Error string returned by libraries
	S     []string // Results of the run (e.g., HTTP headers)
}

// String dumps all fields of Result on several lines for easier debugging.
func (r *Result) String() string {
	return fmt.Sprintf("%q\njob: %v\nflags: %v\nerr: %v\nstart: %v\nelapsed: %d.%06d s\n",
		r.S, r.JobId, r.Flags, r.Errs, r.Start,
		r.RT/1e9, r.RT%1e9/1e3)
}

var (
	resultOk        = &Result{}
	errParam        = &Result{Flags: ResFail, Errs: "wrong number of parameters"}
	errUnknownCheck = &Result{Flags: ResFail, Errs: "unknown check"}
)

// errResult encloses err in a Result structure.
func errResult(err error) *Result {
	return &Result{Flags: ResFail, Errs: err.Error()}
}

// Maps are slow, arrays would be more efficient here.
// We don't care, because premature optimisation and all that.

// hierarchial maps of checks; see runCheck()
type checkMap map[string]struct {
	f func(checkMap, []string, bool) *Result // function to run
	m checkMap                               // map to pass to f
}

// top level
var checks = checkMap{
	"http": {runCheck, httpChecks},
	"dns":  {checkDNSLookup, nil},
}

// http
var httpChecks = checkMap{
	"get":  {checkHttpGet, nil},
	"head": {checkHttpHead, nil},
	//"post": checkHttpPost,
}

// runCheck finds the check represented by s[0] in m and runs it,
// passing the parameters s[1:] to it.  Hierarchial trees can be
// built by setting its f to runCheck and its m to another map.
func runCheck(m checkMap, s []string, dryrun bool) *Result {
	if len(s) < 2 {
		return errParam
	}
	if v, ok := m[s[0]]; ok {
		return v.f(v.m, s[1:], dryrun)
	}
	return errUnknownCheck
}

// checks

// if you edit this, edit the hardcoded array at the start of checkHttp()
const (
	httpGet = iota
	httpHead
	httpPost
)

// the real handler got GET, HEAD and POST
func checkHttp(v int, s []string, dryrun bool) *Result {
	// hardcoded array of numbers of parameters for http "verbs"!
	if len(s) != []int{1, 1, 2}[v] {
		return errParam
	}
	if dryrun {
		return resultOk
	}
	var (
		resp *http.Response
		err  error
	)
	switch v {
	case httpGet:
		resp, err = http.Get(s[0])
	case httpHead:
		resp, err = http.Head(s[0])
	case httpPost:
		// TODO: something sane (dead code now anyway)
		resp, err = http.Post(s[0], "text/plain", strings.NewReader(s[1]))
	}
	if err != nil {
		return errResult(err)
	}
	defer resp.Body.Close()
	a, err := httputil.DumpRequest(resp.Request, false)
	if err != nil {
		return errResult(err)
	}
	b, err := httputil.DumpResponse(resp, false)
	if err != nil {
		return errResult(err)
	}
	var flags int
	if resp.StatusCode != 200 {
		flags |= ResFail
	}
	return &Result{
		Flags: flags,
		S:     []string{resp.Status, string(a), string(b)},
	}
}

func checkHttpGet(m checkMap, s []string, dryrun bool) *Result {
	return checkHttp(httpGet, s, dryrun)
}

func checkHttpHead(m checkMap, s []string, dryrun bool) *Result {
	return checkHttp(httpHead, s, dryrun)
}

func checkDNSLookup(m checkMap, s []string, dryrun bool) *Result {
	if len(s) != 1 {
		return errParam
	}
	if dryrun {
		return resultOk
	}
	a, err := net.LookupHost(s[0])
	if err != nil {
		return &Result{Flags: ResFail, Errs: err.Error(), S: a}
	}
	return &Result{S: a}
}

// IsValid validates the check represented by s without actually running it.
func IsValid(s []string) bool {
	return runCheck(checks, s, true).Flags&ResFail == 0
}

// Run runs the check represented by s.
func Run(id uint64, s []string) *Result {
	start := time.Now()
	r := runCheck(checks, s, false)
	r.JobId, r.Start, r.RT = id, start.UnixNano(), int64(time.Now().Sub(start))
	return r
}
