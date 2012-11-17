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

// Result structure for package check.
// Included in the more extensive package check on the node.

package check

import "fmt"

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
