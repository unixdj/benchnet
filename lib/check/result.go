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
