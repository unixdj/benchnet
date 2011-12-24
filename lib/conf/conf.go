/*
Package conf parses simple configuration files.

Configuration file syntax (see Parse() for semantics):

The file is composed of lines of UTF-8 text, each no longer than 4KB.
Comments start with '#' and continue to end of line.
Whitespace (Unicode character class Z) between tokens is ignored.
Configuration settings look like this:

	ident = value

Identifiers start with an ASCII letter, dash ('-') or underscore ('_'),
and continue with zero or more ASCII letters, ASCII digits, dashes or
underscores.  That is, they match /[-_a-zA-Z][-_a-zA-Z0-9]/.

Values may be plain or quoted.  Plain values may have any character in
them besides space (Unicode character class Z), control characters
(Unicode character class C), or any of '"', '#', `'`, '=', `\`.

Quoted values are enclosed in double quotes (like "this") and obey Go
quoted string rules.  They may not include Unicode control characters.
Any character except `"` and `\` stands for itself.  Backslash escapes
\a, \b, \f, \n, \r, \v, \", \\, \337, \xDF, \u1A2F and \U00104567 are
accepted.  Quoted values, unlike plain ones, can be empty ("").

The rule about control characters means that tabs inside quoted strings
must be replaced with "\t" (or "\U00000009" or whatever).

Example:

	ipv6-addr = [::1]:23         # Look ma, no quotes!
	file      = /etc/passwd      # Comments after settings are OK.
	--        = "hello, world\n" # Variables can have strange names.

More formally:

	file:
		lines
	lines:
		|
		line lines
	line:
		optional-assignment optional-comment '\n'
	optional-assignment:
		|
		assignment
	assignment:
		ident '=' value
	value:
		plain-value |
		quoted-value
	optional-comment:
		|
		comment

Tokens:
	ident: <ident-alpha> <ident-alnum>*
	<ident-alpha>: ascii-alpha | '-' | '_'
	<ident-alnum>: <ident-alpha> | ascii-digit

	plain-value: <plain-val-char>+
	<plain-val-char>: anything except space, control, '"', '#', `'`, '=', `\`

	quoted-value: '"' <quoted-val-char>* '"'
	<quoted-val-char>: <self-char> | `\` <quoted-char>
	<self-char>: anything except control, `"`, `\`
	<quoted-char>: 'a' | 'b' | 'f' | 'n' | 'r' | 'v' | `\` | '"' |
	<octal-byte-value> | <hex-byte-value> | <unicode-value>
	<octal-byte-value>: octal-digit{3}
	<hex-byte-value>: 'x' hex-digit{2}
	<unicode-value>: 'u' hex-digit{4} | 'U' hex-digit{8}

	comment: '#' <any-char>*

	ascii-alpha: [a-zA-Z]
	ascii-digit: [0-9]
	control: Unicode character class C (includes 00-1F and 80-9F)
	space: Unicode character class Z
*/
package conf

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// Type of value for Var.Typ (default is String)
const (
	String   = iota // string
	Signed          // int64
	Unsigned        // uint64
)

// Var describes a configuration variable and has pointers to corresponding
// (Go) variables.  Slice of Var is used for calling Parse().
type Var struct {
	Name     string         // name of configuration variable
	Typ      int            // type: string or integer
	Max      uint64         // maximum num value or 0; signed: min = -max-1
	RE       *regexp.Regexp // regexp to match value or nil
	Required bool           // variable is required to be set in conf file
	Sval     *string        // unquoted string value, always set if present
	Snval    *int64         // signed numeric value, for Typ Signed
	Unval    *uint64        // unsigned numeric value, for Typ Unsigned
	set      bool           // has been set
}

type parser struct {
	r    *bufio.Reader
	file string
	line int
	vars []Var
}

type ParseError struct {
	file string
	line int
	err  string
}

const (
	outOfRange  = "%s: value out of range"
	syntaxError = "syntax error"
)

var (
	identRE  = regexp.MustCompile(`^[-_a-zA-Z][-_a-zA-Z0-9]*`)
	plainRE  = regexp.MustCompile(`^[^\pZ\pC"#'=\\]+`)
	quotedRE = regexp.MustCompile(`^"(?:[^\pC"\\]|\\[^\pC])*"`)
)

func (p *ParseError) Error() string {
	if p.line == 0 {
		return fmt.Sprintf("%s: %s\n", p.file, p.err)
	}
	return fmt.Sprintf("%s:%d: %s\n", p.file, p.line, p.err)
}

func (p *parser) newError(s string, a ...interface{}) *ParseError {
	return &ParseError{p.file, p.line, fmt.Sprintf(s, a...)}
}

func eatSpace(s string) string {
	return strings.TrimLeftFunc(s, unicode.IsSpace)
}

func (p *parser) setValue(v *Var, value string) error {
	if v.set {
		return p.newError("%s appears more than once", v.Name)
	}
	v.set = true
	if v.Sval != nil {
		*v.Sval = value
	}
	var err error
	switch v.Typ {
	case String:
		*v.Sval = value // force panic if nil
		if v.RE != nil && !v.RE.MatchString(value) {
			return p.newError("%s: invalid syntax", v.Name)
		}
	case Signed:
		*v.Snval, err = strconv.ParseInt(value, 0, 64)
		if v.Max != 0 && (*v.Snval > int64(v.Max) || *v.Snval < -int64(v.Max)-1) {
			return p.newError(outOfRange, v.Name)
		}
	case Unsigned:
		*v.Unval, err = strconv.ParseUint(value, 0, 64)
		if v.Max != 0 && *v.Unval > v.Max {
			return p.newError(outOfRange, v.Name)
		}
	default:
		panic("printer on fire")
	}
	if err != nil {
		// strip fluff from strconf.Parse{I,Ui}nt
		if ne, ok := err.(*strconv.NumError); ok {
			return p.newError("%s: %s", v.Name, ne.Err)
		}
		panic("NOTREACHED")
	}
	return nil
}

func (p *parser) parseLine(line string) error {
	line = eatSpace(line)
	if line == "" || line[0] == '#' {
		return nil
	}
	ident := identRE.FindString(line)
	line = eatSpace(line[len(ident):])
	if ident == "" || line == "" || line[0] != '=' {
		return p.newError(syntaxError)
	}
	line = eatSpace(line[1:])
	value := plainRE.FindString(line)
	vlen := len(value)
	if value == "" {
		value = quotedRE.FindString(line)
		vlen = len(value)
		var err error
		value, err = strconv.Unquote(value)
		if err != nil {
			return p.newError(syntaxError)
		}
	}
	line = eatSpace(line[vlen:])
	if len(line) != 0 && line[0] != '#' {
		return p.newError(syntaxError)
	}
	for i, v := range p.vars {
		if ident == v.Name {
			return p.setValue(&p.vars[i], value)
		}
	}
	return p.newError("%s: unknown variable", ident)
}

// Parse parses the configuration file from r according the description
// in vars and sets the variables pointed to to the values in the file.
// The filename is used in error messages; if empty, it's set to "stdin".
// It returns nil on success, ParseError on parsing error and something
// from the depths of io on actual real error.
//
// Parsing stops on the first error encountered.  Setting an unknown
// variable, setting a variable more than once or omitting a Var whose
// Required == true are errors.
//
// When parsing, the value gets unquoted if needed and the Var
// corresponding to the identifier is found.  If Sval is not nil, the
// string it points to is set to the unquoted string value.  If the
// Var's Typ is String, Sval can't be nil.
//
// If Typ is String and RE is not nil, RE is matched against the
// string.  You normally want to enclose your regexp in ^$.
//
// The values at Snval and Unval are set if Typ is Signed or Unsigned,
// respectively.  If Max is not 0, the value is checked against it.  If
// Signed, it's also checked against min == -Max-1.  Numeric values can
// be given as decimal, octal or hexadecimal, in the usual C/Go manner
// (255 == 0377 == 0xff).
//
// The parsing sequence implies that even when a number is desired,
// the quoted string "\x32\u0033" is the same as unquoted 23.
func Parse(r io.Reader, filename string, vars []Var) error {
	p := &parser{file: filename, vars: vars}
	if p.file == "" {
		p.file = "stdin"
	}
	if t, ok := r.(*bufio.Reader); ok {
		p.r = t
	} else {
		p.r = bufio.NewReader(r)
	}
	for {
		p.line++
		buf, ispref, err := p.r.ReadLine()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		} else if ispref {
			return p.newError("line too long")
		}
		if err = p.parseLine(string(buf)); err != nil {
			return err
		}
	}
	for _, v := range p.vars {
		if v.Required && !v.set {
			p.line = 0 // for ParseError.Error()
			return p.newError("%s required but not set", v.Name)
		}
	}
	return nil
}
