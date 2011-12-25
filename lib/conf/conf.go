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
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// Value is the interface to the value pointed to by Var.
type Value interface {
	Set(string) error
}

// Uint64Value represents a configuration variable's uint64 value.
// Numeric values can be given as decimal, octal or hexadecimal,
// in the usual C/Go manner (255 == 0377 == 0xff).
type Uint64Value uint64

func (v *Uint64Value) Set(s string) error {
	u, err := strconv.ParseUint(s, 0, 64)
	if err != nil {
		// strip fluff from strconf.ParseUint
		return err.(*strconv.NumError).Err
	}
	*v = Uint64Value(u)
	return nil
}

// StringValue represents a configuration variable's string value.
type StringValue string

func (v *StringValue) Set(s string) error {
	*v = StringValue(s)
	return nil
}

// Var describes a configuration variable and has pointers to corresponding
// (Go) variables.  Slice of Var is used for calling Parse().
type Var struct {
	Name     string // name of configuration variable
	Val      Value  // Value to set
	Required bool   // variable is required to be set in conf file
	set      bool   // has been set
}

type parser struct {
	r     *bufio.Reader
	file  string
	line  int
	ident string
	value string
	vars  []Var
}

const (
	outOfRange  = "value out of range"
	syntaxError = "syntax error"
)

// ParseError represents the error.
type ParseError struct {
	File  string // filename or "stdin"
	Line  int    // line number or 0
	Ident string // identifier or ""
	Value string // value as appears in input, possibly quoted; or ""
	Err   error  // error
}

// Error prints ParseError as follows:
//     File:[Line:][ Ident:] Err
// Value never gets printed.
func (p *ParseError) Error() string {
	var line, ident string
	if p.Line != 0 {
		line = fmt.Sprintf("%d:", p.Line)
	}
	if p.Ident != "" {
		ident = fmt.Sprintf(" %s:", p.Ident)
	}
	return fmt.Sprintf("%s:%s%s %s\n", p.File, line, ident, p.Err)
}

// newError creates ParseError from s
func (p *parser) newError(s string) *ParseError {
	return &ParseError{p.file, p.line, p.ident, p.value, errors.New(s)}
}

// Regexps for tokens
var (
	identRE  = regexp.MustCompile(`^[-_a-zA-Z][-_a-zA-Z0-9]*`)
	plainRE  = regexp.MustCompile(`^[^\pZ\pC"#'=\\]+`)
	quotedRE = regexp.MustCompile(`^"(?:[^\pC"\\]|\\[^\pC])*"`)
)

func eatSpace(s string) string {
	return strings.TrimLeftFunc(s, unicode.IsSpace)
}

func (p *parser) setValue(value string) error {
	for i := range p.vars {
		v := &p.vars[i]
		if p.ident == v.Name {
			if v.set {
				return p.newError("already defined")
			}
			v.set = true
			if err := v.Val.Set(value); err != nil {
				return &ParseError{p.file, p.line, p.ident,
					p.value, err}
			}
			return nil
		}
	}
	return p.newError("unknown variable")
}

func (p *parser) parseLine(line string) error {
	line = eatSpace(line)
	if line == "" || line[0] == '#' {
		return nil
	}
	p.ident = identRE.FindString(line)
	line = eatSpace(line[len(p.ident):])
	if p.ident == "" || line == "" || line[0] != '=' {
		return p.newError(syntaxError)
	}
	line = eatSpace(line[1:])
	p.value = plainRE.FindString(line)
	unquoted := p.value
	if p.value == "" {
		p.value = quotedRE.FindString(line)
		var err error
		unquoted, err = strconv.Unquote(p.value)
		if err != nil {
			return p.newError(syntaxError)
		}
	}
	line = eatSpace(line[len(p.value):])
	if len(line) != 0 && line[0] != '#' {
		return p.newError(syntaxError)
	}
	return p.setValue(unquoted)
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
// corresponding to the identifier is found.  Then the Set() method
// is called to set the Var.  If you need syntax validation, you
// should create your own Value type and return an error from Set()
// on invalid input.
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
		p.ident, p.value = "", ""
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
			p.ident = v.Name
			p.line, p.value = 0, ""
			return p.newError("required but not set")
		}
	}
	return nil
}
