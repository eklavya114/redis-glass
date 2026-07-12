// Package resp implements the RESP (REdis Serialization Protocol) wire
// format: decoding client bytes into values and encoding replies back out.
// It knows nothing about individual Redis commands or storage.
package resp

import (
	"bufio"
	"errors"
	"strconv"
	"strings"
)

// Value represents a single RESP value.
type Value struct {
	Typ   byte // '+' '-' ':' '$' '*'
	Str   string
	Num   int
	Array []Value
}

var errMalformed = errors.New("malformed RESP input")

// Parse reads one RESP value from r.
func Parse(r *bufio.Reader) (Value, error) {
	line, err := readLine(r)
	if err != nil {
		return Value{}, err
	}
	if len(line) == 0 {
		return Value{}, errMalformed
	}

	typ := line[0]
	body := string(line[1:])

	switch typ {
	case '+':
		return Value{Typ: '+', Str: body}, nil
	case '-':
		return Value{Typ: '-', Str: body}, nil
	case ':':
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, errMalformed
		}
		return Value{Typ: ':', Num: n}, nil
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, errMalformed
		}
		if n < 0 {
			return Value{Typ: '$', Str: "", Num: -1}, nil
		}
		buf := make([]byte, n+2) // +2 for trailing \r\n
		if _, err := readFull(r, buf); err != nil {
			return Value{}, err
		}
		return Value{Typ: '$', Str: string(buf[:n])}, nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return Value{}, errMalformed
		}
		if n < 0 {
			return Value{Typ: '*', Array: nil}, nil
		}
		arr := make([]Value, n)
		for i := 0; i < n; i++ {
			v, err := Parse(r)
			if err != nil {
				return Value{}, err
			}
			arr[i] = v
		}
		return Value{Typ: '*', Array: arr}, nil
	default:
		// Inline command support: treat the whole line as a
		// space-separated command (e.g. plain "PING\r\n" from netcat).
		fields := strings.Fields(string(line))
		arr := make([]Value, len(fields))
		for i, f := range fields {
			arr[i] = Value{Typ: '$', Str: f}
		}
		return Value{Typ: '*', Array: arr}, nil
	}
}

// readLine reads a line up to and including \r\n and returns it without the trailing \r\n.
func readLine(r *bufio.Reader) ([]byte, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return nil, errMalformed
	}
	return line[:len(line)-2], nil
}

// readFull reads exactly len(buf) bytes and validates the trailing \r\n.
func readFull(r *bufio.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		m, err := r.Read(buf[n:])
		if err != nil {
			return n, err
		}
		n += m
	}
	if buf[len(buf)-2] != '\r' || buf[len(buf)-1] != '\n' {
		return n, errMalformed
	}
	return n, nil
}
