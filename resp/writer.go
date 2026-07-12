package resp

import (
	"bufio"
	"strconv"
)

// WriteSimpleString writes s as a RESP simple string (e.g. +OK\r\n).
func WriteSimpleString(w *bufio.Writer, s string) error {
	_, err := w.WriteString("+" + s + "\r\n")
	return err
}

// WriteError writes msg as a RESP error (e.g. -ERR bad request\r\n).
func WriteError(w *bufio.Writer, msg string) error {
	_, err := w.WriteString("-" + msg + "\r\n")
	return err
}

// WriteInteger writes n as a RESP integer (e.g. :5\r\n).
func WriteInteger(w *bufio.Writer, n int) error {
	_, err := w.WriteString(":" + strconv.Itoa(n) + "\r\n")
	return err
}

// WriteBulkString writes s as a RESP bulk string (e.g. $5\r\nhello\r\n).
func WriteBulkString(w *bufio.Writer, s string) error {
	_, err := w.WriteString("$" + strconv.Itoa(len(s)) + "\r\n" + s + "\r\n")
	return err
}

// WriteNullBulk writes the RESP null bulk string ($-1\r\n), used for a
// missing key or nil result.
func WriteNullBulk(w *bufio.Writer) error {
	_, err := w.WriteString("$-1\r\n")
	return err
}

// WriteArray writes vals as a RESP array of bulk strings.
func WriteArray(w *bufio.Writer, vals []string) error {
	if _, err := w.WriteString("*" + strconv.Itoa(len(vals)) + "\r\n"); err != nil {
		return err
	}
	for _, v := range vals {
		if err := WriteBulkString(w, v); err != nil {
			return err
		}
	}
	return nil
}
