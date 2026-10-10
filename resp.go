package main

import (
	"bufio"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
)

// bulkLimit is the largest string the action reads in a reply. A larger one
// fails the step.
const bulkLimit = 16 << 20

// lengthLimit is the most entries a list, a set or a map of a reply can
// have.
const lengthLimit = 1 << 20

// lineLimit is the longest line of a reply, such as a status or an error.
const lineLimit = 64 << 10

// depthLimit is how deep the lists of a reply can nest.
const depthLimit = 32

// replyError is an error the server answered a command with, such as
// "WRONGTYPE Operation against a key holding the wrong kind of value". It
// is a reply like any other, not a failure to talk to the server.
type replyError string

// code is the first word of an error reply, which names its kind: ERR,
// WRONGTYPE, NOAUTH, MOVED and so on.
func (e replyError) code() string {
	code, _, _ := strings.Cut(string(e), " ")
	return code
}

// push is a message the server sent without being asked, which RESP3 marks
// as such. It answers no command.
type push []any

// client speaks RESP, the protocol of Redis and Valkey, over a connection:
// it writes a command and reads the reply to it. It reads the types of
// RESP3 as well as those of RESP2, so it works whichever the connection
// speaks.
type client struct {
	r *bufio.Reader
	w io.Writer
}

func newClient(rw io.ReadWriter) *client {
	return &client{r: bufio.NewReader(rw), w: rw}
}

// do sends a command and returns the reply to it. An error reply is
// returned as a replyError value, and the error is for a connection that
// failed or a reply that cannot be read.
func (c *client) do(args ...string) (any, error) {
	if _, err := c.w.Write(encodeCommand(args)); err != nil {
		return nil, err
	}
	for {
		v, err := c.read(0)
		if err != nil {
			return nil, err
		}
		if _, ok := v.(push); ok {
			continue
		}
		return v, nil
	}
}

// encodeCommand writes a command as a client sends it: a list of bulk
// strings.
func encodeCommand(args []string) []byte {
	b := make([]byte, 0, 32)
	b = append(b, '*')
	b = strconv.AppendInt(b, int64(len(args)), 10)
	b = append(b, '\r', '\n')
	for _, a := range args {
		b = append(b, '$')
		b = strconv.AppendInt(b, int64(len(a)), 10)
		b = append(b, '\r', '\n')
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	return b
}

// read reads one value of a reply as a plain value: a string, an int64, a
// float64, a bool, nil, a list or a map keyed by strings. An error is a
// replyError and a push a push.
func (c *client) read(depth int) (any, error) {
	if depth > depthLimit {
		return nil, fmt.Errorf("the reply nests deeper than %d lists", depthLimit)
	}
	line, err := c.line()
	if err != nil {
		return nil, err
	}
	if line == "" {
		return nil, errors.New("the reply has an empty line where a value starts")
	}
	typ, rest := line[0], line[1:]

	switch typ {
	case '+':
		return text([]byte(rest)), nil
	case '-':
		return replyError(strings.ToValidUTF8(rest, "�")), nil
	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("the reply has an integer that is not one: %q", rest)
		}
		return n, nil
	case '$', '=', '!':
		b, null, err := c.bulk(rest)
		if err != nil || null {
			return nil, err
		}
		switch typ {
		case '!':
			return replyError(strings.ToValidUTF8(string(b), "�")), nil
		case '=':
			// A verbatim string starts with its format, as "txt:".
			if len(b) >= 4 && b[3] == ':' {
				b = b[4:]
			}
		}
		return text(b), nil
	case '*', '~', '>':
		n, null, err := length(rest, lengthLimit)
		if err != nil || null {
			return nil, err
		}
		list := make([]any, 0, min(n, 1024))
		for range n {
			v, err := c.read(depth + 1)
			if err != nil {
				return nil, err
			}
			list = append(list, v)
		}
		if typ == '>' {
			return push(list), nil
		}
		return list, nil
	case '%', '|':
		n, null, err := length(rest, lengthLimit)
		if err != nil || null {
			return nil, err
		}
		m := make(map[string]any, min(n, 1024))
		for range n {
			k, err := c.read(depth + 1)
			if err != nil {
				return nil, err
			}
			v, err := c.read(depth + 1)
			if err != nil {
				return nil, err
			}
			m[key(k)] = v
		}
		// An attribute says something about the value that follows it, and
		// is not part of the reply.
		if typ == '|' {
			return c.read(depth)
		}
		return m, nil
	case '_':
		return nil, nil
	case '#':
		switch rest {
		case "t":
			return true, nil
		case "f":
			return false, nil
		}
		return nil, fmt.Errorf("the reply has a boolean that is not one: %q", rest)
	case ',':
		return double(rest)
	case '(':
		// A big number does not fit a number of a result, so it stays as
		// the digits the server sent.
		return rest, nil
	}
	return nil, fmt.Errorf("the reply starts with %q, which is no type of RESP", typ)
}

// line reads a line of a reply without its CRLF.
func (c *client) line() (string, error) {
	var line []byte
	for {
		part, err := c.r.ReadSlice('\n')
		line = append(line, part...)
		if len(line) > lineLimit {
			return "", fmt.Errorf("the reply has a line longer than %d bytes", lineLimit)
		}
		if err == nil {
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return "", unexpected(err)
		}
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("the reply has a line that does not end with CRLF")
	}
	return string(line[:len(line)-2]), nil
}

// bulk reads the bytes of a string whose length the line gave. null is true
// for the null string of RESP2.
func (c *client) bulk(lengthLine string) (b []byte, null bool, err error) {
	n, null, err := length(lengthLine, bulkLimit)
	if err != nil || null {
		return nil, null, err
	}
	b = make([]byte, n+2)
	if _, err := io.ReadFull(c.r, b); err != nil {
		return nil, false, unexpected(err)
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, false, errors.New("the reply has a string that does not end with CRLF")
	}
	return b[:n], false, nil
}

// length reads the length of a string, a list or a map, which can be limit
// at most. null is true for -1, the null of RESP2.
func length(s string, limit int) (n int, null bool, err error) {
	if s == "-1" {
		return 0, true, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v < 0 {
		return 0, false, fmt.Errorf("the reply has a length that is not one: %q", s)
	}
	if v > int64(limit) {
		return 0, false, fmt.Errorf("the reply has a length of %d, more than the %d the action reads", v, limit)
	}
	return int(v), false, nil
}

// double reads a floating point number of RESP3. An infinity or a NaN,
// which a number of a result cannot be, stays as the server wrote it.
func double(s string) (any, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, fmt.Errorf("the reply has a double that is not one: %q", s)
	}
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return s, nil
	}
	return f, nil
}

// text returns the bytes of a string of a reply as a value of a result:
// the string when it is UTF-8, which is all a result can carry, and
// otherwise an object with its bytes in base64.
func text(b []byte) any {
	if utf8.Valid(b) {
		return string(b)
	}
	return map[string]any{"base64": base64.StdEncoding.EncodeToString(b)}
}

// key returns a key of a map of a reply as the string a result keys by.
func key(k any) string {
	switch v := k.(type) {
	case string:
		return v
	case nil:
		return ""
	case map[string]any:
		if b, ok := v["base64"].(string); ok && len(v) == 1 {
			return b
		}
	}
	return fmt.Sprint(k)
}

// unexpected says that the connection ended in the middle of a reply,
// rather than the bare "EOF".
func unexpected(err error) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errors.New("the server closed the connection before the reply ended")
	}
	return err
}
