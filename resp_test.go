package main

import (
	"io"
	"reflect"
	"strings"
	"testing"
)

// wire is a connection that has raw to read, and keeps what is written.
type wire struct {
	io.Reader
	io.Writer
}

// reply reads raw, a reply as it goes over the wire, as the client does.
func reply(t *testing.T, raw string) (any, error) {
	t.Helper()
	return newClient(wire{strings.NewReader(raw), io.Discard}).do("PING")
}

func TestEncodeCommand(t *testing.T) {
	got := string(encodeCommand([]string{"SET", "key", "a b\r\n", ""}))
	want := "*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\na b\r\n\r\n$0\r\n\r\n"
	if got != want {
		t.Errorf("encodeCommand() = %q, want %q", got, want)
	}
}

func TestReadReply(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want any
	}{
		{"simple string", "+OK\r\n", "OK"},
		{"error", "-ERR unknown command\r\n", replyError("ERR unknown command")},
		{"integer", ":42\r\n", int64(42)},
		{"negative integer", ":-1\r\n", int64(-1)},
		{"bulk string", "$5\r\nhello\r\n", "hello"},
		{"bulk string with CRLF inside", "$4\r\na\r\nb\r\n", "a\r\nb"},
		{"empty bulk string", "$0\r\n\r\n", ""},
		{"null bulk string", "$-1\r\n", nil},
		{"bulk string that is not UTF-8", "$2\r\n\xff\xfe\r\n", map[string]any{"base64": "//4="}},
		{"array", "*3\r\n$1\r\na\r\n:2\r\n$-1\r\n", []any{"a", int64(2), nil}},
		{"empty array", "*0\r\n", []any{}},
		{"null array", "*-1\r\n", nil},
		{"nested array", "*2\r\n*1\r\n+a\r\n*0\r\n", []any{[]any{"a"}, []any{}}},
		{"error inside an array", "*2\r\n+OK\r\n-ERR no\r\n", []any{"OK", replyError("ERR no")}},
		{"null", "_\r\n", nil},
		{"true", "#t\r\n", true},
		{"false", "#f\r\n", false},
		{"double", ",1.5\r\n", 1.5},
		{"infinity", ",inf\r\n", "inf"},
		{"negative infinity", ",-inf\r\n", "-inf"},
		{"big number", "(3492890328409238509324850943850943825024385\r\n", "3492890328409238509324850943850943825024385"},
		{"blob error", "!8\r\nERR a\r\nb\r\n", replyError("ERR a\r\nb")},
		{"verbatim string", "=8\r\ntxt:some\r\n", "some"},
		{"map", "%2\r\n+name\r\n$3\r\nAda\r\n+age\r\n:36\r\n", map[string]any{"name": "Ada", "age": int64(36)}},
		{"map keyed by a number", "%1\r\n:1\r\n+one\r\n", map[string]any{"1": "one"}},
		{"set", "~2\r\n+a\r\n+b\r\n", []any{"a", "b"}},
		{"attribute before the value", "|1\r\n+ttl\r\n:10\r\n$1\r\nv\r\n", "v"},
		{"push before the reply", ">2\r\n+invalidate\r\n*1\r\n+k\r\n:7\r\n", int64(7)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reply(t, tt.raw)
			if err != nil {
				t.Fatalf("read error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("read = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestReadReplyFailures(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"nothing", "", "closed the connection before the reply ended"},
		{"line cut short", "+OK", "closed the connection before the reply ended"},
		{"string cut short", "$5\r\nhel", "closed the connection before the reply ended"},
		{"array cut short", "*2\r\n+a\r\n", "closed the connection before the reply ended"},
		{"line without CR", "+OK\n", "does not end with CRLF"},
		{"string without CRLF", "$2\r\nabcd", "does not end with CRLF"},
		{"unknown type", "?what\r\n", "no type of RESP"},
		{"empty line", "\r\n", "an empty line"},
		{"integer that is not one", ":abc\r\n", "an integer that is not one"},
		{"length that is not one", "$abc\r\n", "a length that is not one"},
		{"negative length", "*-2\r\n", "a length that is not one"},
		{"string too large", "$16777217\r\n", "more than the 16777216 the action reads"},
		{"array too long", "*1048577\r\n", "more than the 1048576 the action reads"},
		{"boolean that is not one", "#x\r\n", "a boolean that is not one"},
		{"double that is not one", ",x\r\n", "a double that is not one"},
		{"nested too deep", strings.Repeat("*1\r\n", depthLimit+2) + "+a\r\n", "nests deeper"},
		{"line too long", "+" + strings.Repeat("a", lineLimit+10) + "\r\n", "a line longer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := reply(t, tt.raw)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("read = %.80v, %v, want an error containing %q", got, err, tt.want)
			}
		})
	}
}

func TestReplyErrorCode(t *testing.T) {
	for in, want := range map[string]string{
		"WRONGTYPE Operation against a key": "WRONGTYPE",
		"ERR":                               "ERR",
		"MOVED 3999 127.0.0.1:6381":         "MOVED",
		"":                                  "",
	} {
		if got := replyError(in).code(); got != want {
			t.Errorf("code of %q = %q, want %q", in, got, want)
		}
	}
}
