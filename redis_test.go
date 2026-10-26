package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe/actionref"
	"github.com/mozership/probe/actionrpc"
)

// recorded is what a test server saw: the commands sent to it, and how many
// connections were made.
type recorded struct {
	mu          sync.Mutex
	commands    [][]string
	connections int
}

func (r *recorded) got() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.commands)
}

func (r *recorded) connected() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connections
}

// serve answers every command sent to l with what handle returns, a reply
// as it goes over the wire. An empty reply closes the connection.
func serve(t *testing.T, l net.Listener, handle func(args []string) string) *recorded {
	t.Helper()
	rec := &recorded{}
	done := make(chan struct{})
	t.Cleanup(func() {
		close(done)
		_ = l.Close()
	})
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			rec.mu.Lock()
			rec.connections++
			rec.mu.Unlock()
			go func() {
				defer func() { _ = conn.Close() }()
				go func() {
					<-done
					_ = conn.Close()
				}()
				c := newClient(conn)
				for {
					v, err := c.read(0)
					if err != nil {
						return
					}
					list, _ := v.([]any)
					args := make([]string, len(list))
					for i, a := range list {
						args[i], _ = a.(string)
					}
					rec.mu.Lock()
					rec.commands = append(rec.commands, args)
					rec.mu.Unlock()
					out := handle(args)
					if out == "" {
						return
					}
					if _, err := conn.Write([]byte(out)); err != nil {
						return
					}
				}
			}()
		}
	}()
	return rec
}

// server starts a test server on a port of its own, and returns the URL of
// it.
func server(t *testing.T, handle func(args []string) string) (string, *recorded) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return "redis://" + l.Addr().String(), serve(t, l, handle)
}

// store answers SET, GET, DEL and a few others from a map, as a server
// would.
func store() func(args []string) string {
	var mu sync.Mutex
	data := map[string]string{}
	return func(args []string) string {
		mu.Lock()
		defer mu.Unlock()
		switch strings.ToUpper(args[0]) {
		case "PING":
			return "+PONG\r\n"
		case "SET":
			data[args[1]] = args[2]
			return "+OK\r\n"
		case "GET":
			v, ok := data[args[1]]
			if !ok {
				return "$-1\r\n"
			}
			return fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		case "DEL":
			_, ok := data[args[1]]
			delete(data, args[1])
			if ok {
				return ":1\r\n"
			}
			return ":0\r\n"
		case "AUTH", "SELECT":
			return "+OK\r\n"
		}
		return "-ERR unknown command '" + args[0] + "'\r\n"
	}
}

func run(t *testing.T, with map[string]any) map[string]any {
	t.Helper()
	ret, err := (&Action{}).Run(with)
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	return ret
}

func res(ret map[string]any) map[string]any {
	return ret["res"].(map[string]any)
}

func TestRun(t *testing.T) {
	url, rec := server(t, store())
	ret := run(t, map[string]any{
		"url": url,
		"commands": []any{
			[]any{"SET", "greeting", "hello world"},
			[]any{"GET", "greeting"},
			[]any{"GET", "missing"},
			[]any{"DEL", "greeting"},
		},
	})

	if got, want := res(ret)["results"], []any{"OK", "hello world", nil, int64(1)}; !reflect.DeepEqual(got, want) {
		t.Errorf("res.results = %#v, want %#v", got, want)
	}
	if got := res(ret)["errors"]; !reflect.DeepEqual(got, []any{}) {
		t.Errorf("res.errors = %#v, want none", got)
	}
	if ret["status"] != 0 {
		t.Errorf("status = %v, want 0", ret["status"])
	}
	if _, err := time.ParseDuration(ret["rt"].(string)); err != nil {
		t.Errorf("rt = %v, want a duration", ret["rt"])
	}
	req := ret["req"].(map[string]any)
	if req["url"] != url {
		t.Errorf("req.url = %v, want %v", req["url"], url)
	}
	wantCommands := []any{
		[]any{"SET", "greeting", "hello world"},
		[]any{"GET", "greeting"},
		[]any{"GET", "missing"},
		[]any{"DEL", "greeting"},
	}
	if !reflect.DeepEqual(req["commands"], wantCommands) {
		t.Errorf("req.commands = %#v, want %#v", req["commands"], wantCommands)
	}
	want := [][]string{{"SET", "greeting", "hello world"}, {"GET", "greeting"}, {"GET", "missing"}, {"DEL", "greeting"}}
	if got := rec.got(); !reflect.DeepEqual(got, want) {
		t.Errorf("the server got %v, want %v", got, want)
	}
	if rec.connected() != 1 {
		t.Errorf("the action made %d connections, want 1", rec.connected())
	}
}

// What a step returns goes to Probe as a protobuf value, which takes plain
// values only.
func TestRunResultIsSendable(t *testing.T) {
	url, _ := server(t, func(args []string) string {
		return "*5\r\n:7\r\n$2\r\n\xff\xfe\r\n%1\r\n+k\r\n,1.5\r\n#t\r\n-ERR no\r\n"
	})
	ret := run(t, map[string]any{"url": url, "commands": []any{"PING"}})
	want := []any{[]any{int64(7), map[string]any{"base64": "//4="}, map[string]any{"k": 1.5}, true, nil}}
	if got := res(ret)["results"]; !reflect.DeepEqual(got, want) {
		t.Errorf("res.results = %#v, want %#v", got, want)
	}
	if _, err := actionrpc.Sendable(ret); err != nil {
		t.Errorf("the result cannot be sent to Probe: %v", err)
	}
}

func TestRunCommandForms(t *testing.T) {
	url, rec := server(t, store())
	run(t, map[string]any{
		"url": url,
		"commands": []any{
			`SET "a key" "two\nlines"`,
			"  GET   'a key' ",
			[]any{"SET", "n", 42},
			[]any{"SET", "f", float64(3)},
			[]any{"SET", "g", 1.5},
			[]any{"set", "big", int64(9007199254740993)},
		},
	})
	want := [][]string{
		{"SET", "a key", "two\nlines"},
		{"GET", "a key"},
		{"SET", "n", "42"},
		{"SET", "f", "3"},
		{"SET", "g", "1.5"},
		{"set", "big", "9007199254740993"},
	}
	if got := rec.got(); !reflect.DeepEqual(got, want) {
		t.Errorf("the server got %q, want %q", got, want)
	}
}

func TestRunErrorReply(t *testing.T) {
	url, rec := server(t, func(args []string) string {
		switch strings.ToUpper(args[0]) {
		case "LPUSH":
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
		case "OBJECT":
			return "-ERR no such key\r\n"
		}
		return "+OK\r\n"
	})
	ret := run(t, map[string]any{
		"url": url,
		"commands": []any{
			[]any{"SET", "k", "v"},
			[]any{"LPUSH", "k", "x"},
			[]any{"object", "encoding", "nope"},
			[]any{"SET", "k2", "v"},
		},
	})

	// The commands after the one that failed are still run.
	if got, want := res(ret)["results"], []any{"OK", nil, nil, "OK"}; !reflect.DeepEqual(got, want) {
		t.Errorf("res.results = %#v, want %#v", got, want)
	}
	wantErrors := []any{
		map[string]any{"index": 1, "command": "LPUSH", "code": "WRONGTYPE", "message": "WRONGTYPE Operation against a key holding the wrong kind of value"},
		map[string]any{"index": 2, "command": "OBJECT ENCODING", "code": "ERR", "message": "ERR no such key"},
	}
	if got := res(ret)["errors"]; !reflect.DeepEqual(got, wantErrors) {
		t.Errorf("res.errors = %#v, want %#v", got, wantErrors)
	}
	if ret["status"] != 1 {
		t.Errorf("status = %v, want 1", ret["status"])
	}
	if len(rec.got()) != 4 {
		t.Errorf("the server got %d commands, want 4", len(rec.got()))
	}
}

// EXEC answers with a list that has the reply to each command of the
// transaction, an error for one that failed.
func TestRunErrorInsideList(t *testing.T) {
	url, _ := server(t, func(args []string) string {
		switch args[0] {
		case "MULTI":
			return "+OK\r\n"
		case "EXEC":
			return "*2\r\n+OK\r\n-WRONGTYPE no\r\n"
		}
		return "+QUEUED\r\n"
	})
	ret := run(t, map[string]any{"url": url, "commands": []any{"MULTI", "SET k v", "LPUSH k x", "EXEC"}})
	if got, want := res(ret)["results"], []any{"OK", "QUEUED", "QUEUED", []any{"OK", nil}}; !reflect.DeepEqual(got, want) {
		t.Errorf("res.results = %#v, want %#v", got, want)
	}
	want := []any{map[string]any{"index": 3, "command": "EXEC", "code": "WRONGTYPE", "message": "WRONGTYPE no"}}
	if got := res(ret)["errors"]; !reflect.DeepEqual(got, want) {
		t.Errorf("res.errors = %#v, want %#v", got, want)
	}
	if ret["status"] != 1 {
		t.Errorf("status = %v, want 1", ret["status"])
	}
}

// A reply of RESP3, which the server sends once HELLO 3 asked for it, is
// read as it is: a map as an object.
func TestRunRESP3(t *testing.T) {
	url, _ := server(t, func(args []string) string {
		switch args[0] {
		case "HELLO":
			return "%2\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n"
		case "HGETALL":
			return "%2\r\n$4\r\nname\r\n$3\r\nAda\r\n$3\r\nage\r\n$2\r\n36\r\n"
		}
		return "_\r\n"
	})
	ret := run(t, map[string]any{"url": url, "commands": []any{"HELLO 3", "HGETALL user", "GET missing"}})
	want := []any{
		map[string]any{"server": "redis", "proto": int64(3)},
		map[string]any{"name": "Ada", "age": "36"},
		nil,
	}
	if got := res(ret)["results"]; !reflect.DeepEqual(got, want) {
		t.Errorf("res.results = %#v, want %#v", got, want)
	}
}

func TestRunAuthAndSelect(t *testing.T) {
	tests := []struct {
		name     string
		url      func(hostport string) string
		with     map[string]any
		setup    [][]string
		shownURL func(hostport string) string
	}{
		{
			name:     "username, password and database in the URL",
			url:      func(h string) string { return "redis://ada:s3cret-pw@" + h + "/2" },
			setup:    [][]string{{"AUTH", "ada", "s3cret-pw"}, {"SELECT", "2"}},
			shownURL: func(h string) string { return "redis://ada@" + h + "/2" },
		},
		{
			name:     "password alone in the URL",
			url:      func(h string) string { return "redis://:s3cret-pw@" + h },
			setup:    [][]string{{"AUTH", "s3cret-pw"}},
			shownURL: func(h string) string { return "redis://" + h },
		},
		{
			name:     "an escaped password in the URL",
			url:      func(h string) string { return "redis://:s3cret%2Fpw%40@" + h },
			setup:    [][]string{{"AUTH", "s3cret/pw@"}},
			shownURL: func(h string) string { return "redis://" + h },
		},
		{
			name:     "username and password in with",
			url:      func(h string) string { return "redis://" + h + "/0" },
			with:     map[string]any{"username": "ada", "password": "s3cret-pw"},
			setup:    [][]string{{"AUTH", "ada", "s3cret-pw"}, {"SELECT", "0"}},
			shownURL: func(h string) string { return "redis://ada@" + h + "/0" },
		},
		{
			name:     "password alone in with",
			url:      func(h string) string { return "redis://" + h },
			with:     map[string]any{"password": "s3cret-pw"},
			setup:    [][]string{{"AUTH", "s3cret-pw"}},
			shownURL: func(h string) string { return "redis://" + h },
		},
		{
			name:     "neither",
			url:      func(h string) string { return "redis://" + h + "/" },
			shownURL: func(h string) string { return "redis://" + h },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, rec := server(t, store())
			hostport := strings.TrimPrefix(url, "redis://")

			var logged bytes.Buffer
			a := &Action{log: hclog.New(&hclog.LoggerOptions{Output: &logged, Level: hclog.Trace})}
			with := map[string]any{"url": tt.url(hostport), "commands": []any{"PING"}}
			for k, v := range tt.with {
				with[k] = v
			}
			ret, err := a.Run(with)
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			want := append(slices.Clone(tt.setup), []string{"PING"})
			if got := rec.got(); !reflect.DeepEqual(got, want) {
				t.Errorf("the server got %v, want %v", got, want)
			}
			if got, want := res(ret)["results"], []any{"PONG"}; !reflect.DeepEqual(got, want) {
				t.Errorf("res.results = %#v, want %#v", got, want)
			}
			if got, want := ret["req"].(map[string]any)["url"], tt.shownURL(hostport); got != want {
				t.Errorf("req.url = %v, want %v", got, want)
			}

			// The password is in neither the result nor the log, whichever
			// way it was given. The log hides with.password by its name.
			for _, secret := range []string{"s3cret-pw", "s3cret%2Fpw%40", "s3cret/pw@"} {
				if strings.Contains(fmt.Sprint(ret), secret) {
					t.Errorf("the result shows the password: %v", ret)
				}
				if strings.Contains(logged.String(), secret) {
					t.Errorf("the log shows the password: %s", logged.String())
				}
			}
			if !strings.Contains(logged.String(), "received request parameters") {
				t.Errorf("the log should have the parameters, got: %s", logged.String())
			}
		})
	}
}

// A server that refuses the AUTH or the SELECT answered, so the step has a
// result, and the commands are not sent.
func TestRunSetupRefused(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		refuse  string
		message string
		code    string
	}{
		{"AUTH", "", "AUTH", "WRONGPASS invalid username-password pair or user is disabled.", "WRONGPASS"},
		{"SELECT", "/99", "SELECT", "ERR DB index is out of range", "ERR"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			url, rec := server(t, func(args []string) string {
				if args[0] == tt.refuse {
					return "-" + tt.message + "\r\n"
				}
				return "+OK\r\n"
			})
			hostport := strings.TrimPrefix(url, "redis://")
			ret := run(t, map[string]any{
				"url":      "redis://ada:s3cret-pw@" + hostport + tt.path,
				"commands": []any{[]any{"SET", "k", "v"}},
			})

			if got := res(ret)["results"]; !reflect.DeepEqual(got, []any{}) {
				t.Errorf("res.results = %#v, want none", got)
			}
			want := []any{map[string]any{"index": -1, "command": tt.refuse, "code": tt.code, "message": tt.message}}
			if got := res(ret)["errors"]; !reflect.DeepEqual(got, want) {
				t.Errorf("res.errors = %#v, want %#v", got, want)
			}
			if ret["status"] != 1 {
				t.Errorf("status = %v, want 1", ret["status"])
			}
			for _, c := range rec.got() {
				if c[0] == "SET" {
					t.Errorf("the commands were sent after %s was refused: %v", tt.refuse, rec.got())
				}
			}
			if strings.Contains(fmt.Sprint(ret), "s3cret-pw") {
				t.Errorf("the result shows the password: %v", ret)
			}
		})
	}
}

func TestRunReadOnly(t *testing.T) {
	url, rec := server(t, store())
	guard := actionrpc.Guard{ReadOnly: true}

	// The whole step is refused, the read before the write too, before the
	// server is connected to.
	tests := []struct {
		commands []any
		want     string
	}{
		{[]any{"GET k", "SET k v"}, "commands[1] SET is not a command known to only read"},
		{[]any{[]any{"del", "k"}}, "commands[0] DEL is not a command known to only read"},
		{[]any{"MULTI", "INCR k", "EXEC"}, "commands[1] INCR is not a command known to only read"},
		{[]any{"EVAL 'return 1' 0"}, "commands[0] EVAL is not a command known to only read"},
		{[]any{"CONFIG GET maxmemory"}, "commands[0] CONFIG is not a command known to only read"},
		{[]any{"OBJECT FREQ k", "CLIENT KILL ID 3"}, "commands[1] CLIENT KILL is not a command known to only read"},
	}
	for _, tt := range tests {
		_, _, err := (&Action{}).RunStep(actionrpc.Call{
			With:  map[string]any{"url": url, "commands": tt.commands},
			Guard: guard,
		})
		if !actionrpc.IsRefused(err) || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("RunStep(%v) error = %v, want a refusal containing %q", tt.commands, err, tt.want)
		}
	}
	if rec.connected() != 0 || len(rec.got()) != 0 {
		t.Errorf("a refused step reached the server: %d connections, %v", rec.connected(), rec.got())
	}

	ret, _, err := (&Action{}).RunStep(actionrpc.Call{
		With:  map[string]any{"url": url, "commands": []any{"PING", "GET k", "MULTI", "GET k", "EXEC"}},
		Guard: guard,
	})
	if err != nil {
		t.Fatalf("RunStep() of reads error = %v", err)
	}
	if got := len(res(ret)["results"].([]any)); got != 5 {
		t.Errorf("a step of reads returned %d results, want 5", got)
	}
}

func TestRunAllowHost(t *testing.T) {
	url, rec := server(t, store())
	hostport := strings.TrimPrefix(url, "redis://")
	_, port, _ := net.SplitHostPort(hostport)
	with := map[string]any{"url": url, "commands": []any{"PING"}}

	for _, allowed := range [][]string{{"127.0.0.1"}, {hostport}, {"example.com", "127.0.0.1:" + port}} {
		if _, _, err := (&Action{}).RunStep(actionrpc.Call{With: with, Guard: actionrpc.Guard{AllowHosts: allowed}}); err != nil {
			t.Errorf("RunStep() under %v error = %v", allowed, err)
		}
	}
	connected := rec.connected()

	for _, allowed := range [][]string{{"example.com"}, {"127.0.0.1:1"}, {"localhost"}} {
		_, _, err := (&Action{}).RunStep(actionrpc.Call{With: with, Guard: actionrpc.Guard{AllowHosts: allowed}})
		if !actionrpc.IsRefused(err) || !strings.Contains(err.Error(), "the host "+hostport+" is not one the run allows") {
			t.Errorf("RunStep() under %v error = %v, want a refusal naming %s", allowed, err, hostport)
		}
	}
	if rec.connected() != connected {
		t.Errorf("a refused step connected to the server")
	}
}

// A URL without a port is checked, and connected to, at the port of Redis.
func TestRunDefaultPort(t *testing.T) {
	var dialed []string
	a := &Action{dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialed = append(dialed, addr)
		return nil, fmt.Errorf("no server in this test")
	}}
	with := map[string]any{"url": "redis://cache.example.com", "commands": []any{"PING"}}

	_, _, err := a.RunStep(actionrpc.Call{With: with, Guard: actionrpc.Guard{AllowHosts: []string{"cache.example.com:6380"}}})
	if !actionrpc.IsRefused(err) || !strings.Contains(err.Error(), "cache.example.com:6379") {
		t.Errorf("RunStep() error = %v, want a refusal naming cache.example.com:6379", err)
	}
	if len(dialed) != 0 {
		t.Errorf("a refused step dialed %v", dialed)
	}

	_, _, err = a.RunStep(actionrpc.Call{With: with, Guard: actionrpc.Guard{AllowHosts: []string{"cache.example.com:6379"}}})
	if err == nil || !strings.Contains(err.Error(), "connecting to cache.example.com:6379: no server in this test") {
		t.Errorf("RunStep() error = %v, want the error of the dial", err)
	}
	if !reflect.DeepEqual(dialed, []string{"cache.example.com:6379"}) {
		t.Errorf("dialed %v, want cache.example.com:6379", dialed)
	}
}

// A node of a cluster answers with where a key is. The action reports that
// as the error it is, and connects nowhere else.
func TestRunFollowsNoRedirect(t *testing.T) {
	other, otherRec := server(t, store())
	url, rec := server(t, func(args []string) string {
		return "-MOVED 3999 " + strings.TrimPrefix(other, "redis://") + "\r\n"
	})
	var dialed []string
	a := &Action{dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialed = append(dialed, addr)
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	ret, err := a.Run(map[string]any{"url": url, "commands": []any{"GET k"}})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	errs := res(ret)["errors"].([]any)
	if len(errs) != 1 || errs[0].(map[string]any)["code"] != "MOVED" || ret["status"] != 1 {
		t.Errorf("res.errors = %v, status = %v, want the MOVED and 1", errs, ret["status"])
	}
	if len(dialed) != 1 || rec.connected() != 1 || otherRec.connected() != 0 {
		t.Errorf("dialed %v, want the server of the URL alone", dialed)
	}
}

func TestRunTimeout(t *testing.T) {
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })
	url, _ := server(t, func(args []string) string {
		if args[0] == "BLPOP" {
			<-hang
		}
		return "+OK\r\n"
	})
	start := time.Now()
	_, err := (&Action{}).Run(map[string]any{
		"url":      url,
		"commands": []any{"PING", "BLPOP queue 0"},
		"timeout":  "200ms",
	})
	if err == nil || !strings.Contains(err.Error(), "commands[1] BLPOP: timed out after 200ms") {
		t.Errorf("Run() error = %v, want a timeout of commands[1]", err)
	}
	if actionrpc.IsRefused(err) {
		t.Errorf("a timeout is not a refusal: %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("the step took %s, want about 200ms", took)
	}
}

func TestRunConnectionFailures(t *testing.T) {
	// A port nothing listens on.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := l.Addr().String()
	_ = l.Close()
	_, err = (&Action{}).Run(map[string]any{"url": "redis://:s3cret-pw@" + closed, "commands": []any{"PING"}})
	if err == nil || !strings.Contains(err.Error(), "connecting to "+closed) {
		t.Errorf("Run() error = %v, want one about connecting to %s", err, closed)
	}
	if err != nil && strings.Contains(err.Error(), "s3cret-pw") {
		t.Errorf("the error shows the password: %v", err)
	}

	// A server that goes away in the middle.
	url, _ := server(t, func(args []string) string {
		if args[0] == "GET" {
			return ""
		}
		return "+OK\r\n"
	})
	_, err = (&Action{}).Run(map[string]any{"url": url, "commands": []any{"PING", "GET k"}})
	if err == nil || !strings.Contains(err.Error(), "commands[1] GET: the server closed the connection before the reply ended") {
		t.Errorf("Run() error = %v, want one about commands[1]", err)
	}

	// Something that is no Redis server.
	url, _ = server(t, func(args []string) string { return "HTTP/1.1 400 Bad Request\r\n\r\n" })
	_, err = (&Action{}).Run(map[string]any{"url": url, "commands": []any{"PING"}})
	if err == nil || !strings.Contains(err.Error(), "no type of RESP") {
		t.Errorf("Run() error = %v, want one about a reply that is not RESP", err)
	}
}

func TestRunTLS(t *testing.T) {
	// The certificate httptest serves with, which names 127.0.0.1 and is
	// signed by no authority the system trusts.
	ts := httptest.NewUnstartedServer(nil)
	ts.StartTLS()
	cert := ts.TLS.Certificates[0]
	trusted := x509.NewCertPool()
	trusted.AddCert(ts.Certificate())
	ts.Close()

	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hostport := inner.Addr().String()
	rec := serve(t, tls.NewListener(inner, &tls.Config{Certificates: []tls.Certificate{cert}}), store())

	for _, scheme := range []string{"rediss", "valkeys"} {
		url := scheme + "://:s3cret-pw@" + hostport + "/1"
		with := map[string]any{"url": url, "commands": []any{"PING"}}

		// Verified against an authority the action is given.
		ret, err := (&Action{tlsConfig: &tls.Config{RootCAs: trusted}}).Run(with)
		if err != nil {
			t.Fatalf("Run(%s) with the certificate trusted error = %v", scheme, err)
		}
		if got, want := res(ret)["results"], []any{"PONG"}; !reflect.DeepEqual(got, want) {
			t.Errorf("res.results = %#v, want %#v", got, want)
		}
		if got, want := ret["req"].(map[string]any)["url"], scheme+"://"+hostport+"/1"; got != want {
			t.Errorf("req.url = %v, want %v", got, want)
		}

		// Refused when it is not trusted, and nothing is sent.
		before := len(rec.got())
		_, err = (&Action{}).Run(with)
		if err == nil || !strings.Contains(err.Error(), "certificate") {
			t.Errorf("Run(%s) with the certificate not trusted error = %v, want one about the certificate", scheme, err)
		}
		if len(rec.got()) != before {
			t.Errorf("commands were sent over a connection that was not verified: %v", rec.got()[before:])
		}

		// Accepted without verifying when the step says so.
		with["insecure_skip_tls"] = true
		if _, err := (&Action{}).Run(with); err != nil {
			t.Errorf("Run(%s) with insecure_skip_tls error = %v", scheme, err)
		}
	}

	// A redis URL does not speak TLS, so the server does not understand it.
	_, err = (&Action{}).Run(map[string]any{"url": "redis://" + hostport, "commands": []any{"PING"}, "timeout": "2s"})
	if err == nil {
		t.Errorf("Run() of a redis URL against a TLS server should fail")
	}
}

func TestParseRequestURL(t *testing.T) {
	tests := []struct {
		url   string
		addr  string
		tls   bool
		db    string
		shown string
	}{
		{"redis://localhost", "localhost:6379", false, "", "redis://localhost:6379"},
		{"redis://localhost:6380/3", "localhost:6380", false, "3", "redis://localhost:6380/3"},
		{"rediss://cache.example.com", "cache.example.com:6379", true, "", "rediss://cache.example.com:6379"},
		{"valkey://localhost/0", "localhost:6379", false, "0", "valkey://localhost:6379/0"},
		{"valkeys://localhost:7000", "localhost:7000", true, "", "valkeys://localhost:7000"},
		{"redis://[::1]", "[::1]:6379", false, "", "redis://[::1]:6379"},
		{"redis://[::1]:7000/1", "[::1]:7000", false, "1", "redis://[::1]:7000/1"},
		{"redis://ada@localhost", "localhost:6379", false, "", "redis://ada@localhost:6379"},
	}
	for _, tt := range tests {
		r, err := parseRequest(map[string]any{"url": tt.url, "commands": []any{"PING"}})
		if err != nil {
			t.Errorf("parseRequest(%s) error = %v", tt.url, err)
			continue
		}
		if r.addr != tt.addr || r.tls != tt.tls || r.db != tt.db || r.shown != tt.shown {
			t.Errorf("parseRequest(%s) = addr %s, tls %v, db %q, shown %s; want %s, %v, %q, %s",
				tt.url, r.addr, r.tls, r.db, r.shown, tt.addr, tt.tls, tt.db, tt.shown)
		}
		if r.timeout != DefaultTimeout {
			t.Errorf("parseRequest(%s) timeout = %s, want %s", tt.url, r.timeout, DefaultTimeout)
		}
	}
}

func TestParseRequestRefuses(t *testing.T) {
	ping := []any{"PING"}
	tests := []struct {
		name string
		with map[string]any
		want string
	}{
		{"unknown key", map[string]any{"url": "redis://localhost", "commands": ping, "comands": ping}, "not comands"},
		{"no url", map[string]any{"commands": ping}, "requires with.url"},
		{"url that is no string", map[string]any{"url": 6379, "commands": ping}, "requires with.url"},
		{"another scheme", map[string]any{"url": "http://ada:s3cret-pw@localhost", "commands": ping}, "must be a redis or rediss URL"},
		{"no scheme", map[string]any{"url": "localhost:6379", "commands": ping}, "must be a redis or rediss URL"},
		{"no host", map[string]any{"url": "redis:///0", "commands": ping}, "must be a redis or rediss URL"},
		{"a unix socket", map[string]any{"url": "unix:///tmp/redis.sock", "commands": ping}, "must be a redis or rediss URL"},
		{"url that cannot be read", map[string]any{"url": "redis://ada:s3cret-pw@localhost:port", "commands": ping}, "is not a URL that can be read"},
		{"query", map[string]any{"url": "redis://:s3cret-pw@localhost?protocol=3", "commands": ping}, "takes no query"},
		{"database by name", map[string]any{"url": "redis://:s3cret-pw@localhost/cache", "commands": ping}, "by its number, as /0, not /cache"},
		{"database with a path", map[string]any{"url": "redis://localhost/0/1", "commands": ping}, "by its number"},
		{"database with a sign", map[string]any{"url": "redis://localhost/+1", "commands": ping}, "by its number"},
		{"username with a URL that has one", map[string]any{"url": "redis://ada:s3cret-pw@localhost", "username": "bob", "commands": ping}, "with.username cannot be given with a URL that has a username or a password"},
		{"password with a URL that has one", map[string]any{"url": "redis://:s3cret-pw@localhost", "password": "other", "commands": ping}, "with.password cannot be given with a URL that has a username or a password"},
		{"password that is no string", map[string]any{"url": "redis://localhost", "password": 1234, "commands": ping}, "with.password must be a string"},
		{"no commands", map[string]any{"url": "redis://localhost"}, "requires with.commands"},
		{"empty commands", map[string]any{"url": "redis://localhost", "commands": []any{}}, "requires with.commands"},
		{"commands that is no list", map[string]any{"url": "redis://localhost", "commands": "PING"}, "requires with.commands"},
		{"command that is an object", map[string]any{"url": "redis://localhost", "commands": []any{map[string]any{"get": "k"}}}, "with.commands[0]: a command must be a list of its words or a string"},
		{"empty command", map[string]any{"url": "redis://localhost", "commands": []any{"PING", "  "}}, "with.commands[1]: a command must start with its name"},
		{"empty list", map[string]any{"url": "redis://localhost", "commands": []any{[]any{}}}, "with.commands[0]: a command must start with its name"},
		{"boolean word", map[string]any{"url": "redis://localhost", "commands": []any{[]any{"SET", "k", true}}}, "with.commands[0]: [2] must be a string or a number, not true"},
		{"null word", map[string]any{"url": "redis://localhost", "commands": []any{[]any{"SET", "k", nil}}}, "with.commands[0]: [2] must be a string or a number"},
		{"list word", map[string]any{"url": "redis://localhost", "commands": []any{[]any{"MGET", []any{"a", "b"}}}}, "with.commands[0]: [1] must be a string or a number"},
		{"quote not closed", map[string]any{"url": "redis://localhost", "commands": []any{`SET k "v`}}, "with.commands[0]: a double quote is not closed"},
		{"AUTH as a command", map[string]any{"url": "redis://localhost", "commands": []any{"PING", "AUTH s3cret-pw"}}, "with.commands[1]: AUTH is not run as a command"},
		{"SUBSCRIBE", map[string]any{"url": "redis://localhost", "commands": []any{"SUBSCRIBE news"}}, "with.commands[0]: SUBSCRIBE is not supported"},
		{"timeout that is no duration", map[string]any{"url": "redis://localhost", "commands": ping, "timeout": "soon"}, "with.timeout"},
		{"timeout that is a list", map[string]any{"url": "redis://localhost", "commands": ping, "timeout": []any{1}}, "with.timeout must be a duration or a number of seconds"},
		{"insecure_skip_tls that is no boolean", map[string]any{"url": "rediss://localhost", "commands": ping, "insecure_skip_tls": "yes"}, "with.insecure_skip_tls must be true or false"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := (&Action{}).Run(tt.with)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Run() error = %v, want one containing %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "s3cret-pw") {
				t.Errorf("the error shows the password: %v", err)
			}
		})
	}
}

func TestParseTimeout(t *testing.T) {
	for in, want := range map[any]time.Duration{
		"10s":        10 * time.Second,
		"250ms":      250 * time.Millisecond,
		5:            5 * time.Second,
		int64(7):     7 * time.Second,
		float64(1.5): 1500 * time.Millisecond,
		0:            0,
	} {
		r, err := parseRequest(map[string]any{"url": "redis://localhost", "commands": []any{"PING"}, "timeout": in})
		if err != nil || r.timeout != want {
			t.Errorf("timeout %v = %v, %v, want %v", in, r.timeout, err, want)
		}
	}
}

func TestSplitWords(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"GET key", []string{"GET", "key"}},
		{"  GET \t key\n", []string{"GET", "key"}},
		{`SET k "hello world"`, []string{"SET", "k", "hello world"}},
		{`SET k 'hello world'`, []string{"SET", "k", "hello world"}},
		{`SET k ""`, []string{"SET", "k", ""}},
		{`SET k ''`, []string{"SET", "k", ""}},
		{`SET k "a\"b\\c\n\t\r"`, []string{"SET", "k", "a\"b\\c\n\t\r"}},
		{`SET k "\x41\xff\xzz"`, []string{"SET", "k", "A\xffxzz"}},
		{`SET k 'it\'s "x" \n'`, []string{"SET", "k", `it's "x" \n`}},
		{`SET k {"a":1}`, []string{"SET", "k", `{"a":1}`}},
		{`KEYS user:*`, []string{"KEYS", "user:*"}},
		{"", nil},
		{"   ", nil},
	}
	for _, tt := range tests {
		got, err := splitWords(tt.in)
		if err != nil || !reflect.DeepEqual(got, tt.want) {
			t.Errorf("splitWords(%q) = %q, %v, want %q", tt.in, got, err, tt.want)
		}
	}

	for in, want := range map[string]string{
		`SET k "v`:     "a double quote is not closed",
		`SET k 'v`:     "a single quote is not closed",
		`SET k "v"x`:   "a closing quote must be followed by a space",
		`SET k 'v'"x"`: "a closing quote must be followed by a space",
	} {
		if got, err := splitWords(in); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("splitWords(%q) = %q, %v, want an error containing %q", in, got, err, want)
		}
	}
}

func TestForLog(t *testing.T) {
	with := map[string]any{"url": "redis://ada:s3cret-pw@localhost:6379/0", "commands": []any{"PING"}}
	got := forLog(with)
	if got["url"] != "redis://ada@localhost:6379/0" {
		t.Errorf("forLog() url = %v", got["url"])
	}
	if with["url"] != "redis://ada:s3cret-pw@localhost:6379/0" {
		t.Errorf("forLog() changed with: %v", with["url"])
	}
	if got := forLog(map[string]any{"url": "redis://ada:s3cret-pw@localhost:port"}); strings.Contains(fmt.Sprint(got), "s3cret-pw") {
		t.Errorf("forLog() of a URL that cannot be read shows the password: %v", got)
	}
}

// TestManifestsDeclare checks that action.yml, and the one the e2e workflows
// use, declare the params and the guard the action has, so that probe
// check and the guard of a run take it as it is. probe manifest keeps what
// action.yml declares in the one of a release.
func TestManifestsDeclare(t *testing.T) {
	for _, name := range []string{"action.yml", "e2e/action/action.yml"} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		m, err := actionref.ParseManifest(data)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !slices.Equal(m.Params, params) {
			t.Errorf("%s declares params %v, want %v", name, m.Params, params)
		}
		if !slices.Equal(m.Guard, keeps) {
			t.Errorf("%s declares guard %v, want %v", name, m.Guard, keeps)
		}
	}
}
