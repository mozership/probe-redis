package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/mozership/probe/actionrpc"
)

// DefaultTimeout bounds a step that does not set with.timeout.
const DefaultTimeout = 30 * time.Second

// defaultPort is the port of a URL that names none.
const defaultPort = "6379"

// setupIndex is the index, in res.errors, of an error the server answered
// the AUTH or the SELECT of the action with, which is no entry of
// with.commands.
const setupIndex = -1

// params are the keys the action takes in with. action.yml declares them,
// for probe check to report a key the action does not take.
var params = []string{"url", "username", "password", "commands", "timeout", "insecure_skip_tls"}

// keeps are the kinds of guard the action keeps to, as action.yml declares
// them: it runs only commands that cannot write under read-only, and
// connects only to a host the run allows.
var keeps = []string{actionrpc.KindReadOnly, actionrpc.KindAllowHost}

// schemes are the schemes of with.url, and whether the connection is over
// TLS.
var schemes = map[string]bool{
	"redis":   false,
	"rediss":  true,
	"valkey":  false,
	"valkeys": true,
}

// Action connects to the Redis or Valkey server a step describes, runs its
// commands in order, and returns the reply to each.
type Action struct {
	log hclog.Logger
	// dial opens the connection. It defaults to a net.Dialer.
	dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// tlsConfig is what the TLS of a rediss URL starts from. It defaults to
	// the system's trusted authorities.
	tlsConfig *tls.Config
}

// Run runs the commands in with, under no guard.
func (a *Action) Run(with map[string]any) (map[string]any, error) {
	ret, _, err := a.RunStep(actionrpc.Call{With: with})
	return ret, err
}

// RunStep runs the commands in call.With under the guard of the run. Once
// the server is connected to, what it answers is a result, an error reply
// included; only a connection that cannot be made or that fails, a
// timeout, or a step the guard refuses, is an error.
func (a *Action) RunStep(call actionrpc.Call) (map[string]any, map[string]any, error) {
	if a.log == nil {
		a.log = hclog.NewNullLogger()
	}
	actionrpc.LogParams(a.log, "received request parameters", forLog(call.With))

	req, err := parseRequest(call.With)
	if err != nil {
		return nil, nil, err
	}
	if err := checkReadOnly(call.Guard, req); err != nil {
		return nil, nil, err
	}
	ret, err := a.do(req, call.Guard)
	actionrpc.LogOutcome(a.log, "redis commands", ret, err)
	return ret, nil, err
}

// forLog returns with as it is logged: the URL without its password. The
// log hides with.password by its name, but not a password inside a URL.
func forLog(with map[string]any) map[string]any {
	raw, ok := with["url"].(string)
	if !ok {
		return with
	}
	shown := "<a URL that cannot be read>"
	if u, err := url.Parse(raw); err == nil {
		if u.User != nil {
			u.User = url.User(u.User.Username())
		}
		shown = u.String()
	}
	logged := make(map[string]any, len(with))
	for k, v := range with {
		logged[k] = v
	}
	logged["url"] = shown
	return logged
}

// checkReadOnly returns a Refused error when the run is read-only and the
// step has a command that is not known to only read. The whole step is
// refused, before the server is connected to.
func checkReadOnly(guard actionrpc.Guard, r *request) error {
	if !guard.ReadOnly {
		return nil
	}
	for i, words := range r.commands {
		if !readOnly(words) {
			return actionrpc.Refuse("the run is read-only, and commands[%d] %s is not a command known to only read", i, name(words))
		}
	}
	return nil
}

type request struct {
	// addr is the host and port to connect to.
	addr string
	// host is the name the certificate of a TLS connection is checked for.
	host string
	tls  bool
	// insecure accepts the certificate of the server without verifying it.
	insecure bool
	// auth has the action send AUTH with username and password. It is set
	// when either is given.
	auth     bool
	username string
	password string
	// db is the database to SELECT, or empty to stay in the one a
	// connection starts in.
	db string
	// shown is the URL without its password, as a result has it.
	shown    string
	commands [][]string
	timeout  time.Duration
}

func parseRequest(with map[string]any) (*request, error) {
	r := &request{timeout: DefaultTimeout}

	for k := range with {
		if !slices.Contains(params, k) {
			return nil, fmt.Errorf("redis action takes %s, not %s", strings.Join(params, ", "), k)
		}
	}

	if err := r.parseURL(with); err != nil {
		return nil, err
	}

	list, ok := with["commands"].([]any)
	if !ok || len(list) == 0 {
		return nil, errors.New("redis action requires with.commands, a list of commands that is not empty")
	}
	for i, c := range list {
		words, err := parseCommand(c)
		if err == nil {
			err = checkSupported(words)
		}
		if err != nil {
			return nil, fmt.Errorf("with.commands[%d]: %w", i, err)
		}
		r.commands = append(r.commands, words)
	}

	if v, ok := with["timeout"]; ok {
		var err error
		if r.timeout, err = parseTimeout(v); err != nil {
			return nil, err
		}
	}

	if v, ok := with["insecure_skip_tls"]; ok && v != nil {
		if r.insecure, ok = v.(bool); !ok {
			return nil, fmt.Errorf("with.insecure_skip_tls must be true or false, not %v", v)
		}
	}

	return r, nil
}

// parseURL reads with.url, and with.username and with.password, which give
// what the URL can also carry. No message of it shows the URL as written,
// which may hold a password.
func (r *request) parseURL(with map[string]any) error {
	raw, _ := with["url"].(string)
	if raw == "" {
		return errors.New("redis action requires with.url")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("with.url is not a URL that can be read; it is written as redis://[username:password@]host[:port][/db]")
	}
	useTLS, ok := schemes[u.Scheme]
	if !ok || u.Hostname() == "" {
		return errors.New("with.url must be a redis or rediss URL with a host, as redis://[username:password@]host[:port][/db]")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return errors.New("with.url takes no query and no fragment")
	}
	r.tls = useTLS
	r.host = u.Hostname()
	port := u.Port()
	if port == "" {
		port = defaultPort
	}
	r.addr = net.JoinHostPort(r.host, port)

	if db := strings.TrimPrefix(u.Path, "/"); db != "" {
		if n, err := strconv.Atoi(db); err != nil || n < 0 || strconv.Itoa(n) != db {
			return fmt.Errorf("with.url must name the database by its number, as /0, not /%s", db)
		}
		r.db = db
	}

	if u.User != nil {
		r.username = u.User.Username()
		r.password, _ = u.User.Password()
	}
	for _, k := range []string{"username", "password"} {
		v, ok := with[k]
		if !ok || v == nil {
			continue
		}
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("with.%s must be a string", k)
		}
		if u.User != nil {
			return fmt.Errorf("with.%s cannot be given with a URL that has a username or a password", k)
		}
		if k == "username" {
			r.username = s
		} else {
			r.password = s
		}
	}

	r.auth = r.username != "" || r.password != ""

	shown := url.URL{Scheme: u.Scheme, Host: r.addr}
	if r.username != "" {
		shown.User = url.User(r.username)
	}
	if r.db != "" {
		shown.Path = "/" + r.db
	}
	r.shown = shown.String()
	return nil
}

// parseCommand reads an entry of with.commands: a list of the words of a
// command, or a string that is split into them.
func parseCommand(c any) ([]string, error) {
	var words []string
	switch v := c.(type) {
	case string:
		var err error
		if words, err = splitWords(v); err != nil {
			return nil, err
		}
	case []any:
		for i, w := range v {
			s, err := word(w)
			if err != nil {
				return nil, fmt.Errorf("[%d] %w", i, err)
			}
			words = append(words, s)
		}
	default:
		return nil, fmt.Errorf("a command must be a list of its words or a string, not %T", c)
	}
	if len(words) == 0 || words[0] == "" {
		return nil, errors.New("a command must start with its name")
	}
	return words, nil
}

// word returns a word of a command written as a list: a string as it is,
// and a number as its digits. Redis has no boolean and no null, so those
// are not guessed at.
func word(w any) (string, error) {
	switch v := w.(type) {
	case string:
		return v, nil
	case int:
		return strconv.Itoa(v), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		if math.IsInf(v, 0) || math.IsNaN(v) {
			return "", fmt.Errorf("must be a string or a number, not %v", v)
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	}
	return "", fmt.Errorf("must be a string or a number, not %v; quote it to send it as a string", w)
}

// splitWords splits a command written as a string into its words, as
// redis-cli does: at spaces, except inside quotes. Double quotes take the
// escapes \n, \r, \t, \b, \a, \\, \" and \xHH; single quotes take \' alone.
func splitWords(s string) ([]string, error) {
	var words []string
	i := 0
	for {
		for i < len(s) && isSpace(s[i]) {
			i++
		}
		if i == len(s) {
			return words, nil
		}

		var w []byte
		switch s[i] {
		case '"':
			i++
			for {
				if i == len(s) {
					return nil, errors.New("a double quote is not closed")
				}
				if s[i] == '"' {
					i++
					break
				}
				if s[i] == '\\' && i+1 < len(s) {
					i++
					switch s[i] {
					case 'n':
						w = append(w, '\n')
					case 'r':
						w = append(w, '\r')
					case 't':
						w = append(w, '\t')
					case 'b':
						w = append(w, '\b')
					case 'a':
						w = append(w, '\a')
					case 'x':
						if i+2 < len(s) {
							if b, err := strconv.ParseUint(s[i+1:i+3], 16, 8); err == nil {
								w = append(w, byte(b))
								i += 2
								break
							}
						}
						w = append(w, 'x')
					default:
						w = append(w, s[i])
					}
					i++
					continue
				}
				w = append(w, s[i])
				i++
			}
		case '\'':
			i++
			for {
				if i == len(s) {
					return nil, errors.New("a single quote is not closed")
				}
				if s[i] == '\'' {
					i++
					break
				}
				if s[i] == '\\' && i+1 < len(s) && s[i+1] == '\'' {
					i++
				}
				w = append(w, s[i])
				i++
			}
		default:
			for i < len(s) && !isSpace(s[i]) {
				w = append(w, s[i])
				i++
			}
			words = append(words, string(w))
			continue
		}
		// A quote closes a word, so nothing but a space can follow it.
		if i < len(s) && !isSpace(s[i]) {
			return nil, errors.New("a closing quote must be followed by a space or the end of the command")
		}
		words = append(words, string(w))
	}
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}

// parseTimeout accepts a duration string such as "10s" or a number of
// seconds, as the http action does.
func parseTimeout(v any) (time.Duration, error) {
	switch t := v.(type) {
	case string:
		d, err := time.ParseDuration(t)
		if err != nil {
			return 0, fmt.Errorf("with.timeout: %w", err)
		}
		return d, nil
	case int:
		return time.Duration(t) * time.Second, nil
	case int64:
		return time.Duration(t) * time.Second, nil
	case float64:
		return time.Duration(t * float64(time.Second)), nil
	default:
		return 0, fmt.Errorf("with.timeout must be a duration or a number of seconds, not %T", v)
	}
}

func (a *Action) do(r *request, guard actionrpc.Guard) (map[string]any, error) {
	// The server named by the URL is the only one the action connects to:
	// it follows no redirect of a cluster and looks up no other node.
	if err := guard.CheckHost(r.addr); err != nil {
		return nil, err
	}

	ctx := context.Background()
	if r.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	commands := make([]any, len(r.commands))
	for i, words := range r.commands {
		list := make([]any, len(words))
		for j, w := range words {
			list[j] = w
		}
		commands[i] = list
	}
	reqInfo := map[string]any{"url": r.shown, "commands": commands}

	start := time.Now()
	conn, err := a.connect(ctx, r)
	if err != nil {
		return nil, r.failure(ctx, "connecting to "+r.addr, err)
	}
	defer func() { _ = conn.Close() }()
	c := newClient(conn)

	results := []any{}
	failures := []any{}

	// A server that refuses the AUTH or the SELECT answered, so that is a
	// result. The commands would run as someone else or in another
	// database, so they are not sent.
	setup := [][]string{}
	if r.auth {
		auth := []string{"AUTH", r.password}
		if r.username != "" {
			auth = []string{"AUTH", r.username, r.password}
		}
		setup = append(setup, auth)
	}
	if r.db != "" {
		setup = append(setup, []string{"SELECT", r.db})
	}
	for _, words := range setup {
		reply, err := c.do(words...)
		if err != nil {
			return nil, r.failure(ctx, words[0], err)
		}
		if e, ok := reply.(replyError); ok {
			failures = append(failures, errorEntry(setupIndex, words[0], e))
			return result(reqInfo, results, failures, time.Since(start)), nil
		}
	}

	for i, words := range r.commands {
		reply, err := c.do(words...)
		if err != nil {
			return nil, r.failure(ctx, fmt.Sprintf("commands[%d] %s", i, name(words)), err)
		}
		results = append(results, plain(reply, func(e replyError) {
			failures = append(failures, errorEntry(i, name(words), e))
		}))
	}
	return result(reqInfo, results, failures, time.Since(start)), nil
}

// connect opens the connection, over TLS for a rediss URL, and has it end
// when the step times out.
func (a *Action) connect(ctx context.Context, r *request) (net.Conn, error) {
	dial := a.dial
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	conn, err := dial(ctx, "tcp", r.addr)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	if !r.tls {
		return conn, nil
	}

	cfg := &tls.Config{}
	if a.tlsConfig != nil {
		cfg = a.tlsConfig.Clone()
	}
	cfg.ServerName = r.host
	cfg.InsecureSkipVerify = r.insecure
	secured := tls.Client(conn, cfg)
	if err := secured.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return secured, nil
}

// failure is the error of a step that could not talk to the server: what
// it was doing, and why it stopped.
func (r *request) failure(ctx context.Context, doing string, err error) error {
	if ctx.Err() != nil || errors.Is(err, os.ErrDeadlineExceeded) {
		return fmt.Errorf("%s: timed out after %s", doing, r.timeout)
	}
	return fmt.Errorf("%s: %w", doing, err)
}

// errorEntry is an entry of res.errors: the error the server answered a
// command with.
func errorEntry(index int, command string, e replyError) map[string]any {
	return map[string]any{
		"index":   index,
		"command": command,
		"code":    e.code(),
		"message": string(e),
	}
}

// plain returns a reply as a value of a result. An error has no place
// among values, so it becomes null and is handed to report: the reply to a
// command, or one inside a list, as EXEC answers for a command of a
// transaction that failed.
func plain(v any, report func(replyError)) any {
	switch t := v.(type) {
	case replyError:
		report(t)
		return nil
	case []any:
		list := make([]any, len(t))
		for i, e := range t {
			list[i] = plain(e, report)
		}
		return list
	case map[string]any:
		m := make(map[string]any, len(t))
		for k, e := range t {
			m[k] = plain(e, report)
		}
		return m
	}
	return v
}

// result returns what the step sees. Its status is 0 only when the server
// answered no command with an error.
func result(req map[string]any, results, failures []any, rt time.Duration) map[string]any {
	status := 0
	if len(failures) > 0 {
		status = 1
	}
	return map[string]any{
		"req": req,
		"res": map[string]any{
			"results": results,
			"errors":  failures,
		},
		"rt":     rt.String(),
		"status": status,
	}
}
