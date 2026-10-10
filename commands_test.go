package main

import (
	"strings"
	"testing"
)

func TestReadOnly(t *testing.T) {
	reads := []string{
		"GET k", "get k", "MGET a b", "EXISTS k", "TTL k", "TYPE k", "STRLEN k", "GETRANGE k 0 1",
		"HGETALL h", "HGET h f", "LRANGE l 0 -1", "SMEMBERS s", "ZRANGE z 0 -1 WITHSCORES",
		"XRANGE x - +", "XREAD COUNT 1 STREAMS x 0", "SCAN 0", "HSCAN h 0", "KEYS *", "DBSIZE",
		"PING", "ECHO hi", "TIME", "INFO", "INFO server", "SELECT 1", "HELLO 3",
		"OBJECT ENCODING k", "object encoding k", "MEMORY USAGE k", "XINFO STREAM x",
		"CLIENT ID", "CLIENT LIST", "COMMAND", "COMMAND COUNT", "PUBSUB CHANNELS",
		"SORT_RO l", "BITFIELD_RO k GET u8 0", "GEOSEARCH g FROMMEMBER m BYRADIUS 1 km",
		"MULTI", "EXEC", "DISCARD", "WATCH k", "UNWATCH",
	}
	for _, c := range reads {
		if !readOnly(strings.Fields(c)) {
			t.Errorf("%q should be run under read-only", c)
		}
	}

	writes := []string{
		"SET k v", "set k v", "DEL k", "UNLINK k", "EXPIRE k 10", "PERSIST k", "RENAME a b",
		"INCR k", "APPEND k v", "GETSET k v", "GETDEL k", "GETEX k EX 10", "SETRANGE k 0 v",
		"HSET h f v", "HDEL h f", "LPUSH l v", "RPOP l", "BLPOP l 1", "LMOVE a b LEFT RIGHT",
		"SADD s m", "SPOP s", "SINTERSTORE d a b", "ZADD z 1 m", "ZRANGESTORE d z 0 -1", "ZPOPMIN z",
		"XADD x * f v", "XREADGROUP GROUP g c STREAMS x >", "XACK x g 1-0", "XGROUP CREATE x g $",
		"SORT l", "SORT l STORE d", "BITFIELD k SET u8 0 1", "BITOP AND d a b",
		"GEOADD g 0 0 m", "GEOSEARCHSTORE d g FROMMEMBER m BYRADIUS 1 km", "GEORADIUS g 0 0 1 km",
		"PFADD p a", "PFCOUNT p", "TOUCH k", "COPY a b", "MOVE k 1", "RESTORE k 0 x",
		"EVAL return_1 0", "EVAL_RO return_1 0", "EVALSHA abc 0", "FCALL f 0", "FCALL_RO f 0",
		"FUNCTION LOAD x", "FUNCTION LIST", "SCRIPT FLUSH", "SCRIPT EXISTS abc",
		"CONFIG SET a b", "CONFIG GET a", "FLUSHALL", "FLUSHDB", "SWAPDB 0 1", "SHUTDOWN",
		"DEBUG SLEEP 1", "SAVE", "BGSAVE", "REPLICAOF NO ONE", "SLAVEOF NO ONE", "MIGRATE h 1 k 0 1",
		"PUBLISH c m", "SPUBLISH c m", "ACL SETUSER u", "ACL LIST", "CLUSTER NODES",
		"OBJECT", "OBJECT FLUSH", "MEMORY PURGE", "MEMORY", "XINFO", "CLIENT KILL ID 1",
		"CLIENT SETNAME n", "CLIENT", "COMMAND FLUSH", "PUBSUB", "RESET", "QUIT", "WAIT 1 0",
		"FT.SEARCH i q", "JSON.SET k $ 1", "NOSUCHCOMMAND",
	}
	for _, c := range writes {
		if readOnly(strings.Fields(c)) {
			t.Errorf("%q should not be run under read-only", c)
		}
	}
}

// The commands are looked up in upper case, so a name written otherwise in
// a list would never match.
func TestCommandListsAreUpperCase(t *testing.T) {
	lists := map[string]map[string]bool{"readCommands": readCommands, "streamCommands": streamCommands}
	for n, subs := range readSubcommands {
		lists["readSubcommands["+n+"]"] = subs
		if n != strings.ToUpper(n) {
			t.Errorf("readSubcommands has %q, which is not upper case", n)
		}
	}
	for list, names := range lists {
		for n := range names {
			if n != strings.ToUpper(n) || strings.ContainsAny(n, " \t") {
				t.Errorf("%s has %q, which is not one word in upper case", list, n)
			}
		}
	}
}

func TestCheckSupported(t *testing.T) {
	for _, c := range []string{"GET k", "SET k v", "HELLO 3", "HELLO 3 SETNAME probe", "CLIENT ID", "CLIENT NO-EVICT on", "PUBLISH c m", "QUIT"} {
		if err := checkSupported(strings.Fields(c)); err != nil {
			t.Errorf("checkSupported(%q) error = %v", c, err)
		}
	}
	for c, want := range map[string]string{
		"SUBSCRIBE c":            "SUBSCRIBE is not supported",
		"psubscribe c*":          "PSUBSCRIBE is not supported",
		"SSUBSCRIBE c":           "SSUBSCRIBE is not supported",
		"MONITOR":                "MONITOR is not supported",
		"SYNC":                   "SYNC is not supported",
		"PSYNC ? -1":             "PSYNC is not supported",
		"CLIENT REPLY OFF":       "CLIENT REPLY is not supported",
		"client reply skip":      "CLIENT REPLY is not supported",
		"AUTH secret":            "AUTH is not run as a command",
		"auth user secret":       "AUTH is not run as a command",
		"HELLO 3 AUTH u secret":  "HELLO is not run with AUTH",
		"hello 3 auth u secret2": "HELLO is not run with AUTH",
	} {
		err := checkSupported(strings.Fields(c))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("checkSupported(%q) error = %v, want one containing %q", c, err, want)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("checkSupported(%q) error shows the password: %v", c, err)
		}
	}
}

func TestName(t *testing.T) {
	for c, want := range map[string]string{
		"get k":             "GET",
		"object encoding k": "OBJECT ENCODING",
		"CLIENT":            "CLIENT",
		"config set a b":    "CONFIG",
	} {
		if got := name(strings.Fields(c)); got != want {
			t.Errorf("name(%q) = %q, want %q", c, got, want)
		}
	}
}
