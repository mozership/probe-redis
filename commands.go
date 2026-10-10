package main

import (
	"errors"
	"fmt"
	"strings"
)

// readCommands are the commands that are run under read-only: those that
// cannot write, whatever their arguments. A command that reads in one form
// and writes in another, as SORT does with STORE, is not among them, and
// nor is one that changes the server or what its clients see.
var readCommands = set(
	// Keys.
	"EXISTS", "TYPE", "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME",
	"KEYS", "SCAN", "DBSIZE", "RANDOMKEY", "DUMP", "SORT_RO",
	// Strings and bits.
	"GET", "MGET", "STRLEN", "GETRANGE", "SUBSTR", "LCS",
	"GETBIT", "BITCOUNT", "BITPOS", "BITFIELD_RO",
	// Hashes.
	"HGET", "HMGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HEXISTS",
	"HSTRLEN", "HSCAN", "HRANDFIELD",
	"HTTL", "HPTTL", "HEXPIRETIME", "HPEXPIRETIME",
	// Lists.
	"LRANGE", "LLEN", "LINDEX", "LPOS",
	// Sets.
	"SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD", "SRANDMEMBER", "SSCAN",
	"SINTER", "SUNION", "SDIFF", "SINTERCARD",
	// Sorted sets.
	"ZRANGE", "ZRANGEBYSCORE", "ZRANGEBYLEX",
	"ZREVRANGE", "ZREVRANGEBYSCORE", "ZREVRANGEBYLEX",
	"ZSCORE", "ZMSCORE", "ZCARD", "ZCOUNT", "ZLEXCOUNT",
	"ZRANK", "ZREVRANK", "ZSCAN", "ZRANDMEMBER",
	"ZINTER", "ZUNION", "ZDIFF", "ZINTERCARD",
	// Streams.
	"XRANGE", "XREVRANGE", "XLEN", "XREAD", "XPENDING",
	// Geo.
	"GEOPOS", "GEODIST", "GEOHASH", "GEOSEARCH",
	"GEORADIUS_RO", "GEORADIUSBYMEMBER_RO",
	// The connection and the server.
	"PING", "ECHO", "TIME", "INFO", "SELECT", "HELLO",
	// A transaction, each command of which is checked like any other.
	"MULTI", "EXEC", "DISCARD", "WATCH", "UNWATCH",
)

// readSubcommands are the commands that are run under read-only with some
// of their subcommands only. An empty subcommand stands for the command
// without one.
var readSubcommands = map[string]map[string]bool{
	"OBJECT":  set("ENCODING", "FREQ", "IDLETIME", "REFCOUNT", "HELP"),
	"MEMORY":  set("USAGE"),
	"XINFO":   set("STREAM", "GROUPS", "CONSUMERS", "HELP"),
	"CLIENT":  set("ID", "GETNAME", "INFO", "LIST"),
	"COMMAND": set("", "COUNT", "INFO", "DOCS", "LIST", "GETKEYS", "GETKEYSANDFLAGS", "HELP"),
	"PUBSUB":  set("CHANNELS", "NUMSUB", "NUMPAT", "SHARDCHANNELS", "SHARDNUMSUB", "HELP"),
}

// streamCommands are the commands that are never run: the server answers
// them with a stream of messages, or stops answering, where the action
// reads one reply to each command.
var streamCommands = set(
	"SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "MONITOR", "SYNC", "PSYNC",
)

func set(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}

// name returns what a command is called in a message: its name in upper
// case, with its subcommand when it is one of those that have them.
func name(words []string) string {
	n := strings.ToUpper(words[0])
	if _, ok := readSubcommands[n]; ok && len(words) > 1 {
		return n + " " + strings.ToUpper(words[1])
	}
	return n
}

// checkSupported returns an error for a command the action never runs,
// whatever the guard.
func checkSupported(words []string) error {
	n := strings.ToUpper(words[0])
	switch {
	case streamCommands[n]:
		return fmt.Errorf("%s is not supported: the server does not answer it with one reply", n)
	case n == "CLIENT" && len(words) > 1 && strings.EqualFold(words[1], "REPLY"):
		return errors.New("CLIENT REPLY is not supported: the action reads a reply to every command")
	case n == "AUTH":
		return errors.New("AUTH is not run as a command: give the username and the password in with.url, or in with.username and with.password")
	case n == "HELLO":
		for _, w := range words[1:] {
			if strings.EqualFold(w, "AUTH") {
				return errors.New("HELLO is not run with AUTH: give the username and the password in with.url, or in with.username and with.password")
			}
		}
	}
	return nil
}

// readOnly reports whether a command is one that is run under read-only.
func readOnly(words []string) bool {
	n := strings.ToUpper(words[0])
	if subs, ok := readSubcommands[n]; ok {
		sub := ""
		if len(words) > 1 {
			sub = strings.ToUpper(words[1])
		}
		return subs[sub]
	}
	return readCommands[n]
}
