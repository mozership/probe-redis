# probe-redis

A [Probe](https://github.com/mozership/probe) action that runs commands on a Redis or Valkey server and returns their replies, so a workflow can check what an API left in a cache or a session store.

Probe downloads it the first time a workflow uses it. It needs Probe v1.21.0 or later, which reads the guard and the params the action declares in `action.yml`.

```yaml
name: Session
jobs:
- name: session
  steps:
  - name: Sign in
    id: login
    uses: http
    with:
      url: https://api.example.com
      post: /login
      body: {user: ada, password: "{{vars.password}}"}
    test: res.code == 200
    outputs:
      session: res.body.session_id

  - name: The session is in the store, and expires
    uses: github.com/mozership/probe-redis@<commit SHA>
    with:
      url: redis://cache.example.com:6379/0
      password: "{{vars.redis_password}}"
      commands:
      - [HGET, "session:{{outputs.login.session}}", user]
      - [TTL, "session:{{outputs.login.session}}"]
    test: status == 0 && res.results[0] == "ada" && res.results[1] > 0
```

Probe only takes a full 40-character commit SHA. The notes of each [release](https://github.com/mozership/probe-redis/releases) start with the `uses` line to copy.

Every step opens a new connection, runs its `commands` in order, and closes it, so what belongs to a connection does not carry over to the next step: put a transaction, or a `HELLO 3` and the commands that follow it, in one step.

## Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `url` | String | Yes | - | The server, as `redis://[username:password@]host[:port][/db]`. `rediss` connects over TLS, and `valkey` and `valkeys` are other names for the two. The port defaults to `6379`, and `db` is the number of the database to select |
| `username` | String | No | - | The user to authenticate as, when the URL names none |
| `password` | String | No | - | The password, when the URL has none. See [Passwords](#passwords) |
| `commands` | List | Yes | - | The commands to run, in order. See [Commands](#commands) |
| `timeout` | Duration | No | `30s` | Time limit for the whole step, from connecting to the last reply, as `10s` or a number of seconds. `0` removes it |
| `insecure_skip_tls` | Boolean | No | `false` | Accept the certificate of a `rediss` server without verifying it. The connection is still encrypted, but no longer protected against an impostor, so use it only for servers you control |

A key of `with` that is not one of these fails the step before anything is sent, and `probe check` reports it with its line.

When a username or a password is given, in the URL or in `with`, the action authenticates with `AUTH` before the commands, and then selects the database of the URL, if it names one.

The action connects to the one server the URL names. It does not follow the `MOVED` and `ASK` redirects of a cluster or look up the nodes of one, and it does not ask a sentinel for a master: such a redirect is an error reply like any other.

### Commands

Each entry of `commands` is one command, in either of two forms.

| Form | Example | How it is read |
|---|---|---|
| List | `[SET, greeting, hello world]` | Each entry is one word of the command: a string as it is, and a number as its digits. `true`, `false` and `null` are refused, as Redis has no such values; quote them to send them as strings |
| String | `SET greeting "hello world"` | Split into words at spaces, as `redis-cli` does. Double quotes keep spaces and take the escapes `\n`, `\r`, `\t`, `\b`, `\a`, `\\`, `\"` and `\xHH`; single quotes keep everything but `\'` |

The list form needs no quoting, so it is the one to use for a value that comes from a template.

These commands are never run:

- `AUTH`, and `HELLO` with `AUTH`: the username and the password are given in `url`, or in `username` and `password`, so that they are not shown with the commands.
- `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `MONITOR`, `SYNC`, `PSYNC` and `CLIENT REPLY`: the server does not answer them with one reply, which is what the action reads for each command.

### Passwords

Give a password in `password` rather than in the URL. Probe hides the value of a `password` parameter wherever it shows a step, in its output, its reports and its logs. It does not know a password inside `url`: the action leaves it out of `req.url`, of its errors and of its own log, but the verbose log of Probe (`-v`) shows the parameters of a step as they were written, the URL included, unless the password is one of the workflow's `secrets`.

## Result

| Field | Type | Description |
|---|---|---|
| `res.results` | Array | The reply to each command, in the order of `commands`. See below |
| `res.errors` | Array | The errors the server answered with, described below; empty when there are none |
| `req` | Object | The `url`, without its password and with its port, and the `commands`, each as the list of words that was sent |
| `rt` | Duration | Time from connecting to the last reply |
| `status` | Integer | `0` when the server answered no command with an error; `1` otherwise |

A reply becomes a plain value.

| Reply | Value |
|---|---|
| A string, such as that of `GET`, or a status, such as `OK` | String |
| An integer, such as that of `INCR` or `TTL` | Number |
| Nothing, such as `GET` of a key that does not exist | `null` |
| A list, such as that of `LRANGE` or `MGET`, or a set | Array, each entry a value of this table |
| A map, a double, a boolean or a big number, which a server sends after `HELLO 3` | Object, number, boolean, and the digits as a string |
| An error | `null`, and an entry in `res.errors` |

A string that is not UTF-8 cannot be a string of a result, so it becomes an object with its bytes in base64, as `{"base64": "//4="}`. An integer beyond 2<sup>53</sup> loses its last digits on the way to a test, as any number does there.

Without `HELLO 3` the server answers in RESP2, in which a map is a list of keys and values in turn: `HGETALL` of a hash with `name` and `age` is `["name", "Ada", "age", "36"]`. Put `HELLO 3` before it in the same step to have it as `{"name": "Ada", "age": "36"}`.

Each entry of `res.errors` has these fields.

| Field | Type | Description |
|---|---|---|
| `index` | Integer | Which entry of `commands` the server answered with the error, from `0`. `-1` for the `AUTH` or the `SELECT` the action sends itself |
| `command` | String | The name of the command, in upper case, such as `LPUSH` or `OBJECT ENCODING` |
| `code` | String | The first word of the error, which names its kind: `ERR`, `WRONGTYPE`, `NOAUTH`, `WRONGPASS`, `NOPERM`, `MOVED` and so on |
| `message` | String | The whole error, as the server sent it |

Once the server is connected to, what it answers is a result, so a test can assert on an error:

```yaml
- name: The key is not a list
  uses: github.com/mozership/probe-redis@<commit SHA>
  with:
    url: redis://localhost:6379
    commands:
    - [LPUSH, greeting, x]
  test: status == 1 && res.errors[0].code == "WRONGTYPE"
```

An error does not stop the step: the commands after it are run, and their replies are in `res.results` at their own index. An error inside a list, as `EXEC` answers for a command of a transaction that failed, is `null` there and an entry of `res.errors` with the index of the command that returned the list.

When the server refuses the `AUTH` or the `SELECT` of the action, the commands are not sent: `res.results` is empty and `res.errors` has the one error, with the index `-1`.

Only a step that cannot talk to the server fails as an error: a connection that is refused, a certificate that is not trusted, a server that closes the connection or answers with something that is not a reply, a reply larger than the action reads (a string of 16 MiB, or a list of 1,048,576 entries), or a timeout.

## Guard

The action keeps to the guard of the run, and its `action.yml` declares `guard: [read-only, allow-host]`, so Probe runs it under `--read-only` and `--allow-host` without `--allow-action`.

- Under `--read-only`, a step is run only when every one of its commands is known to only read. Otherwise the whole step is refused before connecting, and the refusal names the command.
- Under `--allow-host`, the host and the port of `url` must be one the run allows. A URL without a port is taken at `6379`. The action connects to no other host.

A refused step fails with the kind `refused`.

The commands known to only read are these. A command that reads in one form and writes in another, as `SORT` does with `STORE`, is not among them, and nor are scripts and functions, whose effect cannot be told from the command: use the `_RO` form where there is one.

| Group | Commands |
|---|---|
| Keys | `EXISTS`, `TYPE`, `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `KEYS`, `SCAN`, `DBSIZE`, `RANDOMKEY`, `DUMP`, `SORT_RO`, `OBJECT ENCODING`, `OBJECT FREQ`, `OBJECT IDLETIME`, `OBJECT REFCOUNT`, `MEMORY USAGE` |
| Strings and bits | `GET`, `MGET`, `STRLEN`, `GETRANGE`, `SUBSTR`, `LCS`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITFIELD_RO` |
| Hashes | `HGET`, `HMGET`, `HGETALL`, `HKEYS`, `HVALS`, `HLEN`, `HEXISTS`, `HSTRLEN`, `HSCAN`, `HRANDFIELD`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME` |
| Lists | `LRANGE`, `LLEN`, `LINDEX`, `LPOS` |
| Sets | `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SRANDMEMBER`, `SSCAN`, `SINTER`, `SUNION`, `SDIFF`, `SINTERCARD` |
| Sorted sets | `ZRANGE`, `ZRANGEBYSCORE`, `ZRANGEBYLEX`, `ZREVRANGE`, `ZREVRANGEBYSCORE`, `ZREVRANGEBYLEX`, `ZSCORE`, `ZMSCORE`, `ZCARD`, `ZCOUNT`, `ZLEXCOUNT`, `ZRANK`, `ZREVRANK`, `ZSCAN`, `ZRANDMEMBER`, `ZINTER`, `ZUNION`, `ZDIFF`, `ZINTERCARD` |
| Streams | `XRANGE`, `XREVRANGE`, `XLEN`, `XREAD`, `XPENDING`, `XINFO STREAM`, `XINFO GROUPS`, `XINFO CONSUMERS` |
| Geo | `GEOPOS`, `GEODIST`, `GEOHASH`, `GEOSEARCH`, `GEORADIUS_RO`, `GEORADIUSBYMEMBER_RO` |
| Connection and server | `PING`, `ECHO`, `TIME`, `INFO`, `SELECT`, `HELLO`, `CLIENT ID`, `CLIENT GETNAME`, `CLIENT INFO`, `CLIENT LIST`, `COMMAND` and its `COUNT`, `INFO`, `DOCS`, `LIST`, `GETKEYS` and `GETKEYSANDFLAGS`, `PUBSUB CHANNELS`, `PUBSUB NUMSUB`, `PUBSUB NUMPAT`, `PUBSUB SHARDCHANNELS`, `PUBSUB SHARDNUMSUB` |
| Transactions | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, each command between them checked like any other |

`OBJECT`, `MEMORY`, `XINFO`, `CLIENT`, `COMMAND` and `PUBSUB` also take `HELP`. The commands of a module, such as `JSON.GET` or `FT.SEARCH`, are not known to the action, so a step that uses one has to be allowed with `--allow-action`.

## Testing

`go test ./...` runs against a stub server. `e2e/workflow.yml` runs the action through Probe against a real one, Redis or Valkey:

```sh
docker run -d --rm -p 6379:6379 redis:8
go build -o e2e/action/probe-redis . && probe e2e/workflow.yml
```

`REDIS_URL` names another server. `e2e/guard.yml` is run under `--read-only` and under `--allow-host`, where its steps are refused.

## Releasing

Pushing a `v*` tag builds the executables with GoReleaser and publishes them on the release. The workflow then has `probe manifest` write an `action.yml` with their URLs and SHA-256 digests, commits it to `main`, and adds the commit to the release notes. The name, the description, `guard` and `params` are those of the `action.yml` at the tag, so that file is where to change them. That commit is the one to pin: Probe reads `action.yml` at the pinned commit and refuses an executable whose digest differs.

## License

MIT
