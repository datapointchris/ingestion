# ingestion

ingestion takes items in from outside sources: exports, feeds, playlists, watched folders, and
links or memos pushed from a phone. It delivers each item once, to the one place it belongs.

On the way it parses each item and scans it for content aimed at whatever reads it next. Then it
gates the item by quality and by side. A model judges only what a rule has not already decided.

## Install

```bash
go install github.com/datapointchris/ingestion@latest
```

Or download the linux or darwin archive for your machine from the
[releases page](https://github.com/datapointchris/ingestion/releases), and put `ingestion` on
your `PATH`.

## First run

```bash
ingestion --version   # prints the version, which proves the binary runs
ingestion version     # adds the commit and the date it was built from
ingestion update      # replaces the binary with the latest release
ingestion --help      # lists every command, and each command answers --help
```

## Configuration

ingestion reads `$XDG_CONFIG_HOME/ingestion/config.toml`, which is
`~/.config/ingestion/config.toml` when the variable is unset. A model stage calls a model only
through the adapter this file names. To run every model stage through the Claude Code CLI on the
same machine:

```toml
[model]
# claude-cli runs `claude -p` with no tools, none of the machine's own
# settings, and no permission prompts. The stage's JSON schema constrains
# the answer.
# queue posts the same prompt and schema to an HTTP dispatch service instead.
adapter = "claude-cli"
```

With no adapter named, ingestion still lands, parses, scans and routes every item by its rules.
An item that needs a model then waits at that stage, and its status names the missing setting.

## Running the service and its workers

`ingestion serve` holds the store and answers the API. `ingestion worker run` leases tasks from
it. The CLI's other commands are clients of that API. All three read the same config file, and
`serve` and `worker run` each take `--config <path>` to read another.

```toml
# The OAuth issuer whose access tokens the API accepts. serve refuses to start
# without one.
issuer = "https://auth.example.com/realms/home"
# The service's address, for the CLI and the worker.
api = "https://ingestion.example.com"
# The client the CLI logs in as, whose id starts ingestion-cli-. A worker names
# its service client instead, whose id starts ingestion-svc-.
client_id = "ingestion-cli-laptop"
# Optional: a command that prints an OAuth token response, used in place of a
# login.
# token_command = ["my-token-helper", "ingestion"]

[serve]
listen = ":8080"
state_dir = "/var/lib/ingestion"
# The address callers reach the API at. With it, serve publishes RFC 9728
# metadata, and `ingestion auth login <public_url>` finds the issuer there.
public_url = "https://ingestion.example.com"
# The OTLP/HTTP logs endpoint the outbox ships every change to. With none,
# serve keeps every record and ships nothing. Set it only once the collector
# routes ingestion.record and ingestion.text each to a table of its own that
# folds on kind, id and version. Once the collector accepts a record, the
# outbox moves past it, so a record filed elsewhere first never ships again.
records_otlp_url = "http://collector.example.com:4318/v1/logs"

# Each named root's directory. A file under a root lands by reference, as
# <root>:<path below it>, rather than as a copy in the blob volume.
[serve.homes]
books = "/srv/books"

[worker]
slots = 4     # tasks leased at once
lease = "10m" # how long a lease holds without a heartbeat
poll = "10s"  # how often an idle worker asks for work
```

`ingestion serve --help` and `ingestion worker run --help` print each setting's default.

- **`INGESTION_CLIENT_SECRET`** is the worker's client secret. It makes the worker authenticate
  with the client-credentials grant. The worker requires it, or a `token_command`.
- **`NODE_NAME`**, where set, names the node in each shipped record's resource.
- **The access token decides what a caller reaches.** A person's token reaches every route. A
  service token reaches only the routes its scopes grant. A worker needs `ingestion.tasks.claim`,
  which also reads and writes bytes through `/api/v1/blobs/{sha256}`. A scan task reads its item's
  parse through `/api/v1/tasks/{id}/parse`, under the task's lease. A service that reads items
  needs `ingestion.items.read`, and one that runs a stage again needs `ingestion.items.replay`. A
  job that applies declarations needs `ingestion.declarations.apply`, one that reads connectors
  `ingestion.connectors.read`, one that runs them `ingestion.connectors.run`, and a source that
  drops files `ingestion.items.drop`. Releasing a connector's first fill spends the model stages on
  every item it held, so it takes `ingestion.first-fills.release`, which no other scope grants.

`ingestion auth login`, `auth status` and `auth logout` hold a person's login. `ingestion tasks
list` reads the service's open tasks and the workers it has heard from:

```text
TASK                              ITEM              STAGE  CAPABILITY         STATE   ATTEMPTS  LEASE  UPDATED
8a35d7545c3cb9c4cfdf1bb5128fddfd  reddit/t3_aq7ggd  parse  parse:thread-text  queued  0         —      2026-11-24 21:13:50 EST

WORKER                                     VERSION  CAPABILITIES       STARTED                  LAST HEARTBEAT
ingestion-worker-7c9d8f6b4-x2kqp-0f683526  0.4.0    parse:thread-text  2026-11-24 21:13:50 EST  2026-11-24 21:13:53 EST
```

`--state` narrows the list to one state and repeats, `--limit` caps it, and `--json` prints the
shape in `schemas/tasks-list.schema.json`. The list shows each worker heard from lately, and
`--all-workers` lists every worker the service holds, since each process start is a worker of its
own. A command exits 4 when the service gives no answer, and 2 when a flag is wrong.

### Reading items and their bytes

`ingestion items list` lists items, newest first, and `ingestion items show <key or url>` shows
one with every record it holds. A URL resolves to the key of the artifact it names, so a Reddit
or YouTube link reaches its post or video:

```text
$ ingestion items show https://www.reddit.com/r/golang/comments/aq7ggd/a_post/
reddit/t3_aq7ggd   outside   landed
  url      https://www.reddit.com/r/golang/comments/aq7ggd/a_post/
  parent   file/5d41402abc4b2a76
  sighted  reddit-exports  outside  2026-11-24 21:13:50 EST  tags none  external t3_aq7ggd
  landed   2026-11-24 21:13:50 EST  application/x-reddit-save+json  blob 9f86d081884c  1.2 KiB  md5 …
```

A title is outsider text where the zone is `outside`. `items list --keys` prints the keys alone,
one per line, so a script or a session can work through items without reading one. `--limit`
bounds the list, and `ingestion items list --help` prints its default. `--json` prints the shapes
in `schemas/items-list.schema.json` and `schemas/item.schema.json`. `items show` sends a URL to
the service, which resolves it to its key.

The service keeps each item's bytes under their SHA-256 in `blobs/sha256/<first two>/<rest>` below
`state_dir`, with their MD5 beside them. A file under a `[serve.homes]` root is read where it lies
instead. Its size and modification time are recorded when it lands. A file whose size or time
differs is hashed again before it is served, and is not served where its bytes changed.
`ingestion blobs unpaired` names each blob in the volume that no row names, and each row whose
bytes are missing. It exits 3 where it names any, and 1 where it could not compare. It deletes
nothing.

### A person applies each connector as a declaration kept in git

Each source enters through a connector declared in a directory kept in git:

```text
connectors/<name>.yml   one connector each, in a file named for it
routing-rules.yml       rules: the rules that name a destination for what they match
destinations.yml        destinations: each place an item is delivered
```

`examples/declarations/` holds a set to start from. `schemas/connector.schema.json`,
`schemas/routing-rules.schema.json` and `schemas/destinations.schema.json` hold each file's shape,
for an editor to check against. A connector names these:

| Field | Holds |
| --- | --- |
| `name` | the connector's name, which each item it lands is sighted under |
| `mode` | `poll`, `push`, `folder` or `drop` |
| `source` | the external feed it reads, by its name in the data catalog |
| `contract` | optional: the data contract its records hold to |
| `zone` | who wrote what it carries: `outside` for an outsider, `owner` for the owner's own words |
| `tags` | the tags every item it lands carries |
| `read_rule` | optional: `as-is` or `model`, in place of the side's default |
| `schedule` | a poll's or a folder's cron expression, a descriptor such as `@hourly`, or `@every 6h`, read in UTC unless it opens `CRON_TZ=<zone>` |
| `freshness` | optional, for a poll or a folder: how long it may go without a delivery before it reads overdue |
| `due_interval` | a drop's or a push's expected cadence |
| `root`, `path` | a folder's `[serve.homes]` root, and the directory below it to scan |
| `format` | optional, for a drop: the registered handler that reads its file, which otherwise lands as one item |

`freshness` and `due_interval` are a positive count of minutes, hours, days or weeks, such as
`90m`, `12h`, `30d` or `2w`. A day is 24 hours.

A routing rule is named, and its match holds a `connector`, a `tag`, or a source `field` with the
`values` it takes, such as a subreddit. Every condition it names must hold. A destination names
its `side`, `personal` or `fleet`, and the `tags` that place an item there. `apply` checks and
keeps the rules and destinations. No stage reads them, so no item is placed by a rule.

`ingestion connectors apply <dir>` reads the whole set, checks each file against its schema, and
sends it to the service, which keeps it beside its commit. The commit is the directory's `HEAD`,
marked `+dirty.<digest>` where the directory holds an uncommitted change. A directory holding none
of the three files is refused, and the command exits 2, so a wrong path never empties the set in
force. A set holding any invalid declaration is refused whole. The refusal names each one with
every reason, and the set in force stays as it was:

```text
$ ingestion connectors apply ./declarations
the set holds invalid declarations, so none of it is applied
  connector stream
    mode: value must be one of 'poll', 'push', 'folder', 'drop'
    zone: value must be one of 'outside', 'owner'
  routing rule to-nowhere
    destination nowhere is not one destinations.yml declares
```

It exits 3 on a refusal.

An apply names the commit it replaces, which it reads from the service first. Where that commit
came from git, the directory's `HEAD` has to contain it, so a checkout that was not pulled cannot
replace a newer set. The service refuses an apply whose set in force moved after it was read, with
409 `declarations_moved`. Both exit 3. `--replacing <commit>` replaces the commit in force
whatever `HEAD` holds, such as for a rollback to an older commit.

`ingestion connectors list` prints each connector in force:

```text
     CONNECTOR    MODE  APPLIED         LAST DELIVERY  ITEMS  WAITING  REVIEW  FAILED                                       DUE
  books-folder  folder  5519f33                     —      0        0       0       0  */15 * * * * · next 2026-11-25 00:00 EST
   feed-reader    poll  5519f33                     —      0        0       0       0       @hourly · next 2026-11-25 00:00 EST
reddit-exports    drop  5519f33  2026-11-24 23:50 EST      1        0       0       0     every 30d · next 2026-12-24 23:50 EST
```

`ingestion connectors show <name>` adds its source, its tags, its last run and its first fill.
`--json` on either prints the shape in `schemas/connectors-list.schema.json`.

- **The service schedules each connector.** A poll or a folder is due at its schedule's next time.
  A drop or a push is due its `due_interval` after its last delivery, and reads overdue once that
  passes with nothing delivered. `ingestion connectors run <name>` makes a poll or a folder due
  now.
- **A failed run is kept and tried again sooner.** The cursor keeps the error, its time, and the
  runs that failed in a row. The next try waits 5 minutes, doubling after each failure, and comes
  no later than the schedule's next time. `list` marks the failures in DUE, and `show` prints the
  last error. A run that succeeds clears all three.
- **A poll's run is a task with no item.** Its stage is `poll` and its capability is
  `poll:<source>`, so only a worker that handles that source leases it. The task names its
  connector, and a connector never has two polls in flight.
- **The service scans a folder itself**, since only it holds the `[serve.homes]` roots. Each new
  file lands once by reference. A file whose size and time match its landing is skipped, so a
  rerun lands nothing. A file modified in the last 10 seconds is left for the next scan, so a copy
  in progress does not land half written. A scan stops after 30 seconds and the folder stays due,
  so a large first scan resumes on the next pass rather than holding up every other connector.
- **A drop connector takes a file a person or a job sends.** `ingestion items drop <file>
  --connector <name>` posts it to `/api/v1/connectors/<name>/drops` under an `Idempotency-Key`. A
  retry under that key answers the first landing, and another file under it is refused with 409.
  The CLI derives the key from the connector, the file's name, its media type and its bytes, so
  dropping a file twice lands it once, and a copy under another name is one more sighting of the
  same item. A connector whose `format` names a registered handler gives the file to it, and any
  other lands the file as one item.
- **A connector's first fill runs the first of its items to reach a model stage, and holds the
  rest there.** `show` prints that item and its tokens by stage. `ingestion connectors release
  <name>` sends `PATCH /api/v1/connectors/<name>/first-fill` with `{"state": "released"}`, which
  queues every model task the fill held. The hold lives in the store, so it survives a restart. A
  connector that a later set dropped can still be released. An item that two connectors sighted
  waits while either holds it.

### Parsing an item, and running a stage again

Landing a new item queues its parse in the same write. A worker runs the extractor registered for
the item's media type, which writes the item's text as numbered units. `thread-text` reads a saved
Reddit post or comment: the post first, then each comment depth first, each unit labeled with
its author. A deleted or removed comment is left out, and its replies are kept.

`GET /api/v1/items/{key}/parse` answers the newest parse: its text, its units and the scan's
findings, in `schemas/item-parse.schema.json`. The text is the units joined by a blank line.
`?extractor=` and `?version=` read an older parse, and every version stays readable. An extractor
that finds no text fails its task with the reason, and the item waits in review.

`ingestion items replay --stage <stage>` runs a stage again over the items that `--connector` and
`--key` select, or over every item. A new extractor version writes its parse beside the old one.

```text
$ ingestion items replay --stage parse --connector reddit-exports
Queued parse on 12 items.
```

The command posts to `POST /api/v1/replays` under one `Idempotency-Key`, so a retry queues
nothing twice. The answer is `schemas/replay.schema.json`. The service refuses a stage that it
holds no replay for with `stage_not_replayable`. It refuses a `--key` that names no item with
`item_not_found`, and a `--connector` that sighted no item with `no_item_from_connector`. A
replay at an unchanged extractor version retries the items whose parse failed. An item whose
parse is done keeps it, even where the rerun fails.

### Scanning an item for what it suggests and what it hides

A parse that is done queues the item's scan in the same write that reports it. A worker runs
YARA-X's `yr` over the parse's units, joined as the parse's text joins them, with two rule sets:

- Agent Threat Rules at a pinned commit, converted to YARA. A converted rule is kept only where
  `yr` compiles it and it passes the true positives and true negatives that ATR ships with it.
- ingestion's own rules in `scan/ingestion-rules/`. They list the commands, code and links an
  item suggests, and find Unicode that hides text from a person reading it.

`yr` reads the text as written, and again as ATR's own engine reads it: after NFKC, with every
zero-width and bidirectional control removed and each Cyrillic or Greek letter that looks Latin
folded to that letter. So `Ignоre all previous instructions` with a Cyrillic `о` matches as
ATR's engine would match it. A rule marked `suppress_in_code_blocks` does not count a match inside
markdown code.

A finding names its rule, the rule's severity, the unit it starts in, its byte offset and length
in that unit, and the span it matched, each in the text as written. It reads through the parse
route, and through `ingestion items show`, which prints each span quoted with any hidden character
escaped. A finding never drops an item. A scan that fails runs again, up to three runs, then fails
its task, and the item's phase reads `failed` until a later scan of it is done.

`ingestion items replay --stage scan` scans the selected items again, as a new rule set needs. A
scan by new rules, or of a new parse, writes beside the old one. A scan that is done keeps its
findings, even where a rerun fails.

The image builds the rules once, and a worker reads them from
`/usr/local/share/ingestion/scan-rules`. A worker with no `yr` or no built rules claims no scan
task, and logs why. To build them by hand:

```text
release/install-tools ./bin yr
release/download-atr-rules ./atr
go run ./scan/build-rules --atr ./atr --yr ./bin/yr --out ./scan-rules
```

`build-rules` prints the rules' version, how many rules it kept, and each reason it dropped a rule
with how many it dropped for that reason. `rules.json` beside the compiled rules records the
decision on each rule.

### Running it in a container

Each release publishes the image `ghcr.io/datapointchris/ingestion:<version>`. It runs `serve`
by default, and `worker run` when given those arguments. The store and its blobs live on a volume at
`/var/lib/ingestion`, and the config is read from `/etc/ingestion/config.toml`. The image runs as
uid and gid 10600.

`deploy/` holds the Kubernetes manifests: a StatefulSet of one for `serve`, and a Deployment for
`worker run`. Each release publishes them as the OCI artifact
`ghcr.io/datapointchris/ingestion/manifests:<version>`, with the image pinned by digest. Both are
signed with cosign by the release workflow. The manifests leave two things for the cluster to
supply: the ConfigMap `ingestion-config` holding `config.toml`, and the Secret
`ingestion-worker-client` holding the worker's client secret under `secret`.

**An upgrade that changes a table copies the store first.** Before `serve` changes the store's
tables, it writes a copy beside the store, named `ingestion.db.before-<UTC time>`. A column that
SQLite cannot change in place, such as one a release makes nullable, is rebuilt in one transaction
that keeps every row. `serve` leaves alone a table that its own schema files do not create, so a
table that a later release added does not stop an earlier one opening the store. v0.5.0 and
earlier refuse such a table. A rollback to one of them stops `serve`, puts the copy back as
`ingestion.db`, and starts the older release. The copy holds nothing landed after the upgrade, and
stays until a person deletes it.

### Every change leaves through the outbox

Each table logs its own change in the transaction that writes it, and the outbox ships every
change past its mark as an OTLP log record. A row's keys, states, hashes, versions, tags, zones
and decisions go out as an `ingestion.record`. Its outsider-written text goes out as an
`ingestion.text` with the same kind, id and version, so a reader can be granted one without the
other. `schemas/` holds the shape of each.

Delivery is at least once. A restart ships from each table's mark, so a clean restart sends
nothing twice. After a crash, a reader folds a resent record on its kind, id and version.

## How it works

Every item passes one line of stages:

```text
land → parse → scan → quality gate → side → read → model read → deliver
```

- **An item's key names the artifact, not the source that sent it.** The same article from a feed
  and from a phone is one item, read once and delivered once. Each source's arrival is a sighting
  of that item.
- **A rule decides before a model does.** A source's tags or a routing rule place an item first.
  The quality gate's model reads only what the rules leave.
- **A drop is soft.** A dropped item keeps its row and its one-line idea, so a person can still
  send it on.
- **The zone records who wrote the content.** `outside` marks what an outsider wrote, such as a
  page, a post, a transcript or a book, even when you share the link. `owner` marks your own
  words, such as a voice memo. An item takes the strictest zone among its sightings. Outsider text
  is shipped apart from everything else, so a reader can be granted one without the other.
- **A model stage holds no tools.** It reads the prompt and answers the stage's schema, and
  nothing else.
- **`ingestion serve` is the one writer, and `ingestion worker run` does the heavy work.** The
  service holds the SQLite store and the stored bytes. A worker leases tasks by capability under a
  fencing token, and moves bytes through the service, so a heavy extractor never shares memory
  with the writer.
