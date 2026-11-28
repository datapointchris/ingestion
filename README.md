# ingestion

ingestion takes items in from outside sources: exports, feeds, playlists, watched folders, and
links or memos pushed from a phone. It delivers each item once, to the one place it belongs.

On the way it parses each item and scans it for content aimed at whatever reads it next. Then it
gates the item by quality and by side. A model judges only what a rule has not already decided.
An item on the fleet side, about the software you build and run, is read whole by a model, which
answers your note on it with a summary and a suggested change. Where you wrote no note, a model
drafts one from the item's opening first.

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
ingestion             # opens `ingestion tui` at a terminal, and prints the help with none
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

[model.claude-cli]
command = "claude"  # a path, or a name on PATH

# What the contents stage asks each run for.
[model.contents]
model = "sonnet"
effort = "medium"

# What the topics stage asks each run for.
[model.topics]
model = "sonnet"
effort = "medium"

# What the quality gate asks each run for.
[model.quality]
model = "haiku"
effort = "medium"

# What the note's draft asks each run for.
[model.note]
model = "haiku"
effort = "medium"

# What the model read asks each run for.
[model.model-read]
model = "haiku"
effort = "medium"

# What a prompt proposal's weekly draft asks its run for.
[model.prompt-proposal]
model = "sonnet"
effort = "medium"

# What the read of a video's top comments asks its run for.
[model.comments]
model = "haiku"
effort = "medium"
```

With no adapter named, ingestion still lands, parses, scans and routes every item by its rules.
An item that needs a model then waits at that stage, and its status names the missing setting.

The `queue` adapter posts each call to a dispatch service as one run of an agent with no tools,
and reads the run's answer once it ends:

```toml
[model]
adapter = "queue"

[model.queue]
api = "https://dispatch.example.com"
agent = "outside-text"           # the kind of run the service starts
client_id = "dispatch-svc-ingestion"
# token_command = ["my-token-helper", "dispatch"]  # in place of client_id
# issuer = "..."  # the OIDC provider that signs the service's tokens, if not the file's
```

The secret of the `client_id` client is in **`INGESTION_MODEL_QUEUE_CLIENT_SECRET`**. Its token
asks for the scopes that the adapter's routes need, which `modelport.QueueScopes` names and
`ingestion worker run --help` prints. A worker logs a `[model]` table that cannot open and runs on
without the model stages.

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

# serve reads it: the playlist that a sort by watch adds a video to where the
# person picks none. With none, such a watch is refused.
[watch]
default_playlist = "Watch soon"

[worker]
slots = 4     # tasks leased at once
lease = "10m" # how long a lease holds without a heartbeat
poll = "10s"  # how often an idle worker asks for work

# Optional: how many tasks run at once, within slots, so a long task leaves the
# other slots to every other stage. A key names a capability, or a slot group
# whose capabilities share one bound. Docling and Whisper parses share the group
# heavy, whose default extract.SlotGroups holds, named here at that default. A grouped capability takes its group's bound alone. A capability
# that nothing caps may take every slot.
[worker.slot_limits]
heavy = 1

# A table for each deliverer the worker opens, named for it. This one saves
# items as life articles, as a client that holds the scope life.articles.write.
[deliver.life-articles]
api = "https://life.example.com"
client_id = "life-svc-ingestion"
# token_command = ["my-token-helper", "life"]  # in place of client_id
# issuer = "..."  # the OIDC provider that signs life's tokens, if not the one above

# This one creates each PDF book, and each video, as a resource in learning, as
# a client that holds the scope learning.resources.write. It writes a book by
# its file, so serve reads root too: the [serve.homes] root that each book's
# path is below.
[deliver.learning-resources]
api = "https://learning.example.com"
client_id = "learning-svc-ingestion"
root = "books"

# A table for each source whose stages the worker runs, named for the source.
# It takes api, client_id, token_command and issuer as a [deliver.<name>]
# table does. This one reads YouTube through ypl, as a client that holds the
# scopes that `ingestion worker run --help` names for ypl. The ypl-watch
# deliverer writes through the same client, so it takes no table of its own.
[source.ypl]
api = "https://ypl.example.com"
client_id = "ypl-svc-ingestion"

# A source that signs in as a person names api and login instead. This one
# reads a FreshRSS user's reading list through its Google Reader API, with the
# login's API password in INGESTION_SOURCE_FRESHRSS_PASSWORD.
[source.freshrss]
api = "https://rss.example.com/api/greader.php"
login = "someone"

# A table for each lens whose context the worker reads, named for the lens. It
# takes api, client_id, token_command and issuer as a [deliver.<name>] table
# does, and names the context reader this build registers. This one reads a
# slate tracker's focus and active initiatives, as a client that holds the
# scope slate.records.read.
[lens.fleet.context]
reader = "slate"
api = "https://slate.example.com"
client_id = "slate-svc-ingestion"
```

`ingestion serve --help` and `ingestion worker run --help` print each setting's default.

- **`INGESTION_CLIENT_SECRET`** is the worker's client secret. It makes the worker authenticate
  with the client-credentials grant. The worker requires it, or a `token_command`.
- **`INGESTION_DELIVER_<NAME>_CLIENT_SECRET`** is the secret of the client that a
  `[deliver.<name>]` table names by `client_id`, such as
  `INGESTION_DELIVER_LIFE_ARTICLES_CLIENT_SECRET`. The worker logs a table that cannot open, such
  as one whose secret is unset, and runs on without that deliverer. Its other stages are
  unaffected.
- **`INGESTION_SOURCE_<NAME>_CLIENT_SECRET`** is the secret of the client that a
  `[source.<name>]` table names by `client_id`, such as `INGESTION_SOURCE_YPL_CLIENT_SECRET`. The
  worker runs the stages that the source registers only where its table opens, and leaves that
  source's polls and deletes queued otherwise.
- **`INGESTION_SOURCE_<NAME>_PASSWORD`** is the password of the login that a `[source.<name>]`
  table names, for a source that signs in as a person, such as
  `INGESTION_SOURCE_FRESHRSS_PASSWORD`. Where it is unset, the table does not open, and the
  worker leaves that source's polls queued.
- **`INGESTION_LENS_<NAME>_CONTEXT_CLIENT_SECRET`** is the secret of the client that a
  `[lens.<name>.context]` table names by `client_id`, such as
  `INGESTION_LENS_FLEET_CONTEXT_CLIENT_SECRET`. The worker logs a table that cannot open, and the
  note and the model read under that lens run without its context.
- **`NODE_NAME`**, where set, names the node in each shipped record's resource.
- **The access token decides what a caller reaches.** A person's token reaches every route. A
  service token reaches only the routes its scopes grant. A worker needs `ingestion.tasks.claim`,
  which also reads and writes bytes through `/api/v1/blobs/{sha256}`. A scan task reads its item's
  parse through `/api/v1/tasks/{id}/parse`, under the task's lease. A service that reads items,
  or the gates' agreement with each placement, needs `ingestion.items.read`, and one that runs a
  stage again needs `ingestion.items.replay`. A
  job that applies declarations needs `ingestion.declarations.apply`, one that reads connectors
  `ingestion.connectors.read`, one that runs them `ingestion.connectors.run`, and a source that
  drops files `ingestion.items.drop`. To push links and memos, such as from a phone, a client
  needs `ingestion.items.push`. That scope reaches the push route alone. An app that shows the review queue needs
  `ingestion.review.read`, which reads each waiting item's title, URL, quality gate answer and
  sightings' fields without `ingestion.items.read`. One that places items from it for a person
  needs `ingestion.review.place`. One that marks an item `low-quality` or `read` for a person
  needs `ingestion.marks.write`, and one that promotes a gate family `ingestion.trust.promote`.
  One that asks for the read of a video's top comments needs `ingestion.comments.read`.
  One that holds a connector needs `ingestion.connectors.hold`. A release spends the model
  stages on every item that the connector's first fill held. So releasing takes
  `ingestion.connectors.release`, and no other scope grants it.

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
own. A command exits 4 when the service gives no answer. It exits 2 when a flag is wrong, or when
it names an item that the service holds no record of.

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

Each listed item carries its outcome, where its decisions leave it, beside the decision that left
it there:

- `placed` on a side at a destination, even where its delivery or model read failed
- `archived` by a person, with no destination
- `dropped` with its cause, by the gate, a rule or a person
- `split`, a memo whose topics each carry on
- `waiting` for a decision, as an item in review or at a model stage does

`items list --outcome <outcome>` and `GET /api/v1/items?outcome=<outcome>` list only the items of
one. The service first reads each row whose stored phase can give the outcome. It then keeps the
items whose records give it. So a page holds `--limit` items of the outcome wherever they sit.
The decision's `decider` names who made it: `person`, `rule`, `tag`, `gate`, `service`, or `book`
for a chapter that takes its book's decision.

The service keeps each item's bytes under their SHA-256 in `blobs/sha256/<first two>/<rest>` below
`state_dir`, with their MD5 beside them. A file under a `[serve.homes]` root is read where it lies
instead. Its size and modification time are recorded when it lands. A file whose size or time
differs is hashed again before it is served, and is not served where its bytes changed.
`ingestion blobs unpaired` names each blob in the volume that no row names, and each row whose
bytes are missing. It exits 3 where it names any, and 1 where it could not compare. It also names
each row older than an hour that no item, home or output names, such as a page that a poll kept
for an item that already held its bytes. Those set no exit code. It deletes nothing.

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
| `source` | what it reads: for a poll, the source that this build registers, such as `ypl` or `freshrss`, and otherwise the external feed by its name in the data catalog |
| `contract` | optional: the data contract its records hold to |
| `zone` | who wrote what it carries: `outside` for an outsider, `owner` for the owner's own words |
| `tags` | the tags every item it lands carries |
| `read_rule` | optional: `as-is` or `model`, in place of the side's default |
| `schedule` | a poll's or a folder's cron expression, a descriptor such as `@hourly`, or `@every 6h`, read in UTC unless it opens `CRON_TZ=<zone>`. A poll may say `once` instead |
| `after_landing` | optional, for a poll: `delete` removes each item it lands from its source, through the deleter that its source registers |
| `person_sorts` | optional: `true` leaves the side of what it lands to a person, with the quality gate's guess shown beside it |
| `source_parameters` | optional: the source's own settings as names and values, such as the playlist that a `ypl` connector reads. The source names the keys it takes and checks each, and a set that names any for a source that takes none is refused |
| `freshness` | optional, for a poll or a folder: how long it may go without a delivery before it reads overdue |
| `due_interval` | a drop's or a push's expected cadence |
| `root`, `path` | a folder's `[serve.homes]` root, and the directory below it to scan |
| `format` | optional, for a drop: the registered handler that reads its file, which otherwise lands as one item. The connector's `zone` is the one the format lands |

`freshness` and `due_interval` are a positive count of minutes, hours, days or weeks, such as
`90m`, `12h`, `30d` or `2w`. A day is 24 hours.

A routing rule is named, and its match holds a `connector`, a `tag`, or a source `field` with the
`values` it takes, such as a subreddit. Every condition it names must hold on one sighting. A
destination names its `side`, `personal` or `fleet`, and the `tags` that place an item there. A
destination with no side drops what it takes. The side gate reads the rules and destinations of
the set in force.

A destination's `deliverer` names the code that writes each item placed there into its app, such
as `life-articles`. A destination with none keeps its items in ingestion for a reader to pull.
`default_for_side: true` makes a destination take each item placed on its side with no
destination named. The quality gate's guess places an item that way, and so do tags naming two
destinations on one side. A side holds one default at most. A set naming a deliverer that the
build does not carry is refused.

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
     CONNECTOR    MODE  APPLIED         LAST DELIVERY  ITEMS  WAITING  REVIEW  FAILED             HELD                                       DUE
  books-folder  folder  5519f33                     —      0        0       0       0  held · 0 tasks  */15 * * * * · next 2026-11-25 00:00 EST
   feed-reader    poll  5519f33                     —      0        0       0       0               —       @hourly · next 2026-11-25 00:00 EST
reddit-exports    drop  5519f33  2026-11-24 23:50 EST      1        0       0       0               —     every 30d · next 2026-12-24 23:50 EST
```

`ingestion connectors show <name>` adds its source, its tags, its last run, a person's hold and
its first fill.
`--json` on either prints the shape in `schemas/connectors-list.schema.json`. REVIEW counts what
`ingestion review list` lists, including an item whose parse failed. FAILED counts an item whose
newest task past its parse failed.

- **The service schedules each connector.** A poll or a folder is due at its schedule's next time.
  A drop or a push is due its `due_interval` after its last delivery, and reads overdue once that
  passes with nothing delivered. `ingestion connectors run <name>` makes a poll or a folder due
  now.
- **A failed run is kept and tried again sooner.** The cursor keeps the error, its time, and the
  runs that failed in a row. The next try waits 5 minutes, doubling after each failure, and comes
  no later than the schedule's next time. `list` marks the failures in DUE, and `show` prints the
  last error. A run that succeeds clears all three. A poll's run succeeds or fails when its worker
  reports. A poll whose lease lapses three times in a row with no report accepted fails too.
- **A poll's run is a task with no item.** Its stage is `poll` and its capability is
  `poll:<source>`, so only a worker that handles that source leases it. The task names its
  connector, and a connector never has two polls in flight. The lease hands the worker the
  connector's cursor and its declaration. A claim ends a poll that the set in force no longer
  runs, such as one of a connector it dropped, rather than lease it.
- **The service lands what a poll's worker reports, in the write that ends its lease.** A done
  report carries each item that the poll found past the cursor, and the cursor to keep. Each
  arrival lands once under its key, sighted under the connector with the tags and zone that the
  set in force declares for it. An arrival's `metrics`, such as a video's views, are counts by
  name that ride on its sighting. A later poll's value of a name replaces the one kept.
- **An arrival that cannot land fails its poll and holds its cursor, and the rest land.** Such an
  arrival has a metric named in other than lowercase snake case, for example, or a URL that
  derives no key. The poll's task and the cursor keep the first refusal, and the failure counts
  toward the backoff. The cursor stays where it was, so the next poll reads the refused item again
  rather than passing over it. A report for a connector that the set in force dropped lands
  nothing and fails its poll.
- **A poll scheduled `once` runs until one poll lands everything it found**, then never again.
  Its worker reports done only once every item that its source holds has been reported. A poll
  that fails tries again after the backoff. A once connector whose poll's lease lapsed is due again
  after a fixed recheck interval. `list` reads `once · finished` with its time, and a finished
  connector never reads overdue. Once an apply declares it on a schedule instead, it loses its
  finished time and reads stale and overdue as any poll does.
- **`after_landing: delete` queues one `delete` task for each item that a poll lands**, in the same
  write. Its capability is `delete:<source>`, and its lease hands the worker the sighting to
  remove. The source registers its deleter, and a set naming `delete` for a source without one is
  refused. A worker opens that deleter from its `[source.<name>]` table. A delete that fails is
  queued again after a backoff that doubles with each attempt, up to a cap. A wait that the source
  asks for, such as until its quota resets, takes the place of that backoff. A poll queues no
  delete of an item while a delete of it from the same connector is queued or leased. A claim ends
  a delete whose connector the set in force no longer declares `after_landing: delete` for, rather
  than lease it. A delete task sits on no item's line, so it never moves an item's phase.
- **The service scans a folder itself**, since only it holds the `[serve.homes]` roots. Each new
  file lands once by reference. A file whose size and time match its landing is skipped, so a
  rerun lands nothing. A file modified in the last 10 seconds is left for the next scan, so a copy
  in progress does not land half written. A scan stops after 30 seconds and the folder stays due,
  so a large first scan resumes on the next pass rather than holding up every other connector.
- **A file that leaves the folder keeps its item.** After each listing, the scan marks gone every
  home under the connector's path that the listing lacks. The item keeps its parse and its
  deliveries. A listing that finds no files marks nothing, because a share that failed to mount
  lists nothing too. An item that lands with no title takes its file name, without the extension.
- **A drop connector takes a file a person or a job sends.** `ingestion items drop <file>
  --connector <name>` posts it to `/api/v1/connectors/<name>/drops` under an `Idempotency-Key`. A
  retry under that key answers the first landing, and another file under it is refused with 409.
  The CLI derives the key from the connector, the file's name, its media type and its bytes, so
  dropping a file twice lands it once, and a copy under another name is one more sighting of the
  same item. A connector whose `format` names a registered handler gives the file to it, and any
  other lands the file as one item. A file that the handler refuses answers 422 and lands nothing.
  A drop that stops partway, such as on a full volume, answers 500, and dropping the file again
  lands the rest.
- **A push connector takes a link or a memo that a person pushes.** `POST /api/v1/items` reads
  `{"connector": "<name>", "url": "<link>", "destination": "<name>"}` under an `Idempotency-Key`,
  and answers as a drop does. The link lands as an `outside` item under its key, and the
  connector's tags go on its sighting. A retry under that key answers the first landing, and
  another link under it is refused with 409. The route sets each item's zone by what the push
  carries. It lands a link outside, and a memo as the owner's. So an apply refuses a push
  connector that declares a `zone`.
  - **A memo's recording is the body itself**, sent to `POST /api/v1/items?connector=<name>`
    with any Content-Type, as an iPhone Shortcut sends a file. The route reads the body's opening:
    a `{` is a link's JSON, and an M4A, MP3 or WAV recording is a memo. Any other body answers
    415 `media_not_read`, and one past 100 MiB answers 413 `memo_too_large`. An optional
    `recorded_at`, in RFC 3339, is the memo's source time.
  - **A memo lands as the owner's**, one item in the `owner` zone under `memo/<hash>`. The hash
    names the caller and the `Idempotency-Key`, so a retry lands on the first attempt's item. The
    route streams the recording to the blob volume under its SHA-256, and queues its parse at
    `audio-whisper`. Under one key, a retry answers the first landing, whatever `recorded_at` it
    names. Another recording under that key answers 409.
  - **A link lands only where an extractor of this build reads it at its source, or fetches its
    page.** `youtube-captions` reads a YouTube video's link. Any other link to a web page keys as
    `url/<hash>`, and lands as `text/html` with no bytes. The worker's `article-trafilatura` parse
    then GETs the page, as a FreshRSS poll does. The service keeps the page as the item's bytes,
    and the page's own title as the item's where it holds none. The title is the page's
    `og:title`, else its `<title>`, else its first `<h1>`, with a trailing site name cut. The
    service stores nothing that it did not fetch itself. A Reddit post's link answers 422
    `link_not_read`, naming what the build lands, and stores nothing. So does a link on a video
    site that names no single video, such as a channel's page.
  - **When the link's page is a PDF, the item becomes a PDF**, and `pdf-text` parses it. The
    parse that GET it keeps it, and ends deferred. When the page is any other file, the parse
    fails with its media type.
  - **When the page refuses the GET, the parse fails and says why**, as `the page was not
    fetched: HTTP 403`. `ingestion items show` and `ingestion push` print it. The refusals are
    the ones that a FreshRSS poll meets. When a refusal may clear, as a 503 or a timeout may, the
    parse queues again after a wait, until its claims reach `extract.ReadAttempts`. `ingestion
    tasks retry` queues a failed one again. When a feed's entry later carries the same page, the
    item takes the feed's bytes, and its failed parse queues again over them.
  - **The item lands under the URL that its source names it by**, as the source's poll lands it,
    whatever form of the link was pushed. So a video's item holds its watch URL, whichever
    connector landed it first.
  - **A push that names its `destination` is the person's placement**, written in the write that
    lands the item. It decides the read as is too, so neither gate and no model stage reads the
    item. Where the push names no destination, the gates place the item.
  - **A destination declared `push_only` takes a push's placement alone.** Review leaves it out
    of its destinations, and a placement from review answers 422 `destination_push_only`. No tag,
    routing rule or side default may name it. `psyche` in `examples/declarations/` is one,
    because the push hands the item to psyche itself and no deliverer would reach it.
  - **The route refuses before it stores anything.** It answers 400 `invalid_link` for a url
    that is not http or https naming a host, and for a Reddit share link. It answers 404
    `connector_not_found` and 409 `connector_mode` for a connector that is not a push connector
    of the set in force. It answers 422 `destination_not_declared` for a destination that the set
    does not declare.
- **A connector's first fill runs the first of its items to reach a model stage, and holds the
  rest there.** `show` prints that item and its tokens by stage. `ingestion connectors release
  <name>` sends `POST /api/v1/connectors/<name>/release`, which queues every model task the fill
  held. The hold lives in the store, so it survives a restart. A connector that a later set
  dropped can still be released. An item that two connectors sighted waits while either holds it.
- **`ingestion connectors hold <name>` pauses a connector until `release` returns its work.** It
  sends `POST /api/v1/connectors/<name>/hold`. The scheduler runs no poll or folder scan of a
  held connector. No worker claims its queued poll. No worker claims any queued task of an item
  that it sighted, at any stage. While the connector is held, the store makes each new task of
  such an item held. Each task that a worker already leased runs to its end. The cursor keeps
  when and by whom the connector was held, so the hold survives a restart and ships with the
  cursor. `list` and `show` read it held, with the queued tasks it holds. `release` lifts the
  hold and releases the first fill in one write. Each task they held returns in its place by when
  it was made, so a held folder's replay resumes where it stood.

### A Reddit export lands each save once, under its post's or comment's key

Reddit answers its JSON only to a logged-in browser. So a bookmarklet in that browser exports the
saves, and a person drops the file it downloads. To install it, write the page that holds it:

```sh
# from a checkout
go run ./redditexport/bookmarklet reddit-export.html

# anywhere Go is installed
go run github.com/datapointchris/ingestion/redditexport/bookmarklet@latest reddit-export.html
```

Open the page, and drag its link onto the bookmarks bar. A bookmark editor can refuse a typed
`javascript:` URL, and a dragged link is saved as written. The page also holds the script to paste
into the browser's console. Clicked on a thread, the bookmarklet exports that thread. Clicked
anywhere else on reddit.com, it asks whether to export the newest saves or this month's top posts
of the subreddits it is given.

A drop connector whose `format` is `reddit-export` lands each such file:

```text
$ ingestion items drop reddit-saved-2026-11-25.json --connector reddit-exports
Dropped reddit-saved-2026-11-25.json into reddit-exports: it holds 51 items, 51 of them new.
```

- **The file lands as one item, and each save as its child**, under `reddit/<fullname>`, such as
  `reddit/t3_aq7ggd`. The file's own item counts among those the command prints.
- **Every item is `outside`**, because Reddit's users wrote it. A connector that names the format
  declares `zone: outside`, or the set is refused.
- **A save's sighting records its subreddit** under the field `subreddit`, for a routing rule to
  match.
- **A save that an earlier export landed keeps its item, its parent and its bytes**, so it is
  parsed, gated and delivered once. Its sighting names the newest export that carried it.
- **A file that is not a whole export is refused**, and nothing of it lands. That covers a file of
  another kind, and an export that the bookmarklet did not finish, such as one Reddit throttled
  partway. The refusal carries the bookmarklet's own error. Export again, and drop the new file.

### A ypl connector lands each video of a playlist, or each new upload of a channel

[ypl](https://github.com/datapointchris/ypl) holds a YouTube channel's credential, prices each
call against the day's quota, and paces every read. ingestion's YouTube source reads through ypl's
API and never calls YouTube itself. A poll connector of source `ypl` names what it reads in
`source_parameters`, one of two keys:

```yaml
source_parameters:
  playlist: capture          # a playlist's YouTube id, or the title of one that ypl holds
# channel: "@somechannel"    # or a channel's id or @handle
```

A worker reads ypl through its `[source.ypl]` table, as a client that holds the scopes that
`ingestion worker run --help` names for ypl. The worker holding that table claims `poll:ypl`,
`delete:ypl`, `refresh-metrics:ypl` and `parse:youtube-captions`. It claims `parse:youtube-whisper` too where its image
holds `audio-whisper`. Each read gives up after the bound that `ypl/client.go` sets for its route.
An audio read's bound covers the wait for its answer to begin, and each read of its body then waits
up to `audioIdle` for a byte. A redirect is a refusal.

Grant the client every one of those scopes. The worker asks for all of them in one token request.
An identity provider such as Authelia refuses a request that names a scope the client lacks. The
worker then reads nothing through ypl, so its polls, deletes and parses all fail.

- **Each video lands once as `youtube/<video id>`**, with its watch URL, its title and the media
  type `application/x-youtube-video`. Its item holds no bytes. Its sighting carries `channel_id`
  and `channel_title`, and its metrics `duration_s`, `views`, `likes` and `comments`. Its source
  time is when it was published. A poll reads every new video's details in as few requests as
  ypl's limit allows.
- **A playlist polled on a schedule declares `after_landing: delete`.** Each landed video then
  leaves the playlist through ypl, so the playlist holds only what has not landed. Without it each
  poll would read the whole playlist again. A playlist that keeps its videos is scheduled `once`.
- **A playlist's poll reports each available video it holds, in order.** It skips a video that
  YouTube withholds, such as a deleted or private one, and logs how many. Its sighting keeps the
  playlist item's id and the playlist's `playlist_id`, which the delete names. A video it already
  landed lands as nothing new.
- **A playlist named by its title stays the one that the title named at the first poll.** The
  connector's cursor keeps that playlist's YouTube id. Where the title later reads as another
  playlist, as after a rename, the poll fails and lands nothing. Name the playlist by its id to
  read the other one.
- **A delete costs quota that ypl prices.** One that the day's quota cannot afford waits until the
  Retry-After that ypl sends. ypl answers an item already gone as deleted. A delete that ypl
  refuses with a 4xx other than 408 or 429 fails, such as YouTube refusing a write to a playlist
  that the channel cannot edit. No later poll asks to delete that video again until `ingestion
  tasks retry` queues the failed delete once more.
- **A channel's first poll keeps its newest page of uploads as its window, notes the time as its
  baseline, and lands nothing.** Each later poll reads newest first until it meets an upload from
  the last window, and lands each upload before it, oldest first. An upload published before the
  baseline never lands, so a back catalog never floods the review queue. A poll that meets none of
  the window logs a warning. It reads a bounded number of uploads past the window, and logs a gap
  where they run on past that. A read that holds no uploads keeps the last window. `windowSize`
  and `mostUploads` in `ypl/poll.go` hold the two numbers. A connector declared again for another
  channel takes a new baseline. A channel's connector neither deletes nor runs `once`.
- **A video on air or scheduled waits until its recording is finished.** A playlist leaves it in
  place for the next poll, and a channel's cursor holds it. A poll of a playlist scheduled `once`
  is its last, so it lands such a video rather than leave it behind.
- **A refusal that ypl asks a wait for sends the poll back for that wait**, such as while the
  day's quota is spent or another caller holds ypl's turn. Any other refusal fails the poll, and
  the connector's backoff makes it due again.

A video's views, likes and comments go on changing while it waits in review, and no poll reports
it again. So at each time of its `[review.refresh-metrics]` schedule, `serve` queues one
`refresh-metrics:ypl` task where a ypl connector sighted a video in review.

```toml
[review.refresh-metrics]
schedule = "0 5 * * *"  # cron, in UTC unless a CRON_TZ=<zone> prefix names another; the default
```

- **The claim hands the task each video that waits in review then.** A video placed or dropped
  since the task was queued is not read. The worker reads every video's details in as few
  requests as ypl's limit allows.
- **`serve` writes the counts onto each ypl sighting of each video, and stamps when it read
  them.** The refresh lands nothing, queues no delete and runs no gate. It leaves when the
  sighting was last seen as it is. A video that ypl leaves out of its answer, such as a deleted
  one, keeps its counts and its read time. `items show` prints each sighting's metrics with when
  they were last read.
- **A refresh leaves out a views, likes or comments count that reads 0.** ypl answers 0 for a
  count that the owner hides, so a hidden count keeps the one measured before. A count that truly
  falls to 0 keeps its last value too.
- **Where a request fails partway, the counts already read are written.** A retry reads only the
  videos that no read took since the task was queued.
- **A refresh waits and spends no attempt in two cases.** In the first, ypl asks it to wait, as
  while the day's quota is spent or its Data API calls are busy. In the second, ypl does not
  answer. A worker logs no answer as a warning. A 4xx other than 408 or 429 fails the refresh. Any
  other failed read, such as a refused token or an address with no scheme, tries again a bounded
  number of times. The next day's refresh reads again either way.
- **A refresh reads only the sightings of the connectors in the set in force.** Renaming or
  removing a connector ends the refresh of each video that only it sighted.

### A freshrss connector lands each new entry of a reading list, with the page it links

The `freshrss` source reads a person's reading list through
[FreshRSS](https://freshrss.org)'s Google Reader API. The list holds every entry of every feed
that they follow. Its poll connector takes no `source_parameters`. The worker signs in through its
`[source.freshrss]` table. The table names the API and the login whose subscriptions it reads:

```toml
[source.freshrss]
api = "https://rss.example.com/api/greader.php"
login = "someone"
```

The login's API password reaches the worker as `INGESTION_SOURCE_FRESHRSS_PASSWORD`. The person
sets that password in FreshRSS under Profile, API management. The worker signs in through
`accounts/ClientLogin` before its first read. It signs in again where a read answers 401, and
claims `poll:freshrss`.

- **The first poll keeps the newest entry's crawl time as its baseline, and lands nothing.** An
  entry crawled before it never lands, so the feeds' back catalogs never flood the review queue.
  The set refuses a connector of the source that runs `once`, because it would land nothing.
- **Each later poll lands each entry crawled since the cursor, oldest first.** It follows each
  page's `continuation` to the reading list's end. Its cursor then moves to the newest crawl time
  it read, so a rerun lands nothing.
- **A poll reads again the hour behind its cursor.** FreshRSS stamps an entry's crawl time as it
  parses the entry, and commits the feed's entries after. So two refreshes at once can commit an
  entry crawled before one that a poll already read. The cursor keeps the id of each entry that a
  poll read in that hour. The next poll passes over those ids alone.
- **A poll reads for ten minutes at most.** When it stops, past that or at a failure after its
  first read, it reports what it read before the stop. The cursor moves past those entries alone.
  The next poll reads on from there.
- **When an entry links a YouTube video, the entry lands as the video, with no bytes.** A pushed
  link of the video lands the same way. When an entry links a Reddit post, the entry lands
  nothing. No extractor reads a Reddit post from its link. Nor does an entry land whose link is a
  video site's page that names no single video, such as a channel's. A push of each answers 422.
- **An entry lands once, under its link's key, as `text/html`.** Its key is the canonical link's
  `url/<hash>`. So when the same page arrives another way, it is one item with a sighting of each.
  The sighting keeps the entry's id, `feed_stream_id`, `feed_title` and `feed_label`. That label is
  the label of the feed's category, and a routing rule matches it. The item's source time is when
  the feed published it. Its zone is `outside`, and the set refuses another.
- **The worker GETs the page that the entry links, and keeps its HTML as the item's bytes.** Its
  sighting's `body` reads `page`. When the page refuses the GET, the worker lands the entry's
  content from the feed instead, wrapped as a page of its own. Its `body` then reads `feed`, and
  its `page_refused` says why:
  - a status other than 2xx
  - a redirect to the site's home or a section above the page
  - a media type other than HTML
  - a page past `publicweb.MaxPageBytes`
  - a bot's challenge, by its title, in place of the page
  - no answer

  The common case is a paywall's or a bot check's 403. The feed's content is often a teaser.
- **A refusal that may clear holds the entry for the next poll.** That covers a 5xx, a 408, a 429,
  a timeout, a failed lookup and no answer. After three polls, the entry lands the feed's
  content.
- **The GET dials only public addresses.** It checks each address after its name resolves, and
  again on every redirect. A link to a loopback, private, link-local or shared address lands the
  feed's content, with `page_refused` reading `an address that is not public`. The GET reads no
  proxy from the environment and carries no credential.
- **`article-trafilatura` parses the page.** It reads the page's main text with trafilatura, as a
  reader view does: the article without its navigation, footer or comment section, with its
  tables. Each paragraph, heading, list item and table row is one unit. When trafilatura finds no
  main text, the parse fails and the item waits in review.

### A video parses from its captions, or Whisper transcribes its audio

A video's item holds no bytes, so its parse reads through ypl. `youtube-captions` reads the caption
track that ypl picks. That is a manual track in the video's language or English, and otherwise an
automatic one in its original language or English.

- **The first unit is the video's description.** Each later unit is one chapter, labeled with its
  start and title, such as `2:00 The middle`. A video with no chapters takes one unit per five
  minutes that hold speech, labeled with the block's start. Each run of whitespace in a caption
  reads as one space.
- **The parse keeps the track it read** in its answer, with the video's language and length.
- **A video with no caption track writes its captions parse `deferred`.** A deferred parse is
  neither done nor failed. So nothing after the parse runs on the description alone, and the item
  does not wait in review. The write that ends that parse queues `youtube-whisper`.
- **`youtube-whisper` streams the video's audio from ypl into `audio-whisper`.** That program
  decodes the audio with ffmpeg to 16 kHz mono, ten minutes at a time. It runs faster-whisper's
  `large-v3-turbo` with the settings that the program's docstring names. It reads the model that
  the worker image pins, and never the network. Its units take the captions parse's layout: the
  description, then each chapter or five-minute block.
- **A Whisper run's time and memory are measured as `deploy/worker.yaml` records.** A run fails
  past `transcribeBound` in `ypl/parse.go`, which grows with the video's length.
- **A broken or short audio stream runs the parse again**, up to `extract.ReadAttempts` times. That
  covers a dropped connection and a stream that sends no byte for `audioIdle`. It also covers a
  transcript well short of the video's length, because audio cut off partway decodes with no error.
- **A failed Whisper parse puts the video in review as `no-parse`.** That holds however it failed,
  such as when its leases lapsed because its worker was killed. `ingestion tasks retry` reads it
  again.
- **A parse waits for as long as ypl asks it to**, while ypl is busy, paused, stopping or limiting
  the caller. Each wait lasts the Retry-After that ypl sent, and spends none of the parse's
  attempts. A read that no answer from ypl reached waits too, five minutes at a time, as while ypl
  restarts. The parse fails once it has waited `extract.ReadWaitCeiling` in all. A premiere or a
  stream that has not begun waits an hour at a time.
- **A read that ypl failed runs again** up to `extract.ReadAttempts` times. That covers ypl's
  `youtube_read_failed` and `internal`, and an identity provider that refused the token. Any other
  refusal fails the parse with ypl's refusal kept, and the item waits in review. That covers a
  members-only video, which no signed-out read reaches. It also covers a ypl that holds no yt-dlp.
- **Where ypl bounds a read's time or an audio's size, a long video's parse can fail.** ypl answers
  a read past its time bound as busy, so the parse waits until `extract.ReadWaitCeiling`. It refuses
  audio past its size bound with `audio_too_large`. A worker that reaches ypl at an address with
  neither bound avoids both.
- **`ingestion items replay --stage parse` reads each selected video's captions again.** Where the
  parse defers again, it queues Whisper only where Whisper has not read the video at its version.
  So a failed Whisper parse runs again, and a done one is kept.

### A memo is transcribed, then split into one item per topic

`audio-whisper` is the primary extractor of each recording that a memo lands as. It runs the same
program as `youtube-whisper` over the memo's bytes, in the slot group `heavy`.

- **Each segment that holds speech becomes one unit**, labeled by its span of the recording, such
  as `01:05–01:12`. The parse's answer keeps the language, the length, the voiced time, the
  segment count and where each unit starts.
- **A memo skips the quality gate.** The gate judges what an outsider wrote, so its scan queues
  no `quality` task for an item in the `owner` zone. The scan queues the `topics` stage instead.
- **The `topics` stage splits the memo's transcript.** It runs on each worker whose config names
  a `[model] adapter`, asking for what `[model.topics]` names. Its prompt, in
  `prompts/topics/prompt.tmpl`, quotes each unit numbered from 1 with its span. The answer holds
  to `prompts/topics/answer.schema.json`: each topic's first and last unit, a title and an idea.
  The worker keeps an answer only where its topics hold every unit once, in order. It fails the
  task on an answer with a gap or an overlap. `ingestion tasks retry` then asks again.
- **Each topic lands as an item of its own**, under `<memo key>#<n>`, with the memo as its
  parent and media type `application/x-memo-topic`. Each connector that sighted the memo sights
  it, with that sighting's tags. Its parse is its run of the memo's units, and its scan follows.
  Where the push named `recorded_at`, a topic's source time is when its first unit starts.
- **The memo then reads `split`**, and routes nothing of its own. Its side decision is `split`,
  by `gate`, for `topics`. The `topics` table keeps each topic's units, title and idea, and the
  outbox ships the title and idea as `ingestion.text`.
- **Each topic waits in review as `owners-words`**, because the quality gate never reads the
  owner's words. A tag or a rule still places it first. `suggestion` reads a topic by a model
  after its note's draft.
  `learning` and `watch` refuse a topic, because neither deliverer takes one.
- **The connector's first fill holds every memo after the first at the `topics` stage.** The
  fill also holds each later model stage of the first memo's topics. `ingestion connectors
  release <name>` sends the rest.
- **A memo's topics are written once, over the parse that the answer read.** When a newer parse
  lands while the call runs, the memo still splits over the older one.
- **`ingestion items replay --stage topics` asks again for a memo that never split**, once a
  person confirms its estimate. It covers a memo whose topics task failed, and one whose scan
  queued none. It skips a memo that holds a done answer over the parse that the stage quotes.

### Parsing an item, and running a stage again

Landing a new item queues its parse in the same write. A worker runs the extractor registered for
the item's media type, which writes the item's text as numbered units. `thread-text` reads a saved
Reddit post or comment: the post first, then each comment depth first, each unit labeled with
its author. A deleted or removed comment is left out, and its replies are kept.

`pdf-text` reads a PDF with PyMuPDF, one unit per PDF page, so unit n is PDF page n. A blank page
keeps its empty unit in place. A unit's label is the page's printed label where the PDF declares
page labels, and empty where it declares none. The extractor's version names PyMuPDF's, so a
library upgrade writes a parse beside the old one.

`GET /api/v1/items/{key}/parse` answers the item's reading parse: its text, its units and the
scan's findings, in `schemas/item-parse.schema.json`. The reading parse is the newest done parse
by an extractor that reads better than the primary one, such as Docling's below, and the newest
done parse otherwise. The scan, the gates and the model read all read it. The text is the units joined by a blank line.
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

### Profiling a PDF, and finding its chapters

A PDF's `pdf-text` parse queues its structure in the same write. A worker runs `pdf-structure`,
which reads the PDF with PyMuPDF and writes one profile:

- its page count, and each outline entry's level, title and page
- its page-label rules, and its metadata's creation date and title
- the share of every fifth page whose fonts include a monospace one, a sign of code, or a TeX or
  math one, a sign of formulas

The service writes the book's sections in the write that reports the profile, one row of the
`sections` table per outline entry:

- **An outline is usable** with 3 or more entries on 3 or more pages, spanning at least half the
  book.
- **The chapter level is the shallowest usable one.** A level whose entries are mostly `Part N`
  is skipped. Above the chapter level, an entry is a part where its title names one or it holds
  chapters, and otherwise a chapter, such as an Introduction before Part I. A deeper entry is a
  section of its chapter.
- **Front and back matter keep their kind and are no chapter.** Contents, a preface, an appendix,
  an index or a bibliography is found by its title. A matter word that opens a longer title
  marks matter only before a tail such as `of` or a colon, so `Index of Notation` is back matter
  and `Index Structures` is a chapter. An entry on PDF page 1 whose title names the book, as its
  file's name or its PDF's title spells it, is front matter too, as a title page is.
- **A section ends where the next entry at its level or above begins.** A printed page comes from
  the page's label, else from the book's offset, else stays empty. A book whose labels nearly all
  restate their PDF pages, at `identityShare` in `sections/outline.go`, reads none of its labels,
  because they say nothing of the numbers it prints. The offset is the most common
  difference between a PDF page and a number standing alone on its first or last three lines,
  where it holds on 30% of the pages.
- **A PDF with no usable outline and under 50 pages is one section over the whole file.** A
  longer one waits for the contents stage, below, to read its printed contents.

Each chapter becomes a child item, `<book key>#<n>` from 1, which holds no bytes. It carries its
own title and the book's zone and source time. It takes the book's side and read decisions,
decided by `parent:<book key>`, and each later decision on the book moves every chapter with it.
So placing a book from review places its chapters. A chapter is in no review queue, and a
placement naming a chapter is refused with its book's key. No model read runs on a chapter,
because it holds no parse of its own. A book with no source time takes its PDF's creation date.
A failed profile costs the book its sections and leaves its phase as the other stages set it.

`ingestion items show` prints each version of the sections, counting its entries and chapters,
and a line for each chapter. `ingestion items replay --stage structure` profiles each PDF again,
such as with a new `pdf-structure`. Its sections write beside the old. In a book that already
holds chapters, a new version's chapter names the chapter item with the same start page and
title key, as `sections.TitleKey` folds a title, and a chapter that matches none makes no item.

### A code-heavy book is read again by Docling

PyMuPDF's page text loses a code block's layout. `docling` reads a PDF a second time with
Docling's standard pipeline, OCR off and table structure on, one unit per PDF page holding the
page's markdown. A code block is fenced and a table keeps its rows. A blank page keeps its empty
unit, and a unit's label is the page's printed label, as `pdf-text` reads it.

- **Only a code-heavy, math-light book takes it.** Its profile's monospace share must reach
  `sections.DoclingMonospaceLeast`, and its TeX-or-math share must stay under
  `sections.DoclingTeXOrMathUnder`. Docling drops every formula, so a book with both stays
  `pdf-text`'s alone.
- **The write that ends the book's structure queues the parse**, of capability `parse:docling`.
  The parse writes beside `pdf-text`'s and is scanned like it. It becomes the item's reading
  parse, and stays so when a later `pdf-text` version parses the book again.
- **Nothing waits on it.** The book's sections and its contents stage keep reading `pdf-text`'s
  parse, and a delivery reads no parse. A placed book stays placed, as the side gate's rules
  below say.
- **`ingestion items replay --stage structure` queues it for each selected book** that Docling
  has not read at its version. A book whose Docling parse failed is read again.
- **It runs offline**, from the models that the worker image pins. A run past
  `extract.DoclingTimeout` fails its task. So does a convert that lost a page, so no lost page
  reads as a blank one.
- **A worker runs one Docling or Whisper parse at a time** and leaves its other slots to every
  other stage. Each holds gigabytes, so the two share the slot group `heavy`, whose default
  `extract.SlotGroups` holds. A `heavy` entry in `[worker.slot_limits]` changes that, where the
  worker's memory holds more runs. A worker runs one captions parse at a time too, because ypl
  reads one video at a time.

### A long book with no outline takes its chapters from its printed contents

Where the outline rule in force writes no sections for a PDF of `sections.WholeFilePages` pages
or more, the write that ends its first structure queues one task of capability `contents`. A
later structure queues none, so a structure or parse replay starts no model call.
`ingestion items replay --stage contents` reads such a book once a person confirms its estimate
of tokens, and `tasks retry` runs a failed task again. It is a model stage, so a connector's
first fill holds every book after the first to reach it, until `ingestion connectors release`.

- **The prompt quotes the pages that look like contents.** Among a book's front pages, a page is
  quoted where enough of its lines end in a number, as `ContentsPages` in `contents/handler.go`
  counts them. A run of such pages that reaches the last front page is read on past it. Where
  none looks like contents, the front pages are quoted whole, and the model may answer that they
  hold no contents.
- **The answer holds to `prompts/contents/answer.schema.json`**: each entry's title, the page
  that the contents prints beside it or null, and its level.
- **The service places each entry on a PDF page.** A printed page that a page label names maps to
  that page. Otherwise the book's offset maps a printed number. A book that prints no page
  numbers takes its offset from its entries: the offset at which the most entries' titles open
  pages, where at least `leastAgreeing` in `contents/place.go` agree. The nearby page that opens on
  the entry's title places it, within `searchPages` in `contents/place.go` of the mapped page.
  That is the start of a run of such pages, never before the entry ahead of it at its level. A
  title found only inside the running head of the entry that holds it, such as a chapter's,
  places nothing. A printed page that maps to none places its entry at the first page after the
  entry before it that opens on its title. An entry with no printed page, such as a part's
  heading, starts where the next placed entry starts.
- **A placement that leaves out many entries writes nothing.** Where it leaves out more than
  `mostLeftOut` in `contents/contents.go` of one level's entries with a printed page, at or above
  the chapter level, the book holds no sections from the answer, and serve logs the counts. An
  answer whose entries make no usable outline also writes none. The book then stays whole,
  rather than holding chapters that take in the pages of the ones left out.
- **The entries then pass the outline rule.** They need a usable level, and they take their
  kinds, their ends and their printed pages as an outline's entries do. They write `sections`
  rows with producer `contents` and a `start_placed_by` of `label`, `offset`, `title` or
  `next-entry`, and each chapter becomes a child item as an outline's does.
- **A structure replay places each stored answer again, with no model call.** A new placement,
  outline rule or parse then writes its sections beside the old. A chapter keeps its item where
  the new version's chapter shares its start page and title key, so a reading that words a
  title anew keeps the book's chapter items.
- **A failed or waiting reading costs the book its sections only.** The book waits at `contents`
  while its task is open, and a failed one leaves its phase as the other stages set it.

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
its task. The item's phase then reads `failed` until a later scan of it is done, unless the item
waits in review or a side decision dropped it.

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

### The quality gate reads each item's opening before anyone reads it whole

Where no person placed an item first, the write that ends the scan of its reading parse queues one
task of capability `quality`. An item in the `owner` zone, such as a memo or one of its topics,
gets none, because the gate judges what an outsider wrote. A tag or a rule places first, and its placement stands whatever
the gate answers. The gate answers such an item too, because each family's trust is
earned from the gate's answers on every item it reads. An item gets that task once. The gate runs on it again only through
a replay, or through `tasks retry` on its failed task. A worker whose config names a `[model]
adapter` claims it. It reads the item's media type, title, URL and the opening of its parse, up to
the bound that `qualitygate`'s `openingRunes` sets. It puts them to the model through the prompt in
`prompts/quality/prompt.tmpl`.

- **The answer holds to `prompts/quality/answer.schema.json`.** It gives a genre, one or two
  domains, up to four themes, a quality estimate from 1 to 10 and a one-line idea. It ends with a
  verdict of `pass`, `drop` or `unsure`, one sentence of `reason` on the verdict, and a side of
  `personal`, `fleet` or `unsure`, each with its confidence. An answer that breaks the schema
  fails the task rather than being patched.
- **The gate answers a fit to the owner's current work where it reads one.** A worker whose
  `[lens.fleet.context]` table opens quotes that context in the prompt, and the answer's `fit`
  runs from 0 to 10. With no context read, `fit` is null. The review tab sorts by it.
- **A drop needs a confidence at or above `qualitygate.DropFloor`.** A drop below it, or an
  `unsure`, sends the item to review. A confident drop waits in review as `gate-would-drop` until
  the item's family has earned hiding it, as the next section says. A drop is soft: a dropped item
  keeps its row, its one-line idea and its reason, and `items show` prints them.
- **The prompt's version is its number and a digest of `prompt.tmpl` and `answer.schema.json`.**
  A change to either writes the gate's outputs beside the old ones. It also starts a new family
  for every connector, which shows every drop in review again until it earns trust.
- **Each call is dispatched once.** The worker stores the dispatch in the write that ends its
  lease. Until the run answers, the task goes back with a backoff that doubles up to a cap, so a
  waiting item holds no worker slot. The next claim reads the stored dispatch's run, even on a
  worker started again. A run that fails fails the task, and `ingestion tasks retry <task>`
  dispatches it once more.
- **A call that the account's usage limit refused waits for the limit's reset.** It spent
  nothing, so it fails nothing. `items show` prints the refusal as the task's last try.
- **Nothing model-free waits behind the gate.** A held run holds up its own item alone, and
  `connectors list` counts that item as waiting.

`items show` says why an item waits at the gate, and why its run failed:

```text
$ ingestion items show reddit/t3_bb2    # no worker's config names a [model] adapter
reddit/t3_bb2   outside   waiting at quality
  ...
  waiting  unclaimed: no worker heard from lately claims quality tasks. A worker claims them once its config names a [model] adapter

$ ingestion items show reddit/t3_bb2    # the dispatch service holds the run
reddit/t3_bb2   outside   waiting at quality
  ...
  quality  quality-gate v1-<digest>  waiting  haiku at medium  dispatch 7f3a9c
  waiting  dispatched: dispatch 7f3a9c has not answered: the model budget holds new runs

$ ingestion items show reddit/t3_bb2    # the run failed
reddit/t3_bb2   outside   failed
  ...
  quality  quality-gate v1-<digest>  failed  haiku at medium  dispatch 7f3a9c  uncached input 12, cache write 3400, cache read 0, output 180
           "claude_failed: claude exited 1"
$ ingestion tasks retry c2e134a22919ae6efb0bbee0c02b0fb1
Task c2e134a22919ae6efb0bbee0c02b0fb1, quality on reddit/t3_bb2, is queued.
```

When a connector's first fill holds the item's task, the task waits as `first-fill`. When a
person holds the connector, the task waits as `held`. Either waits until `ingestion connectors
release`.
`tasks retry` posts to `/api/v1/tasks/{id}/retries`, and its `--json` prints
`schemas/task-retry.schema.json`. It queues a failed task again, prints a queued or leased task as
it stands, and refuses a done one.

`ingestion items replay --stage quality` runs the prompt in force over scanned items. It skips an
item that a person placed. It also skips one that holds an answer of that prompt over its reading
parse. An item that a tag, a rule or a confident guess placed keeps its placement whatever the new
answer says. The replay first prints how many items it would run on, and the median tokens by
kind of the stage's newest stored runs times that count. It queues only with `--yes`.

Each model stage's answer names the parse that its prompt quoted, in `input_producer` and
`input_version`. `items show --json` prints both. An answer over an older parse does not
count. So once a parse replay writes a new extractor version, each model stage's replay finds the
item due. The gate's call over the new parse is a dispatch of its own. Only its answer replaces
the old one, so while the call waits, and where it fails, the old answer stands. Where the read
gate decided the item's read from a note's draft over the older parse, the new answer appends the
read that the side calls for again and drafts the note over the new parse. The replay's estimate
leaves those drafts out. An item counts once in its family's trust, under its newest answer. Its
person's words against that answer count. Where it holds none, the newest earlier word counts,
read against the newest answer. The first open of a store at the release that records inputs
names, on each stored answer, the parse that its stage quotes.

`--placed-by person` runs the replay instead over each labeled item, one that a person placed
after the gates sent it to review, which the gate otherwise never judges again. The answer
measures the gate against the placement. Its output
and the gate's decision are kept. The side gate leaves the placement, its delivery and its model
read as they are, so even a confident drop moves nothing. While the run waits, and where it fails,
the item's phase stays where the placement took it. `ingestion gates agreement` then counts how
often the gate agreed.

`--stage quality-arm --model <model>` asks the gate's prompt of another model, as an arm of a
measurement. It runs over each labeled item that the gate answered under the prompt in force, and
that no arm has answered, so each arm answer sits beside the gate's answer to the same prompt.
The task carries the model, and the worker asks for it at the quality gate's effort. The answer is
kept at the `quality-arm` stage, which is on no item's line. So it appends no decision, and the
item's phase, placement and delivery stay as they are. One arm answers an item under each prompt.
Its estimate is the median of the arm's own stored runs, of any model, so the first arm call
prints none.

```text
$ ingestion items replay --stage quality-arm --model sonnet --key reddit/t3_bb2 --yes
Queued quality-arm (sonnet) on 1 items.
```

A scan that ended under a release without the quality gate queued no quality task, so its item
waits in review as `no-guess`. The replay is what runs the gate over such items.

### Each connector's family earns trust from a person's marks before the gate hides its drops

A family is one connector under one version of the gate's prompt, answered by one model at one
effort. It is named `<connector>@<prompt version>@<model>@<effort>`. The model is the one that
the run reports answering on, else the one that it asked for. A model or effort that the run
left to the adapter reads `default`. A new prompt version, model or effort starts a new family at
rung 0, so a model swap earns its own trust before it hides anything. The gate writes a family's
row with its first answer on an item that the connector sighted.

The family climbs earnedtrust's ladder at its cautious pace, the skip-lot pattern of acceptance
sampling. Rung 0 shows every drop in review as `gate-would-drop`. Each rung above shows a lower
share of the drops, down to a floor, and drops the rest as `quality-drop`. `ingestion gates trust list`
prints the run that earns asking and each rung's share.

- **A person's mark agrees or disagrees with the gate's answer on the item.** `low-quality`
  agrees with a drop and disagrees with a pass. `read` agrees with a pass and disagrees with a
  drop. A mark on an `unsure` answer is neither. A mark counts on any item the gate answered,
  including one that a tag or rule placed. A chapter's mark counts its book's answer.
- **Only an agreement with a drop climbs.** A family earns hiding drops, and a pass shows its
  item whatever the family's standing. So `read` on a pass is kept as agreed and moves no family.
  A disagreement on either answer counts.
- **A mark moves no item.** It counts on each family that sighted the item, under the prompt
  version, model and effort that answered it. A mark that repeats the item's newest, by the same hand on the same
  answer of the gate, appends nothing.
- **Placing an item on a destination disagrees where the gate showed or hid its drop.** Dropping a
  `gate-would-drop` item counts nothing. The marks say whether the gate was right.
- **A disagreement returns the family to rung 0, and resets its straight run.**
- **A straight run of agreeing marks earns asking.** The family's `asks_promotion` turns true. One
  promote then climbs it to rung 1. Each further straight run climbs a rung, counting only drops
  that the gate answered after the promote.
- **A promotion stands after a disagreement.** The family climbs again on its own once its run
  reaches the threshold. A promote before the run is earned climbs nothing until it is.
- **Which drops show is a seeded draw on the family and the gate's answer.** The same answer
  under the same family always draws the same way, so the share shown is reproducible.
- **An item shows its drop where any family of its sightings shows it.** Like the strictest
  zone, the lowest-trusted connector decides.
- **The show or hide is decided once for each answer of the gate.** A family that climbs or falls
  later moves no item already shown or hidden. A connector that sights a hidden drop later shows
  it where that connector's own family would.

A hidden drop keeps its row, its idea and its reason, and a person can still place it.

```text
$ ingestion items mark reddit/t3_bb2 low-quality
Marked reddit/t3_bb2 low-quality, which agrees with the quality gate's drop: reddit-exports@1-<digest>@claude-haiku-4-5@medium rung 0, straight run 30 of 30.
$ ingestion gates trust list
A straight run of 30 agreeing marks earns asking. Rungs 1 up show 25%, 18.75%, 14.06%, 10.55%, 7.91%, 5.93%, 5% of the gate's drops.

FAMILY                                             RUNG  RUN  STRAIGHT RUN  SHOWS  ASKS  PROMOTED BY
reddit-exports@1-<digest>@claude-haiku-4-5@medium  0     30   30 of 30      100%   yes   —
$ ingestion gates trust promote reddit-exports@1-<digest>@claude-haiku-4-5@medium
Promoted reddit-exports@1-<digest>@claude-haiku-4-5@medium to rung 1, which shows 25% of the gate's drops and drops the rest as quality-drop.
```

`items mark` posts to `/api/v1/items/{key}/marks`, which needs `ingestion.marks.write`, and its
`--json` prints `schemas/item-mark.schema.json`. `gates trust list` reads `GET
/api/v1/gates/trust` in `schemas/gate-trust-list.schema.json`, under `ingestion.items.read`.
`gates trust promote` posts to `/api/v1/gates/trust/{family}/promote`, which needs
`ingestion.trust.promote`, and prints `schemas/gate-trust-promotion.schema.json`. A service
promoting for a person names the person's `login` in its body, as a placement does. A service
marking for a person may name one. Without it the mark is decided by `service:<client id>`, and
counts as the person's word that the service relays.

Each mark is a row of `marks`. Each word that counts on a family is a row of
`gate_trust_decisions`, and the family's `gate_trust` row holds the standing that earnedtrust reads
from them. Both family tables carry the family's `model` and `effort` as columns, so a reader
filters by them without parsing the name. `serve` reads every family's standing again once it
listens, so a release that changes the ladder places each family again from its decisions. All
three ship on the record channel.

### The side gate places each item, and a person places what it leaves

When the scan of an item's reading parse is done, the service runs the side gate in the same
write. A parse's end alone places nothing, so no item reaches a side before its scan. The gate
places the item by the first rung that decides it. A memo passes none of them. It waits at
`topics` while its task is open, and reads `split` once its topics land.

1. **Every sighting's tags.** A tag that a destination names places the item on that
   destination's side. Tags naming two sides send it to review as `tags-disagree`.
2. **A routing rule**: the first in the file whose match holds on one sighting, then the first
   rule that a person accepted, each keep before each block. A Reddit save's sighting carries its
   `subreddit` field for a rule to match. An accepted rule's review share of its items waits in
   review as `rule-would-hide` instead.
3. **The quality gate.** The item waits at `quality` until the gate answers. A confident drop
   sends it to review as `gate-would-drop`, or drops it as `quality-drop` where its families'
   trust hides it. An unsure answer, or a drop below the floor, sends it to review as `quality-review`. A pass reads the
   gate's side guess. A personal guess places the item where its confidence reaches the floor
   that `route.GuessFloor` sets. A fleet guess places nothing at any confidence, because a tag,
   a rule or a person puts an item on the fleet side, never a guess. A fleet guess, a guess below
   the floor, or `unsure` sends the item to review as `below-floor`, with the guess beside it.
   Where a connector that sighted the item declares `person_sorts: true`, no guess places it, and
   a pass sends it to review as `person-sorts`, with the guess beside it. The gate never reads
   an item that holds the owner's own words, such as a memo's topic. Such an item waits in
   review as `owners-words`.
4. **Review.** An item that no tag or rule placed, and that holds no answer of the quality gate,
   waits as `no-guess` once an apply runs the side gate again. That is an item whose quality task
   failed, or whose scan could not read the set in force and so queued none.

The gate runs again in the write that ends each later stage before it, such as the quality gate,
in the write that applies a changed set, and in the write that lands another connector's sighting
of the item. Each run passes over an item whose reading parse is not scanned yet. Each run decides
again an item that the gate sent to review as `no-guess`, `below-floor`, `person-sorts`,
`owners-words` or `gate-error`, or that the quality gate dropped or sent to review. So a guess, a rule, or a second
sighting's tag that arrives later places it. A person's placement withdraws the item's queued
quality task. A tag's or a rule's leaves it queued, so the gate answers that item too, and its
answer moves nothing. A placed item stays where it is: a later sighting or parse moves nothing, and only a
person's placement moves it. Where the gate cannot read the set in force or the item's sightings,
it logs why and sends the item to review as `gate-error`, and the scan stands.

An item whose parse failed waits in review as `no-parse`. When an item's scan fails for good
before any side decision, the item waits neither on a side nor in review. Its phase reads
`failed` until `items replay --stage scan` scans it.

Where the item lands on a side, the read gate chooses how it is read: as is, or by a model under
the side's lens. The first connector that sighted it and declares a `read_rule` decides.
Otherwise a personal item is read as is, and a fleet item by a model.

Each decision is appended to the item's records with who decided it: `tag`, `rule:<name>`,
`model:<run>`, `person:<login>`, or `gate` where the rungs ran out. `items show` prints them.

```text
$ ingestion review list
ITEM                   ZONE     REASON    GUESS  WAITING SINCE            TITLE
reddit/t3_bb2          outside  no-guess  —      2026-11-25 01:07:26 EST  A post
file/5d41402abc4b2a76  outside  no-parse  —      2026-11-25 01:07:26 EST  —
$ ingestion review place reddit/t3_bb2 work-inbox --cause subreddit --note "what would this change?"
Placed reddit/t3_bb2 on work-inbox: side fleet, read by a model under the fleet lens.
$ ingestion review drop file/5d41402abc4b2a76 --cause title
Dropped file/5d41402abc4b2a76, cause title.
```

`review list` reads `GET /api/v1/review`, the longest waiting first, in
`schemas/review-list.schema.json`. Each item carries the quality gate's whole answer and each
sighting's tags, fields and metrics. It also carries what any source's sightings say of who
published it, how long it runs and how it was received: `channel`, `length_s`, `views`, `likes`
and `comment_count`. Each source registers which of its fields and metrics those are, through
`landing.RegisterSightingRoles`, so a reader needs no source's names. Whatever the reason an item waits, its `guess` is present
wherever the gate gave one, so a reader shows it by its presence. The answer also lists each declared destination with its side, so a
reader needs no call per item. Where the set in force cannot be read, `destinations` is null and
the queue still answers. `place`, `archive` and `drop` post to `/api/v1/review/{key}/place`,
`…/archive` and `…/drop`, and print `schemas/review-placement.schema.json` with `--json`. A
placement names one of review's choices, as the next section tells, or a destination that the set
in force declares, and any other is refused. `--cause` names the aspect
that decided it, such as subreddit or title, and a decision that names none is kept as
`unstated`. `--note` carries the questions that a model read answers. With no note, a model drafts
one, and a placement with no note of an item that holds a draft takes the draft. Sending the same placement again changes nothing. A service placing for a person names
the person's `login` in its body, because a service token carries no person. The decision keeps
the service's client id beside the login.

### Each of review's choices places, archives or drops an item

Each choice says where the item goes and what runs on it then. `ingestion review place <key>
<choice>` posts the choice to `POST /api/v1/review/{key}/place`. `review archive` and `review
drop` post to `…/archive` and `…/drop`. Each route needs `ingestion.review.place`, and the review
tab binds one key to each choice:

| Key | Choice | Where it goes, and what runs | When |
| --- | --- | --- | --- |
| `A` | `articles` | delivery to the destination whose deliverer is `life-articles`, read as is | at once |
| `L` | `learning` | delivery to the destination whose deliverer is `learning-resources`, whatever the quality gate answered | at once |
| `S` | `suggestion` | the fleet side's default destination, read by a model under the `fleet` lens after a note's draft | in the background |
| `W` | `watch` | the `ypl-watch` deliverer adds the video to the playlist picked, or to `[watch] default_playlist` | at once |
| `P` | `psyche` | the destination named `psyche`, then `psyche relate` on the machine that placed it, as `ingestion push --to psyche --relate` runs it | at once |
| `E` | `archive` | nowhere: the record stays on the personal side with no destination, with the `read` mark | at once |
| `d` | `drop` | the drop, with the cause picked from the drop menu, and the `low-quality` mark for a drop for low quality | at once |

```text
$ ingestion review place youtube/aqz-KE-bpKQ watch --playlist "Watch soon"
Placed youtube/aqz-KE-bpKQ on watch in the playlist Watch soon: side personal, read as-is.
$ ingestion review archive youtube/Ks-_Mh1QhMc --liked channel
Archived youtube/Ks-_Mh1QhMc, liked for its channel. Its record stays with no destination, and your read mark teaches the quality gate.
```

- **Each placing choice writes a person's side decision, then a read decision whose cause is the
  choice.** The side decision keeps `--cause`, or `unstated`. The gate's trust and its agreement
  count it as a person's placement. A card sent with it is kept. A placement that repeats the
  newest side decision, playlist included, changes nothing.
- **A choice places on the first destination whose deliverer is the choice's.** Suggestion places
  on the fleet side's default, and psyche on the destination named `psyche`. `review list --json`
  lists each choice under `choices` with that destination. It says why a choice places nothing
  where the set declares no destination for it. The placement then answers 422
  `choice_refused`. It answers the same where the deliverer cannot take the item, such as
  `ypl-watch` for anything but a YouTube video.
- **`review place` also takes a destination's name**, for a destination that no choice reaches.
  The item takes that destination's side, as a tag or a rule would place it. The route's body
  names `destination` rather than `choice`, and a caller sends one of the two. A name that is
  both a choice's and a destination's is read as the choice. The tab's note menu numbers each
  such destination after the choices.
- **A psyche placement lands first, then `psyche relate` reads the item's parse and its scan on
  the machine that placed it.** The service keeps nothing of the relate. Where it fails, the item
  stays at psyche, and `ingestion push <key> --to psyche --relate` runs it again.
- **An archive is done with the item, such as a video watched elsewhere.** Its side decision is
  the person's, on the personal side with no destination. Its read decision has verdict
  `archive`, so nothing is delivered, and the item's phase reads `archived`. It writes the `read`
  mark, which counts on the item's gate families. `--liked` names what the person liked:
  `channel`, `theme` or `this-item`. The side decision keeps it as its cause, and `unstated`
  where none is named. The tab's `E` asks the same from a menu.
- **A drop for low quality writes the `low-quality` mark** in the same write as the drop, so the
  gate families count it as `items mark` would.
- **An archive or a drop writes its mark only for a caller that may write marks.** A person's
  token may. A service's token may only where it also holds `ingestion.marks.write`, the scope of
  `POST /api/v1/items/{key}/marks`. A mark against the gate's drop raises the trust that hides
  later drops, so `ingestion.review.place` alone does not write one. The decision is written
  either way.
- **Two reasons wait in review only on records already written.** `shelved` is a read decision
  of verdict `shelf` that no opening followed. `learning-refused` is a learning sort that the
  quality gate did not pass. A person places either item by a choice.
- **`ypl-watch` inserts the video through ypl's `POST /api/v1/youtube/playlists/{id}/items`.**
  Each insert spends ypl's Data API quota. YouTube keeps a second copy of a video that a playlist
  holds already. The `deliveries` row keeps the playlist picked, and is never queued again once
  settled. From its second attempt on, the deliverer reads the playlist before it inserts. The
  attempt before may have landed with its answer lost. The deliverer then settles on the video
  that it finds there. A quota
  that ypl reports spent holds the task for its `Retry-After`, and spends no attempt. The
  deliverer writes through the client of the worker's `[source.ypl]` table. The Data API cannot
  write the Watch Later list, so a watch names a playlist of the channel's own.
- **One delivery adds the video to one playlist.** Where the item's delivery holds another
  playlist, a watch answers 422 `choice_refused` and names the playlist held.
- **Only the watch choice places an item on the `watch` destination**, because only it picks the
  playlist. Review leaves the destination out of `destinations`. A placement that names it
  answers 422 `destination_takes_playlist`. The set check refuses the destination as a side's
  default or a tag's target. It also refuses an accepted rule that names it.
- **The review tab's `W` asks which playlist.** It lists the default first, then each playlist
  that `ypl playlists list --json` prints on the person's machine. Enter takes the default, a
  number picks its playlist, and any other text names a playlist by its title or its id.

### A person accepts a routing rule in the service, and its review share shows what it would hide

A rule written by hand stays in `routing-rules.yml`. A rule that a person accepts, such as one
proposed from their run of verdicts on a connector, is a row of `routing_rules` instead. Each
write appends a version, so the table's history ships on the record channel. The newest version
of a name is in force until it is retired. The side gate reads each rule in force after every
declared rule: each keep, then each block, by name. A keep goes first because a like on a feature
wins over a dislike, as NewsBlur weighs them.

- **A block drops each item that it matches** as `rule:<name>`, and keeps its row. A keep places
  it on a destination on a side.
- **A rule's `review_share`, from 0 to 1, of its items waits in review instead**, marked
  `rule-would-hide`, so a wrong rule shows itself. A seeded draw over the rule's name and the
  item's key picks them. A replay picks the same items, and raising the share keeps every item
  that a lower one picked.
- **A write runs the side gate again** over each scanned item that waits on it, as an apply does,
  and over each item that the rule dropped or sent to review. So each item follows the rule's
  newest version. An item that a tag, another rule, a model's confident guess or a person placed
  stays where it is. A write that repeats the newest version runs the gate too, so a retry
  decides what a failed first attempt could not.
- **Retiring a rule brings back what it hid.** The side gate decides again each item that the rule
  dropped or sent to review, as though the rule had never held. An item that it placed on a
  destination stays there, because its delivery may have run.
- **The quality gate still reads each item that a rule decides.** The rule's decision stands
  whatever the gate answers. A retired block's items are then placed from that answer, with no
  new model call.
- **A rule never weighs in the review tab's order.**
- **A rule matches what a source states**: a connector, a tag, or a sighting's field. A theme or a
  genre is a model's output, and a title is outsider text, so no rule matches either before the
  model reads the item.

```text
$ ingestion gates routing-rules put quiet-misc --block --connector reddit-exports --review-share 0.1
Accepted routing rule quiet-misc as version 1. It drops each item that reddit-exports sighted, and sends 10% of them to review as rule-would-hide.
$ ingestion gates routing-rules list
NAME        VERSION  ACTION  MATCH                        DESTINATION  REVIEW SHARE  STATE     DECIDED BY
quiet-misc  1        block   that reddit-exports sighted  —            10%           in force  person:person
$ ingestion gates routing-rules retire quiet-misc
Retired routing rule quiet-misc as version 2. The side gate decided again each item that it dropped or sent to review, and each item that it placed stays there.
```

`put` sends `PUT /api/v1/routing-rules/{name}`, whose body holds to
`schemas/routing-rules.schema.json#/$defs/accepted`. `retire` posts to
`/api/v1/routing-rules/{name}/retire`. Each needs `ingestion.routing-rules.write` and prints
`schemas/routing-rule-version.schema.json` with `--json`. A service writing for a person names the
person's `login`. Where a service names no login, it writes as `service:<client id>`. The
body's `proposal` names the proposal that the person accepted, which the version keeps. Writing
what the newest version already says appends nothing. A rule that the set in force cannot hold
is refused with 422 `routing_rule_refused`, and the answer names each reason. `connectors apply`
refuses a set in turn where an accepted rule could not hold under it: a declared rule under an
accepted rule's name, retired ones included, or a set that drops what a rule in force names.
`list` reads `GET /api/v1/routing-rules` under
`ingestion.items.read`, in `schemas/routing-rules-list.schema.json`. A match's values copy a
source's field, so `match_values` ships on the text channel.

### `ingestion tui` reads each waiting item whole, and one key decides it

`ingestion tui` opens a terminal board with a tab per view, and bare `ingestion` opens it too.
Its `review` tab lists the review queue, grouped by channel. Each row shows the item's channel,
such as a video's channel or `r/golang`, then its title and why it waits. A narrow screen cuts
the title before the columns after it. Then come the quality gate's verdict and confidence, its
quality estimate, its fit, and its side guess and confidence. A video's row adds its length,
views and likes per view. Each row ends on how long ago the item was made at its source. The
side panel reads the item under the cursor. It opens on the card:

- the channel, the length and the upload day
- the views, likes and comments, and the likes per view
- the gate's quality, fit, verdict, reason and side hint
- `gate-would-drop` where the gate would drop it, and how the read of its comments stands
- `rule-would-hide` where a routing rule's share drew it, naming the rule and what it would do

Under the card come its URL, the gate's whole answer, the note's draft where a model drafted one,
the scan's findings, and its text, the post first and then each comment. `enter` reads it full
screen.

`s` cycles the sort: newest made at its source, the gate's quality, likes per view, shortest
first, and the gate's fit. An item with no value for the sort comes after every item with one.
`v` cycles the grouping: its channel, its connector, or none. None draws one list under the
column titles. `/` filters the rows by any word of the title or the channel, and of the URL, the gate's
idea and themes, or a sighting's fields.

| Key | Does |
| --- | --- |
| `A` `L` `S` `W` `P` `E` | places it by `articles`, `learning`, `suggestion`, `watch` or `psyche`, or archives it, as the section on review's choices tells. The bar under the strip gives each choice a line naming where it sends an item |
| `d` | drops it, after asking what decided it: its channel or another field of its source such as its subreddit, its title, its theme, its low quality, or none. A drop for low quality also marks it `low-quality` |
| `c` | asks once what a video's top comments say, as the next section tells |
| `n` | writes a note in `$VISUAL` or `$EDITOR`, which a model read answers, then places the item with it by the choice picked from a numbered menu |
| `o` | opens its URL in the browser, where it is an http or https address |

Each key posts to the routes that `review place`, `review archive` and `review drop` post to,
under the same login, so the decision is the person's. A placement or a drop also keeps `presented`,
the card as the tab showed it:

- the sort and grouping in force
- the item's rank under the sort, its position among the rows shown, and how many were shown,
  once the filter and the folds hid theirs
- the gate's quality, fit, verdict and side guess, with their confidences
- when the cursor came to rest on it

`decided_after_ms` counts the milliseconds from then to the decision. Both sit on the side
decision, and ship on the record channel, so a reader can measure how the card's order and
numbers sway a decision. `review place` and `review drop` send neither. A service built before
the card refuses a decision that carries one, naming the field. The tab then sends the decision
again without it, and says that the card was not kept. A decided item leaves the table at once, and the cursor moves on
through its group. A decision that fails puts the item back with the server's answer on it, and a
note that was not placed stays in its file, which the footer names. Quitting waits for every
decision sent.

A title, a body or a comment that a stranger wrote is drawn with every terminal control taken out,
so an escape sequence in it cannot set the clipboard or clear the screen. Its markdown is drawn as
typed. `--once` prints one frame at `COLUMNS` by `LINES`, and `--json` the snapshot it is drawn
from, in `schemas/tui-review.schema.json`. Where the queue was not read, each still prints, then
exits as `review list` would, 4 where the server gave no answer.

### Placing a gate's drop from the `history` tab overrules the gate

The `history` tab lists every item that came through ingestion, the newest landed first. It reads
`GET /api/v1/items` under the outcome filter in force, up to 500 items, and the strip says where
more are held. Each row shows the item's outcome and where it went. Then come who decided, why,
its connectors and its phase. The row ends on how long ago it landed, and its title. Dim under
it sit its key, its channel and the decision's `decided_by`.

```text
history 7 items  outcome dropped  group none, newest landed first
l outcome: every outcome placed archived [dropped] waiting split
read 0s ago

 ▾ dropped, newest landed first · 7  WHERE  BY    WHY           CONNECTOR       PHASE    LANDED  TITLE
 ▸ dropped                          —      gate  quality-drop  reddit-exports  dropped      0s  A post
    └─ reddit/t3_h107 · r/misc · model:run-38
```

`l` cycles the outcome filter: every outcome, then `placed`, `archived`, `dropped`, `waiting` and
`split`. Each step reads the items again under the new filter. The dropped list is the audit of
the filter: every item that the gate, a rule or a person dropped. `v` cycles the grouping:
none, by outcome, by connector, or by who decided. `/` filters on the title, the URL, the channel
and `decided_by`.

A choice's key places the item under the cursor, as the review tab's key does. `E` archives it
after asking what was liked. Each key calls the route that `review place` or `review archive`
calls, under the same login, so the decision is the person's. The row stays with the write named
on it. The read after the write lands draws the new outcome.

Placing an item that the gate dropped is the person's disagreement with the gate. Gate trust takes
it as it takes a placement from review, so each family of the gate's answer returns to rung 0.
Archiving such an item writes the read mark. A read mark disagrees with the gate's drop too.
`--json` prints the snapshot in `schemas/tui-history.schema.json`.

### `c` reads what a video's top comments say, once, when a person asks

`c` on a video's card posts to `POST /api/v1/items/{key}/comments`, under
`ingestion.comments.read`, and the answer holds to `schemas/item-comments.schema.json`. The first
request queues one task of capability `comments`. Each later request answers how that read
stands, so nothing reads comments in a batch. The review list carries how each item's read
stands, and null where no source of the build reads its comments:

- `unread` where nobody asked
- `reading` while its task or its run has not ended
- `unclaimed` while its task waits and no worker heard from lately claims `comments` tasks
- `done` with the answer
- `failed` where its task failed, which the next request queues again

- **A worker with a model port and a source that reads comments claims it.** ypl is that source.
  The worker reads the top comments that ypl answers, clipping each one. It asks
  the model through `prompts/comments/prompt.tmpl` on `[model.comments]`, which defaults to haiku
  at medium.
- **The answer holds to `prompts/comments/answer.schema.json`.** It gives a summary, up to four
  points of praise and four of criticism, and a tone of `positive`, `mixed`, `negative` or `none`.
- **The answer is outsider text.** A model wrote it over strangers' comments, so the store keeps
  it as the output's `answer`, which ships only as `ingestion.text`. It keeps none of the
  comments themselves.
- **The card shows the answer under "What the comments say" once it lands.**
- **The stage sits on no item's line.** It moves no phase, places nothing and delivers nothing.
- **A video whose comments are off, or that holds none, fails its task with no model call.**
  `c` asks again, and so does `tasks retry`.
- **The route refuses other items.** An item whose media type no source reads comments of gets
  422 `comments_not_read`.

### `gates agreement` counts how often each gate's answer matched a person's placement

The label is a person's placement of an item that the gates sent to review first. The item's
newest side decision is `person:<login>`, and an earlier side decision, or the quality gate's
decision before it, was a review. A placement that no review came before labels nothing, such as
one made before the gates ran, or one that carried over a judgment made somewhere else. The item's
own decisions say which it is, so no list of keys decides it. `ingestion gates agreement` reads
`GET /api/v1/gates/agreement` and sets every done answer of the quality gate on a labeled item
beside the placement.

- **The quality gate's verdict** agrees where it passed an item that the person placed on a side,
  or dropped one that the person dropped.
- **The side guess** agrees where it names the person's side. A person's drop gives it nothing to
  compare.
- **An unsure answer** neither agrees nor disagrees. Agreement is agreed over agreed and
  disagreed.
- **Each answer counts as the model gave it**, before a floor turns a drop or a guess into review.
  So the rows by confidence band show where a floor would sit, as a reject option does in
  selective classification.

Each gate breaks down by confidence band, connector, model and prompt version. An item that two
prompt versions answered counts once under each. A labeled item that the gate never answered,
such as one it sent to review for want of a rule, holds no answer, which `items replay --stage
quality --placed-by person` asks for. `--json` prints `schemas/gate-agreement.schema.json`.

Each arm prints after the gate, by model. Its tables count the arm's answers and then the gate's
answers to the same prompt versions on the same items, so two models compare over one set of
labels. The gate's own tables count only the gate's answers.

The note band prints last. It counts each note's draft by its confidence. A draft is kept where a
person's placement took it, and rewritten where a person's note took its place. It is applied
where it reached the floor and no person placed the item, waiting where the item waits in review as
`note-unclear`, and other where the item left a read by a model. KEPT SHARE is kept over kept and
rewritten, the drafts that a person decided, so the rows by band show where `[note] floor` would
sit.

```text
$ ingestion gates agreement
2 items a person placed from review: 2 personal, 0 fleet, 0 dropped. 0 hold no quality answer.

quality gate, drop floor 0.80
BY                        COMPARED  AGREED  DISAGREED  UNSURE  AGREEMENT  ANSWER BY PLACEMENT
all                       2         1       1          0       50%        drop/personal 1, pass/personal 1
confidence 0.8-0.9        1         1       0          0       100%       pass/personal 1
confidence 0.9-1.0        1         0       1          0       0%         drop/personal 1
connector reddit-exports  2         1       1          0       50%        drop/personal 1, pass/personal 1
model haiku               2         1       1          0       50%        drop/personal 1, pass/personal 1
prompt v1-<digest>        2         1       1          0       50%        drop/personal 1, pass/personal 1

side guess, floor 0.70
...

note drafts, floor 0.70
BY                  DRAFTS  KEPT  APPLIED  REWRITTEN  WAITING  OTHER  KEPT SHARE
all                 3       1     1        1          0        0      50%
confidence 0.4-0.5  1       0     0        1          0        0      0%
confidence 0.5-0.6  1       1     0        0          0        0      100%
confidence 0.9-1.0  1       0     1        0          0        0      —
```

### Each week's misses draft one proposed change to the quality gate's prompt

A miss is one done answer of the quality gate that a person's word went against. The word is a
placement from review that `gates agreement` counts as disagreeing with the verdict or the side
guess. It is also a `low-quality` mark on an item that the gate passed, or a `read` mark on one
that it dropped.

At each time of its `[gates.proposals]` schedule, `serve` projects the misses labeled since the
last done proposal. Where it finds any, it writes one `prompt_proposals` row. The row holds the
misses and the text of the prompt that the draft changes. `serve` queues one `prompt-proposal`
task, which a worker with a `[model]` adapter drafts, and keeps the draft on the row. A time that
finds no miss drafts nothing, and so does a time whose last draft has not ended.

```toml
[gates.proposals]
schedule = "0 6 * * 1"  # cron, in UTC unless a CRON_TZ=<zone> prefix names another; the default
```

- **A projected miss reads no outsider's text.** It holds keys, enums, numbers and the person's
  own words: the connector, media type, the gate's verdict and side with their confidences, its
  quality and genre, the prompt version, model and effort that answered, and the person's label,
  destination, cause and note. It never holds a title, URL, the item's text, the gate's domains,
  themes, idea or reason, or a sighting's fields. So the proposal is in the `owner` zone, and the
  outbox ships it whole as `ingestion.record`.
- **The draft names exact edits.** Each edit quotes a passage of the prompt as `before`, which has
  to occur once, and gives its `after`. A draft whose edits do not apply, or that answers a miss
  the proposal does not hold, fails. A draft may name no edit, where the misses show no change
  worth making.
- **A failed draft's misses roll into the next.** The next time's proposal reads them again beside
  its own. `tasks retry` runs a failed draft again until then, and afterward refuses it as
  `task_superseded`.
- **Nothing here changes a prompt.** A person who accepts a proposal edits the prompt's file, and
  the build that carries it answers under a new prompt version. A draft reads the prompt's text
  from its row, so a worker of another build drafts against the same text.

`ingestion gates proposals list` reads `GET /api/v1/gates/proposals`, the newest first. `--json`
prints `schemas/prompt-proposals-list.schema.json`.

```text
$ ingestion gates proposals list
Proposal 3f0c9a2e41b7d8c5e6f1a0b9c8d7e6f5 changes the quality prompt at v1-<digest>, from 3 misses labeled through 2026-03-02 01:00:00 EST.
  drafted by sonnet at medium
  pattern  The gate passed thin posts and guessed fleet for personal ones.
  change   Ask what an item adds before a pass.
  edit 1
    - - themes: two to four short phrases naming what the item argues or explores.
    + - themes: two to four short phrases naming what the item argues or explores. Name what the item adds.
  answers  3 of 3 misses: 1, 2, 3
```

### A model drafts the note that a person left out, and an unclear draft waits on a person

Where the read gate reads an item by a model and the read carries no person's note, the same write
queues one task of capability `note`, ahead of the model read. A worker whose config names a
`[model]` adapter claims it. It puts the item's media type, title, URL and the opening of its
reading parse to the model, through `prompts/note/prompt.tmpl` and the lens's file in
`prompts/note/lenses/`. The opening is cut at a fixed length, and the prompt says where it stops.
The lens's context goes beside them where the worker opened a reader for the lens. The answer
holds to `prompts/note/answer.schema.json`: questions for the read, why the item was likely saved,
how it bears on the owner's current work, a summary, a one-line idea and a confidence.

- **A draft at or above the floor becomes the read's note.** The read gate appends a read by a
  model, decided by the draft's run, with cause `note-draft`. The model read then answers the
  draft.
- **A draft below the floor waits on a person.** The read gate appends a review with cause
  `note-unclear`. `review list` carries the draft in `note_draft`, and the review tab shows its
  summary, relation and idea. `review place --note` replaces the draft with the person's note.
  Placing the item with no note takes the draft.
- **A draft never reaches a decision's note.** A model wrote it from outsider text, so it takes the
  item's zone, and the outbox ships it as `ingestion.text`. The model read quotes it under its own
  heading.
- **A person's note skips the draft.** A placement with `--note` queues no `note` task, and
  withdraws one that has not run.

```toml
[note]
floor = 0.7  # the confidence at or above which a draft becomes the read's note; the default
```

`serve` reads `[note] floor`, from 0 to 1. `gates agreement` counts each draft by its confidence
band, so its rows show where the floor would sit.

**A lens's context is the owner's current work.** A `[lens.<name>.context]` table in a worker's
config opens a reader that this build registers. The note and the model read under that lens read
the context beside each item. The `slate` reader reads a
[slate](https://github.com/datapointchris/slate) tracker's focus and its active initiatives,
through `GET /api/v1/focus` and `GET /api/v1/initiatives?status=active`. It keeps what it read for
a short while, so a batch of drafts reads the tracker once. Where no table names a reader, or its
table cannot open, each stage under the lens runs without a context, and the note's prompt says
it has none. A read that fails or runs past its deadline costs only the context. The worker logs
it, and the stage runs beside the last context read, or none. At start the worker logs each reader
it opened and how much it read, and each lens with a note prompt that no reader serves.

### A model read answers a fleet item's note with a summary and a suggestion

Where the read gate reads an item by a model, under a lens whose prompt this build holds, the same
write queues one task of capability `model-read`. It queues none where the item already holds a
read that answers the decision's note. A person's drop or move withdraws it where it has not run.
The task carries the read decision's lens, and the note that a person wrote with
`review place --note`, or the note's draft that the read took. A worker whose config names a
`[model]` adapter, and that runs the scan, claims it. It reads the item's media type, title, URL
and its parse's text, cut at a fixed length, and the prompt tells the model where the text stops.
It puts them to the model through `prompts/model-read/prompt.tmpl` and the lens's file in
`prompts/model-read/lenses/`, beside the lens's context where the worker opened a reader.

- **The lens says what the item is read for.** The `fleet` lens reads it for the software it may
  bear on: how that software is built, run and kept safe. The `personal` lens reads it for the
  owner's own life, under a connector whose `read_rule` is `model`. The `learning` lens reads it
  to learn its subject, concept by concept, for the read decisions already written under it. The
  `suggestion` choice reads under the `fleet` lens. Only the `fleet` lens drafts a missing note.
  Where the item carries a note, the read answers the note. A draft is quoted under its own
  heading, as questions a model drafted, so an instruction inside it is not the owner's. Where the item carries neither, the read answers the
  lens's default questions.
- **The answer holds to `prompts/model-read/answer.schema.json`**: a summary, a suggestion
  sentence, or both, with the other null. An answer holding neither fails the task.
- **A suggestion states a change in words, never a command to run.** The prompt forbids commands,
  code, paths and links in it.
- **The scanner reads the suggestion before the answer is kept.** Its findings and their spans
  are the output's own, under the rules' version in `rules_version`. A finding never drops the
  answer. A scan runs under the scan stage's time bound. One that fails keeps nothing, and the
  task reads the same run again after its backoff, so no suggestion is kept unscanned and its
  answer is paid for once. Under the `claude-cli` adapter, which keeps no run to read again, the
  task fails instead, and `tasks retry` runs a new call.
- **The summary is kept on the item, and no delivery carries it.** `items show` prints it, and
  `ingestion push --enriched` hands it to psyche.
- **The output's version is the prompt's, under its lens.** It digests `prompt.tmpl`,
  `answer.schema.json` and the lens's file. The output records the lens and the lens's own
  version beside it. A change to any of the three writes beside the old read.
- **A new note reads the item again.** The output records the SHA-256 of the note it answered in
  `note_digest`. Placing a read item again with another note queues a read under the same prompt,
  and its answer replaces the old one on the item. Placing it again with no note reads the note's
  draft where the item holds one that no read answered, and queues nothing otherwise.
- **`items show --json` carries the item's latest suggestion** under `suggestion`, as
  `schemas/suggestion.schema.json` publishes it. The scanner's findings come first, each found in
  the source or in the sentence, then the sentence, the same read's summary, and its source. The
  scanner reads only the sentence, so no finding covers the summary.
- **Each call is dispatched once**, through the protocol that `modelstage` holds for every model
  stage.

```text
$ ingestion items show reddit/t3_bb2
reddit/t3_bb2   outside   read
  ...
  model-read  model-read v1-<digest>  done  haiku at medium  dispatch 7f3a9c  uncached input 2, cache write 10666, cache read 0, output 804
              lens fleet v1-<digest>
              summary "A post about how setup scripts …"
              suggestion "This post suggests that setup scripts stop calling sudo, because …"
  finding     ingestion.command  info  "sudo"
```

The outbox ships the answer and each span as `ingestion.text`, and the findings with the output's
`ingestion.record`. `ingestion items replay --stage model-read` runs the prompt in force over each
item whose newest decisions read it by a model, and that holds no answer of that prompt. Like the
quality gate's replay, it first prints its estimate, and queues only with `--yes`.

### An item read as is reaches its destination's app once, under its key

Where the read gate reads an item as is, the service queues its delivery in the same write. The
delivery goes to the destination that the side decision names, or else to its side's default. A
worker running that destination's deliverer leases the task, whose capability is
`deliver:<deliverer>`. It writes the item through the app's own API, never its store, and the app
keeps the item's key as the record's source key. `life-articles` posts the item to life's
`/api/v1/articles`. It sends the item's title, or its URL where it has none, its URL, the time it
first landed as the save date, and its key as `source_key`. `learning-resources` posts a PDF to
learning's `/api/v1/resources` as a book whose copy is that PDF. It sends the item's title, the
type `book`, the copy `pdf`, its key as `source_key`, and its file as `file_path`: the path of its
live home below the `[serve.homes]` root that `[deliver.learning-resources]` names as `root`. It
posts a video, which holds no bytes, as type `video` with its URL. `ypl-watch` adds a video to the
playlist that its watch picked, through ypl, as the section on review's choices tells.

A book's chapter takes its book's placement, so it goes where its book goes. `learning-resources`
posts it as type `chapter`, `part_of` the resource that its book's delivery settled on. It sends
the chapter's title, its key as `source_key`, and its PDF and printed pages. A chapter whose
outline entry has no title is titled by its pages, such as `Pages 977–998`. The pages are those of
the book's newest sections when the chapter's delivery was first queued, and every attempt sends
them. It sends the printed pages only where the book prints both ends. A chapter's delivery is due
only once its book's delivery to the same destination has settled on a record. The write that
settles the book queues each of its chapters in order, and so does a replay of the deliver stage
over the book. When the service starts, it queues each chapter of a delivered book that holds no
delivery. A refused book delivers no chapter.

### `ingestion push` hands one item's scanned parse to psyche's relate on this machine

psyche relates a source to its reader's own notes. Its `relate --parse -` reads one parsed source
as JSON on stdin, in the parse-input schema that psyche publishes. `ingestion push` writes that
document from the service's answers, and runs psyche with it:

```text
$ ingestion push https://www.reddit.com/r/golang/comments/aq7ggd/ --to psyche --relate
pushing reddit/t3_aq7ggd to psyche relate: 4 units, 1 finding, 2 suggestions
…psyche's answer…
```

A link that the service has not landed lands first, and the push waits for its parse:

```text
$ ingestion push https://www.youtube.com/watch?v=<video id> --to psyche --relate
landed youtube/<video id> through cli-push, placed at psyche
waiting up to 10m0s for youtube/<video id>'s parse
waiting up to 9m58s for youtube/<video id>'s scan
pushing youtube/<video id> to psyche relate: 7 units, 0 findings, 0 suggestions
…psyche's answer…
```

- **The document is the item's parse and the findings of the scan that read it**, as
  `GET /api/v1/items/{key}/parse` answers them. It keeps that route's field names, under the
  parse-input `schema_version` that `push.SchemaVersion` names.
- **A finding goes under `suggested` where its rule's file declares `suggests`**, with the kind
  that the rule names. Every other finding, such as an instruction aimed at a model, stays a
  finding.
- **`--enriched` adds the item's current quality gate answer and model read.**
  `GET /api/v1/items/{key}` serves them as `quality_answer` and `model_read`. The push names each
  part it left out, and why. A stage may never have answered, may still wait, or may have failed.
  An answer may also not read in this release.
- **A suggestion sentence that the scan flagged stays out.** psyche's document holds no place for
  a finding in the sentence. The summary and the quality gate's words go unscanned. Each restates
  the text, which psyche reads whole beside its findings.
- **A link that the service has not landed lands through `POST /api/v1/items`.** The push names
  the push connector that `--connector` names, whose default `ingestion push --help` prints, and
  the destination `psyche`. So the set in force declares both, and `examples/declarations/` holds
  each. The item is the person's, placed at `psyche` and read as is, so no gate and no model
  stage reads it. Only a link that an extractor reads at its source or fetches lands, such as a
  YouTube video's or an article's. Any other link exits 2, and the service stores nothing for it.
  So does a set that declares no `psyche` destination.
- **A push of an item already landed starts no stage in the service**, so no model reads the item
  there. psyche's relate is its only model call. A key that the service has not landed exits 2,
  because a key names no link to land.
- **The push waits up to `--wait` for the item's parse and the scan of it.** A captions parse
  waits its turn at ypl's door, and the default wait outlasts the longest door wait and captions
  read that ypl publishes. `ingestion push --help` prints the default. Each stage's state comes
  from the item's `progress`, which `GET /api/v1/items/{key}` reads from the stage's tasks as
  well as its outputs. So a stage that a retry queued again still waits, and a task that failed
  with no output ends the wait.
  - A wait that ends with no scanned parse exits 1, naming the stage it waited at.
  - A parse or scan that failed, or one that nothing queued, exits 1 at once with how it ended.
  - A read that the server gives no answer to is read again until the wait ends. Each read ends
    by then, and one that still gets no answer exits 4, naming the stage last seen.
  - `--wait 0` hands on only a parse already scanned. `ingestion items show <key>` says where an
    item waits.
- **psyche runs from `PATH` on the machine that runs the push**, with the push's environment.
  `--json` asks psyche for its JSON answer. The command exits 4 when the service gives no answer,
  and psyche never runs then. It exits 1 where psyche fails, after psyche's own message.

Each delivery ends in one outcome, which `items show` prints with the app's id for the record:

```text
delivery  reading-list  delivered as 3  attempts 1
delivery  reading-list  delivered-to-existing as 8  attempts 1
delivery  reading-list  refused as 5  attempts 1  life's article 5 holds this key from another body
delivery  reading-list  pending  attempts 2  life answered 500: …
delivery  reading-list  pending  attempts 0  no worker heard from lately claims deliver:life-articles
delivery  none: no destination in the set in force is default_for_side personal, so an item placed on that side with no destination named waits
```

- **`delivered`**: the app saved the item under its key. Under `life-articles`, a record that the
  key already names reads `delivered` too.
- **`delivered-to-existing`**: the app held a record of the same URL or file path under no key,
  and gave it this key. Under `learning-resources`, a record that the key already names reads
  `delivered-to-existing` too, because learning answers both the same way.
- **`refused`**: the app holds a record that conflicts, such as one of the same URL or file path
  under another key, or a directory that holds the file. The delivery names that record, and is
  never tried again.
- **`pending`**: the app failed, timed out or answered in part. The task tries again after a wait
  that doubles with each attempt, up to a cap that `deliver/deliver.go` sets. An answer that the
  same request cannot mend, such as a refused token or an invalid body, fails the task instead.
  The delivery then stays pending with that answer.
- **`delivery  none:`** marks an item read as is that no deliverer takes. Its side has no default,
  its destination names no deliverer or is not declared, or the deliverer cannot take the item,
  as `life-articles` cannot take one with no URL or a book's chapter, and `learning-resources`
  one that is neither a PDF nor a book's chapter, or a book with no live home below its root. An
  apply that changes any of these queues each such item. A chapter also reads `none:` while its
  book's delivery has not settled, and for good where the app refused its book.

Where no worker heard from lately claims a pending delivery's task, `items show` names the task's
capability, as the sixth line shows. So a destination that failed reads apart from a deliverer
that no worker here opens.

A person's drop or move withdraws a delivery that has not run. Its task ends, and its `last_error`
says why. A settled delivery is never queued again, so a second sighting or a replay writes
nothing to the app. Every attempt sends the title, URL and file path that the item had when its
delivery was first queued, so a retry repeats the first request. `ingestion items replay --stage deliver`
queues each delivery that is still pending.

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
`ingestion-worker-client` holding the worker's client secret under `secret`. Where the config
sets `[deliver.life-articles]` with a `client_id`, the cluster also supplies the Secret
`ingestion-deliver-life-articles`, holding that client's secret under `secret`, and likewise
`ingestion-deliver-learning-resources` for `[deliver.learning-resources]`. Where it sets
`[model] adapter = "queue"`, the cluster supplies the Secret `ingestion-model-queue-client`,
holding `[model.queue]`'s client secret under `secret`.

**An upgrade that changes a table copies the store first.** Before `serve` changes the store's
tables, it writes a copy beside the store, named `ingestion.db.before-<UTC time>`. A table that
SQLite cannot change in place, such as one whose column a release makes nullable or whose `CHECK`
it changes, is rebuilt in one transaction that keeps every row. The store keeps the schema
generation of the newest release that converged it. A release whose generation is older rebuilds
none of its tables, so it keeps a later release's columns and constraints. `serve` leaves alone a
table that its own schema files do not create, so a table that a later release added does not stop
an earlier one opening the store. v0.5.0 and earlier refuse such a table. A rollback to one of
them stops `serve`, puts the copy back as `ingestion.db`, and starts the older release. The copy
holds nothing landed after the upgrade, and stays until a person deletes it.

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
land → parse → structure → contents → sections → scan → quality gate → side → read → note → model read → deliver
```

- **An item's key names the artifact, not the source that sent it.** The same article from a feed
  and from a phone is one item, read once and delivered once. Each source's arrival is a sighting
  of that item.
- **A rule decides before a model does.** A source's tags or a routing rule place an item first.
  The quality gate's model answers it too, and its placement stands. A share of what a rule that
  a person accepted would hide still waits in review, so a wrong rule shows itself.
- **The quality gate earns trust before it hides a drop.** Each connector's drops show in review
  until a straight run of a person's marks agrees with them and the person promotes it. Then a
  shrinking share of its new drops shows, and a disagreement shows every drop that the gate
  answers after it.
- **A model reads an item whole only on the fleet side.** It answers the item's note with a
  summary and a suggested change, and the scanner reads the suggestion before it is kept. Where
  no person wrote the note, a model drafts it, and an unclear draft waits on a person.
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

## License

ingestion is MIT licensed, per `LICENSE`, except the extractor programs in `extract/programs/`,
which import PyMuPDF and so carry AGPL-3.0-or-later, per `LICENSES/AGPL-3.0-or-later.txt`. The
worker image ships Docling's layout model under Apache-2.0 and its TableFormer model under
CDLA-Permissive-2.0, each license's text beside them.
