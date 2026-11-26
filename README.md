# ingestion

ingestion takes items in from outside sources: exports, feeds, playlists, watched folders, and
links or memos pushed from a phone. It delivers each item once, to the one place it belongs.

On the way it parses each item and scans it for content aimed at whatever reads it next. Then it
gates the item by quality and by side. A model judges only what a rule has not already decided.
An item on the fleet side, about the software you build and run, is read whole by a model, which
answers your note on it with a summary and a suggested change.

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

[model.claude-cli]
command = "claude"  # a path, or a name on PATH

# What the quality gate asks each run for.
[model.quality]
model = "haiku"
effort = "medium"

# What the model read asks each run for.
[model.model-read]
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

[worker]
slots = 4     # tasks leased at once
lease = "10m" # how long a lease holds without a heartbeat
poll = "10s"  # how often an idle worker asks for work

# A table for each deliverer the worker opens, named for it. This one saves
# items as life articles, as a client that holds the scope life.articles.write.
[deliver.life-articles]
api = "https://life.example.com"
client_id = "life-svc-ingestion"
# token_command = ["my-token-helper", "life"]  # in place of client_id
# issuer = "..."  # the OIDC provider that signs life's tokens, if not the one above
```

`ingestion serve --help` and `ingestion worker run --help` print each setting's default.

- **`INGESTION_CLIENT_SECRET`** is the worker's client secret. It makes the worker authenticate
  with the client-credentials grant. The worker requires it, or a `token_command`.
- **`INGESTION_DELIVER_<NAME>_CLIENT_SECRET`** is the secret of the client that a
  `[deliver.<name>]` table names by `client_id`, such as
  `INGESTION_DELIVER_LIFE_ARTICLES_CLIENT_SECRET`. The worker logs a table that cannot open, such
  as one whose secret is unset, and runs on without that deliverer. Its other stages are
  unaffected.
- **`NODE_NAME`**, where set, names the node in each shipped record's resource.
- **The access token decides what a caller reaches.** A person's token reaches every route. A
  service token reaches only the routes its scopes grant. A worker needs `ingestion.tasks.claim`,
  which also reads and writes bytes through `/api/v1/blobs/{sha256}`. A scan task reads its item's
  parse through `/api/v1/tasks/{id}/parse`, under the task's lease. A service that reads items,
  or the gates' agreement with each placement, needs `ingestion.items.read`, and one that runs a
  stage again needs `ingestion.items.replay`. A
  job that applies declarations needs `ingestion.declarations.apply`, one that reads connectors
  `ingestion.connectors.read`, one that runs them `ingestion.connectors.run`, and a source that
  drops files `ingestion.items.drop`. An app that shows the review queue needs
  `ingestion.review.read`, which reads each waiting item's title, URL, quality gate answer and
  sightings' fields without `ingestion.items.read`. One that places items from it for a person
  needs `ingestion.review.place`. Releasing a connector's first fill spends the model stages on every
  item it held, so it takes `ingestion.first-fills.release`, which no other scope grants.

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
     CONNECTOR    MODE  APPLIED         LAST DELIVERY  ITEMS  WAITING  REVIEW  FAILED                                       DUE
  books-folder  folder  5519f33                     —      0        0       0       0  */15 * * * * · next 2026-11-25 00:00 EST
   feed-reader    poll  5519f33                     —      0        0       0       0       @hourly · next 2026-11-25 00:00 EST
reddit-exports    drop  5519f33  2026-11-24 23:50 EST      1        0       0       0     every 30d · next 2026-12-24 23:50 EST
```

`ingestion connectors show <name>` adds its source, its tags, its last run and its first fill.
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
  other lands the file as one item. A file that the handler refuses answers 422 and lands nothing.
  A drop that stops partway, such as on a full volume, answers 500, and dropping the file again
  lands the rest.
- **A connector's first fill runs the first of its items to reach a model stage, and holds the
  rest there.** `show` prints that item and its tokens by stage. `ingestion connectors release
  <name>` sends `PATCH /api/v1/connectors/<name>/first-fill` with `{"state": "released"}`, which
  queues every model task the fill held. The hold lives in the store, so it survives a restart. A
  connector that a later set dropped can still be released. An item that two connectors sighted
  waits while either holds it.

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

Where no tag or rule places an item, the write that ends the scan of its newest parse queues one
task of capability `quality`. An item gets that task once. The gate runs on it again only through
a replay, or through `tasks retry` on its failed task. A worker whose config names a `[model]
adapter` claims it. It reads the item's media type, title, URL and the opening of its parse, up to
the bound that `qualitygate`'s `openingRunes` sets. It puts them to the model through the prompt in
`prompts/quality/prompt.tmpl`.

- **The answer holds to `prompts/quality/answer.schema.json`.** It gives a genre, one or two
  domains, up to four themes, a quality estimate from 1 to 10 and a one-line idea. It ends with a
  verdict of `pass`, `drop` or `unsure` and a side of `personal`, `fleet` or `unsure`, each with
  its confidence. An answer that breaks the schema fails the task rather than being patched.
- **A drop is soft, and needs a confidence at or above `qualitygate.DropFloor`.** A drop below it,
  or an `unsure`, sends the item to review. A dropped item keeps its row and its one-line idea,
  and `items show` prints both.
- **The prompt's version is its number and a digest of `prompt.tmpl` and `answer.schema.json`.**
  A change to either writes the gate's outputs beside the old ones.
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

A connector's first fill holds the item's task as `first-fill` until `ingestion connectors
release`. `tasks retry` posts to `/api/v1/tasks/{id}/retries`, and its `--json` prints
`schemas/task-retry.schema.json`. It queues a failed task again, prints a queued or leased task as
it stands, and refuses a done one.

`ingestion items replay --stage quality` runs the prompt in force over each scanned item whose
side waits on a later input, and that holds no answer of that prompt. That is an item in review
for want of a rule or an answer, or one that a model dropped or sent to review. An item that a
tag, a rule, a person or a confident guess placed is left as it is. The replay first prints how
many items it would run on, and the median tokens by kind of the stage's newest stored runs times
that count. It queues only with `--yes`.

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

### The side gate places each item, and a person places what it leaves

When the scan of an item's newest parse is done, the service runs the side gate in the same
write. A parse's end alone places nothing, so no item reaches a side before its scan. The gate
places the item by the first rung that decides it:

1. **Every sighting's tags.** A tag that a destination names places the item on that
   destination's side. Tags naming two sides send it to review as `tags-disagree`.
2. **A routing rule**: the first in the file whose match holds on one sighting. A Reddit save's
   sighting carries its `subreddit` field for a rule to match.
3. **The quality gate.** The item waits at `quality` until the gate answers. A drop drops it as
   `quality-drop`, and an unsure answer sends it to review as `quality-review`. A pass reads the
   gate's side guess, which places the item where its confidence reaches the floor that
   `route.GuessFloor` sets. A guess below the floor, or `unsure`, sends it to review as
   `below-floor`.
4. **Review.** An item that no tag or rule placed, and that holds no answer of the quality gate,
   waits as `no-guess` once an apply runs the side gate again. That is an item whose quality task
   failed, or whose scan could not read the set in force and so queued none.

The gate runs again in the write that ends each later stage before it, such as the quality gate,
and in the write that applies a changed set. Each run passes over an item whose newest parse is
not scanned yet. Each run decides again an item that the gate sent to
review as `no-guess`, `below-floor` or `gate-error`, or that the quality gate dropped or sent to
review, so a guess or a rule that arrives later places it. A tag, a rule or a person that places
an item withdraws its queued quality task, so no model judges an item already placed. A placed item stays where it is: a later sighting or parse moves nothing, and only a
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
sighting's tags and fields. The answer also lists each declared destination with its side, so a
reader needs no call per item. Where the set in force cannot be read, `destinations` is null and
the queue still answers. `place` and `drop` post to `/api/v1/review/{key}/place` and
`…/drop`, and print `schemas/review-placement.schema.json` with `--json`. A placement names a
destination that the set in force declares, and any other is refused. `--cause` names the aspect
that decided it, such as subreddit or title, and a decision that names none is kept as
`unstated`. `--note` carries the questions that a model read answers. Sending the same placement again changes nothing. A service placing for a person names
the person's `login` in its body, because a service token carries no person. The decision keeps
the service's client id beside the login.

### `ingestion tui` reads each waiting item whole, and one key decides it

`ingestion tui` opens a terminal board with a tab per view. Its `review` tab lists the review
queue, grouped by why each item waits. Each row shows the item's source, such as `r/golang`, the
quality gate's verdict and confidence, its quality estimate, its side guess and confidence, how
long the item has waited, and its title. The side panel reads the item under the cursor: its URL,
the gate's whole answer, the scan's findings, and its text, the post first and then each comment.
`enter` reads it full screen.

| Key | Does |
| --- | --- |
| `1`–`9` | places the item on the destination numbered with that digit under the strip |
| `x` | drops it, after asking what decided it: a field of its source such as its subreddit, its title, its theme, or none |
| `n` | writes a note in `$VISUAL` or `$EDITOR`, which a model read answers, then places the item with it on the destination picked |
| `o` | opens its URL in the browser, where it is an http or https address |

Each key posts to the routes that `review place` and `review drop` post to, under the same login,
so the decision is the person's. A decided item leaves the table at once, and the cursor moves on
through its group. A decision that fails puts the item back with the server's answer on it, and a
note that was not placed stays in its file, which the footer names. Quitting waits for every
decision sent.

A title, a body or a comment that a stranger wrote is drawn with every terminal control taken out,
so an escape sequence in it cannot set the clipboard or clear the screen. Its markdown is drawn as
typed. `--once` prints one frame at `COLUMNS` by `LINES`, and `--json` the snapshot it is drawn
from, in `schemas/tui-review.schema.json`. Where the queue was not read, each still prints, then
exits as `review list` would, 4 where the server gave no answer.

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
```

### A model read answers a fleet item's note with a summary and a suggestion

Where the read gate reads an item by a model, under a lens whose prompt this build holds, the same
write queues one task of capability `model-read`. It queues none where the item already holds a
read that answers the decision's note. A person's drop or move withdraws it where it has not run.
The task carries the read decision's lens, and the note that a person wrote with
`review place --note`. A worker whose config names a `[model]` adapter, and that runs the scan,
claims it. It reads the item's media type, title, URL and its parse's text, cut at a fixed length,
and the prompt tells the model where the text stops. It puts them to the model through
`prompts/model-read/prompt.tmpl` and the lens's file in `prompts/model-read/lenses/`.

- **The lens says what the item is read for.** The `fleet` lens reads it for the software it may
  bear on: how that software is built, run and kept safe. Where the item carries a note, the read
  answers the note. Where it carries none, it answers the lens's default questions.
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
- **The summary is kept on the item alone.** It is delivered nowhere, and `items show` prints it.
- **The output's version is the prompt's, under its lens.** It digests `prompt.tmpl`,
  `answer.schema.json` and the lens's file. The output records the lens and the lens's own
  version beside it. A change to any of the three writes beside the old read.
- **A new note reads the item again.** The output records the SHA-256 of the note it answered in
  `note_digest`. Placing a read item again with another note queues a read under the same prompt,
  and its answer replaces the old one on the item. Placing it again with no note queues none.
- **`items show --json` carries the item's latest suggestion** under `suggestion`, as
  `schemas/suggestion.schema.json` publishes it. The scanner's findings come first, each found in
  the source or in the sentence, then the sentence and its source.
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
first landed as the save date, and its key as `source_key`.

Each delivery ends in one outcome, which `items show` prints with the app's id for the record:

```text
delivery  reading-list  delivered as 3  attempts 1
delivery  reading-list  delivered-to-existing as 8  attempts 1
delivery  reading-list  refused as 5  attempts 1  life's article 5 holds this key from another body
delivery  reading-list  pending  attempts 2  life answered 500: …
delivery  reading-list  pending  attempts 0  no worker heard from lately claims deliver:life-articles
delivery  none: no destination in the set in force is default_for_side personal, so an item placed on that side with no destination named waits
```

- **`delivered`**: the app saved the item under its key, or already held it there.
- **`delivered-to-existing`**: the app held a record of the same URL under no key, and gave it
  this key.
- **`refused`**: the app holds a record that conflicts, such as one of the same URL under another
  key. The delivery names that record, and is never tried again.
- **`pending`**: the app failed, timed out or answered in part. The task tries again after a wait
  that doubles with each attempt, up to a cap that `deliver/deliver.go` sets. An answer that the
  same request cannot mend, such as a refused token or an invalid body, fails the task instead.
  The delivery then stays pending with that answer.
- **`delivery  none:`** marks an item read as is that no deliverer takes. Its side has no default,
  its destination names no deliverer or is not declared, or the deliverer cannot take the item,
  as `life-articles` cannot take one with no URL. An apply that changes any of these queues each
  such item.

Where no worker heard from lately claims a pending delivery's task, `items show` names the task's
capability, as the sixth line shows. So a destination that failed reads apart from a deliverer
that no worker here opens.

A person's drop or move withdraws a delivery that has not run. Its task ends, and its `last_error`
says why. A settled delivery is never queued again, so a second sighting or a replay writes
nothing to the app. Every attempt sends the title and URL that the item had when its delivery was
first queued, so a retry repeats the first request. `ingestion items replay --stage deliver`
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
`ingestion-deliver-life-articles`, holding that client's secret under `secret`. Where it sets
`[model] adapter = "queue"`, the cluster supplies the Secret `ingestion-model-queue-client`,
holding `[model.queue]`'s client secret under `secret`.

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
- **A model reads an item whole only on the fleet side.** It answers the item's note with a
  summary and a suggested change, and the scanner reads the suggestion before it is kept.
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
