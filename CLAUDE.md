# ingestion

ingestion takes items in from outside sources: exports, feeds, playlists, watched folders, and
links or memos pushed from a phone. It lands each item once under one key, then parses and scans
it. It gates the item by quality and by side, and delivers it to the one place it belongs. It is a
Go service on SQLite with a leasing worker, and the same binary is the CLI that talks to it.

This repo is a product for a stranger. Someone who installs it and runs it on its defaults gets a
working tool. So nothing committed here names one person's sources, machines or other tools. A
deployment's connector declarations live outside this repo.

## Layout

`main.go` calls `cmd.Execute`. `cmd/` is the cobra command package. `root.go` builds the root
command, and each other file there is one command that adds itself to `rootCmd` from its own
`init`.

Every other package sits flat beside `main.go`, one per stage, with no `internal/` or `pkg/`. A
package is created with its stage's first code, never ahead of it. A stage's code goes in its own
package:

| Package | Holds |
| --- | --- |
| `store` | the SQLite store: items, sightings, blobs, each stage's outputs and decisions, deliveries, task leases, connector cursors, the declaration sets applied, and each keyed request's answer |
| `itemkey` | the one component that derives an item's key from any form of its artifact |
| `landing` | an artifact's bytes written under their SHA-256 or referenced under a named root, the item and one sighting per connector, and the report of blobs and rows that disagree |
| `redditexport` | the Reddit bookmarklet's export, as a store keeps it, split into one save each and landed as an item with a child per save, registered as the drop format `reddit-export`; and in `bookmarklet/`, the bookmarklet and the command that writes the page installing it |
| `extract` | one extractor per media type, each writing versioned text and units |
| `isolatedprocess` | the one door that starts a program over outsider bytes, in a temporary directory of its own with an environment that holds no secret |
| `scan` | the scanner, which runs YARA-X's `yr` over a parse's text or over one sentence, its stored findings, and `build-rules`, which converts Agent Threat Rules and tests each by its own examples |
| `qualitygate` | the model stage that reads an item's opening and answers its quality, genre, domains, themes, a one-line idea and a side guess: the write after the scan that queues its task, its prompt's data and answer for `modelstage`'s handler, and the decision that its answer appends |
| `route` | the side gate and the read gate, which serve runs in the write that ends each stage between the parse and them, once the item's newest parse is scanned, and in the write that applies a changed set: a sighting's tags, then a rule, then the quality gate's drop, review or guess at or above its confidence floor, else review, with a tag, rule or person withdrawing the item's queued quality task; a person's placement from review; `Due`, the one predicate for where an item is due a delivery, and `ModelReadDue`, the one for where it is due a model read under a lens whose prompt the build holds and that no read answering its note already holds, which each decision's write reconciles, queueing a delivery or a model read and withdrawing either where a drop or a move stops it |
| `modelread` | the model stage that reads a fleet item whole under its read decision's lens and answers its note with a summary, a suggestion sentence, or both: its prompt's data and answer for `modelstage`'s handler, the scan of the suggestion before it is kept, and its replay |
| `modelstage` | what every model stage's worker handler shares: the dispatch-once protocol, which starts a call once, stores its dispatch and follows it, and a replay's estimate |
| `accuracy` | how often the quality gate's verdict and side guess agree with each person's placement, overall and by confidence band, connector, model and prompt version, and the route that answers it |
| `connectors` | the declaration set, checked against its schemas, and its apply, the scheduler that makes each connector due, the folder scan, the drop handler each source registers under its format, the deliverer each destination names, and the side default that takes an item placed with no destination |
| `deliver` | each deliverer, registered with what it takes and how a worker opens it, which upserts an item into its destination through that destination's own API under the item's key, and how its answer maps to an outcome |
| `modelport` | the one port that carries every model call, and its `queue` and `claude-cli` adapters |
| `prompts` | each model stage's prompt template and answer schema, one directory per stage, embedded and versioned by a digest of both |
| `outbox` | the relay that ships each logged change past its mark, with outsider text on a channel of its own |
| `server` | the HTTP API that `ingestion serve` answers, and its route table, whose rows name each route's scope and what a repeat does, read by the CLI's retry and the router's key check |
| `worker` | `ingestion worker run`, which leases tasks by capability under a fencing token |
| `push` | `ingestion push`, which hands a URL or a key straight to a named destination |

`schemas/` holds the JSON Schema of each record the outbox ships, of each API answer and of each
declaration file. Its one Go file embeds them, so `connectors` checks a declaration against the
same file a catalog reads. Each store table's columns are its fields in `ingestion-record.schema.json` and
`ingestion-text.schema.json` together. The second file holds each outsider-written field. A
test in the package that writes a shape validates real output against its file. A data catalog
builds its contracts from these files, so a reader points at them rather than copying the fields.

`examples/declarations/` holds a declaration set to start from. A test reads it whole and checks
each file against its schema.

`store/` holds one `.sql` file per stage, converged on every open, with no numbered migrations.
A new table or index converges through its `CREATE … IF NOT EXISTS`. A file's `CREATE TABLE` never
changes a table that exists, so a column added to one converges through the `ALTER TABLE … ADD
COLUMN` that `converge` runs where the table lacks it, carrying the column's type, `NOT NULL` and
default and no other constraint. A stage declares each table it adds in `store/tables.go`, naming
its key, its stamp, its text columns, and whether it appends or upserts. `Open` refuses a table
that is neither declared there nor listed as unlogged, and the outbox ships every declared table.
Every write runs through `Store.Update`, and each logged table's triggers refuse a row stamped
other than by that write.

`deploy/` holds the Kubernetes manifests, and `release/` the scripts that the release job runs to
pin and publish them. The `Dockerfile` builds the one image that `serve` and `worker run` share.

## Every item passes one line of stages

```text
land → parse → scan → quality gate → side → read → model read → deliver
```

- **Each stage writes its own record, and an item's phase is derived from which records exist.** A
  push skips stages, and a model stage can wait while later model-free work runs elsewhere. A
  single status column would misread both. Kubernetes derives status from conditions for the same
  reason.
- **The store keeps the derived phase on the item's row, so the outbox ships it.** Before
  `Store.Update` commits, it settles the phase of each item whose rows the write stamped, as
  `ItemRecords.Phase` decides it. `serve` settles every item's once it listens, so a release that
  changes `Phase` ships each item it moves. A stage extends `Phase` and writes no phase of its own, and a reader
  of the records counts by the shipped phase rather than deriving it again.
- **A stage that needs a model never holds up a model-free one.** Landing, parsing, scanning, rules
  and deliveries run whatever the model budget. An item waits only at a model stage, and a waiting
  item holds no worker slot.
- **A rule placed before the model always wins over it.** A tag or a routing rule decides first.
  The model judges only what the rules leave, and drops an item only above a high confidence.
- **A drop is soft.** The item keeps its row and its one-line idea, and a person can still send it
  to a model read.
- **A push that names its destination skips the quality gate, the side gate and every model
  stage.** It still parses and scans the item.
- **A delivery writes through the destination's own API, and ingestion never queues a settled one
  again.** The destination keeps the item's key as its record's source key. A destination may
  later forget a record it held, so its own conflict answer cannot be the guard against a second
  copy. The `deliveries` row is that guard. It keeps the title and URL that the item had when the
  delivery was first queued, and every attempt sends those, so a retry repeats the first request.

## The zone records who wrote the content, and the strictest sighting wins

Outsider-written text can carry instructions aimed at whatever model or person reads it next. The
zone tells every reader downstream which text that is.

- **`outside` marks outsider-written content**: a fetched page, a post, a comment, a transcript, a
  feed entry or a book. It stays `outside` when the owner shares the link. **`owner` marks the
  owner's own words**: a voice memo or typed text.
- **The zone names who wrote the content, never who handed it in.** A connector declares its
  content's default zone. The push route reads the zone from the request: a link is `outside`, and
  audio or typed text is `owner`.
- **The strictest zone wins.** An item takes the strictest zone among its sightings. A child takes
  the stricter of its own zone and its parent's. A model's output takes its input's zone.
- **Outsider text leaves only on its own channel.** The outbox ships keys, states, hashes,
  versions, tags, zones and decisions as one record kind. It ships every outsider-written field as
  another kind, so a reader can be granted the first without the second.

## Every model call goes through the model port

`modelport` is the only package that reaches a model. A stage hands it a prompt, a JSON schema and
the prompt's version, and gets back the structured answer.

- **The port has two adapters, and refuses with neither.**
  - `queue` posts the prompt and schema to an HTTP dispatch service and reads the answer back.
  - `claude-cli` runs `claude -p` on the host with the prompt on stdin, under the argv that
    `modelport.ClaudeArgs` builds. That argv loads no settings, holds no tool, answers no
    permission prompt and keeps no session on disk. It passes `--safe-mode`, because without it
    the host's own instructions ride beside the prompt. It runs through `isolatedprocess`,
    inheriting only the variables that carry claude's own login. A call that the account's usage
    limit refused waits for the reset, and fails nothing.

  A model stage runs only where config names an adapter. With none named, the stage refuses, and
  the item waits there naming the missing setting. A stage never falls back to reaching a model
  some other way.
- **A model stage holds no tools.** A model that reads outsider text must never hold both a private
  read and a way to send data out. With no tools it holds neither.
- **Each stage's prompt and schema live in this repo, versioned.** Each output records the prompt
  version beside it, so a better prompt replays over what is already landed. A schema's matched
  names, such as the side and the destinations, are enums that ingestion writes, never values
  taken from the text.
- **A model stage dispatches once, through `modelstage`.** It sends under an idempotency key of
  item, stage, prompt version and, for a model read, the note's digest. It stores the dispatch id
  in the same write that ends the worker's lease. A task whose lease lapsed reads the run it
  already started, rather than paying for a second. So does a task whose answer a later step
  could not keep yet, such as a failed scan of the suggestion. A run that failed dispatches again
  only once `tasks retry` queues its task, under a key that also names the failed dispatch,
  because a dispatch service may answer the old key with the failed run.

## The service is the one writer, and a worker does the heavy work

- **`ingestion serve` is the single writer**, run as a one-replica StatefulSet. It holds the SQLite
  store and the blob volume, serves the API, schedules polls, runs the cheap stages and ships the
  outbox.
- **`ingestion worker run` runs the same image as a Deployment.** It leases tasks by capability
  under a fencing token. It reads and writes bytes through the service's blob routes, so a heavy
  extractor never shares memory with the writer. This is the external task pattern that Camunda
  uses.
- **An extractor whose library is Python runs as a program in the worker image.** It lives in
  `extract.ProgramDir`, and `extract.Program` registers it. It reads the item's bytes on stdin
  and writes one `{"label", "text"}` line per unit on stdout. It runs through `isolatedprocess`
  alone, as `yr` does, whose environment carries none of the worker's secrets, because the
  program reads outsider bytes. The worker marks itself undumpable for the same reason, so the
  program cannot read the worker's environment through `/proc`.
- **The scan runs `yr` from the worker image over a parse's units, joined as the parse's text
  joins them.** It scans the text as written and again as ATR's engine normalizes it: NFKC, no
  zero-width or bidirectional controls, homoglyphs folded to Latin. Each view lowers the case of
  every letter outside ASCII, because YARA-X folds only ASCII's. Each finding maps back to the
  text as written. `release/download-atr-rules` pins Agent Threat Rules to a commit, and
  `release/install-tools` pins `yr`. The image build runs `scan/build-rules`, which keeps a
  converted rule only where `yr` compiles it and it passes its own examples, and counts each drop
  by reason. Ingestion's own rules in `scan/ingestion-rules/` take ATR's shape and are written in
  YARA-X's regex dialect. A scan's version is the rules' version and the parse it read, so a new
  rule set replays beside the old, and each parse keeps its own scan. A finding never drops an
  item, and its span ships only as outsider text.
- **A parse's units are rows of `parse_units`, one per unit.** The parse's `outputs` row holds no
  text. Its whole text is the units joined by a blank line, which `store.JoinUnits` alone writes.
  A done output is kept against a later one that is not done, so a failed rerun fails only its
  task.
- **Each route's row in `server/routes.go` declares what a repeat does.** The CLI's sender reads
  the row to decide a retry, and sets the `Idempotency-Key` of a keyed route itself.
- **A CLI command whose server gave no answer exits 4**, and does nothing locally.

## Testing

`task test` runs `go test ./...`, the whole suite, which is also what CI runs. The commit hook runs
`go test -short ./...`, so a slow test checks `testing.Short()` and skips. A new test pins behavior
that a user reaches and no other test already pins.

A test that runs `yr` carries the `yr` build tag, and `validate.yml` runs those tests with the
pinned `yr` over the ATR-shaped rules in `scan/testdata/atr`. Its `image` job builds the
Dockerfile and runs `yr` in the image over a known post. Each test that reads real outsider text
takes its directory from an environment variable and skips without it, and it reports counts and
rule ids alone.

## Releasing

A push to `main` runs `release.yml`. It validates through `validate.yml`, then go-semantic-release
tags a version from the conventional-commit types, and goreleaser builds linux and darwin archives
for it. `allow-initial-development-versions` holds the major at 0. Drop that input once the API is
settled enough to promise compatibility.

A push that cut a version also builds the image `ghcr.io/datapointchris/ingestion:<version>`. It
then pushes `deploy/` as the OCI artifact `ghcr.io/datapointchris/ingestion/manifests:<version>`,
with that image pinned by digest, and signs both with cosign. `release/pin-manifests` writes the
pinned copy, and `release/install-tools` installs each tool it names at a pinned SHA-256.

## Never write the breaking-change trailer in a commit message

Those words cut a major once the repo is past 0.x. A major on a Go module with no `/vN` path is an
outage, because `go install …@latest` stops seeing the tag. The analyzer matches the words
anywhere in the message, so a commit that only discusses them cuts one too. Call it "that marker".
