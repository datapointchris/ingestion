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
  service token reaches only the routes its scopes grant, and a worker needs
  `ingestion.tasks.claim`.

`ingestion auth login`, `auth status` and `auth logout` hold a person's login. `ingestion tasks
list` reads the service's open tasks and the workers it has heard from:

```text
TASK                              ITEM              STAGE  CAPABILITY       STATE   ATTEMPTS  LEASE  UPDATED
8a35d7545c3cb9c4cfdf1bb5128fddfd  reddit/t3_aq7ggd  parse  parse:text/html  queued  0         —      2026-11-24 21:13:50 EST

WORKER                                     VERSION  CAPABILITIES  STARTED                  LAST HEARTBEAT
ingestion-worker-7c9d8f6b4-x2kqp-0f683526  0.4.0    —             2026-11-24 21:13:50 EST  2026-11-24 21:13:53 EST
```

`--state` narrows the list to one state and repeats, `--limit` caps it, and `--json` prints the
shape in `schemas/tasks-list.schema.json`. The list shows each worker heard from lately, and
`--all-workers` lists every worker the service holds, since each process start is a worker of its
own. A command exits 4 when the service gives no answer, and 2 when a flag is wrong.

### Running it in a container

Each release publishes the image `ghcr.io/datapointchris/ingestion:<version>`. It runs `serve`
by default, and `worker run` when given those arguments. The store lives on a volume at
`/var/lib/ingestion`, and the config is read from `/etc/ingestion/config.toml`. The image runs as
uid and gid 10600.

`deploy/` holds the Kubernetes manifests: a StatefulSet of one for `serve`, and a Deployment for
`worker run`. Each release publishes them as the OCI artifact
`ghcr.io/datapointchris/ingestion/manifests:<version>`, with the image pinned by digest. Both are
signed with cosign by the release workflow. The manifests leave two things for the cluster to
supply: the ConfigMap `ingestion-config` holding `config.toml`, and the Secret
`ingestion-worker-client` holding the worker's client secret under `secret`.

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
