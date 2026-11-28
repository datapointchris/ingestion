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

Every other package sits flat beside `main.go`, one per stage, with no `internal/` or `pkg/`. The
TUI's tabs are the one nesting, under `views/`. A package is created with its stage's first code,
never ahead of it. A stage's code goes in its own package:

| Package | Holds |
| --- | --- |
| `store` | the SQLite store: items, sightings, blobs, each stage's outputs and decisions, each book's sections, deliveries, task leases, connector cursors, the declaration sets applied, each keyed request's answer, each person's mark on an item, each gate family's decisions and the trust read from them, each drafted change to a gate's prompt, each version of a routing rule that a person accepted, the playlist that a watch picked on its decision and its delivery |
| `itemkey` | the one component that derives an item's key from any form of its artifact |
| `landing` | an artifact's bytes, written under their SHA-256 or referenced under a named root. The package lands the item and one sighting per connector, in a write of its own or in a caller's. It holds the hook that runs on a new sighting of a held item. It holds each item's source time, and the fill that reads the time from bytes already landed. Each source registers roles for its sightings' fields and metrics here. `ReadRoles` reads an item's channel, length, views, likes and comment count off its sightings through those roles. The package also holds the report of where blobs and rows disagree, and of rows that nothing names. When a write gives bytes to an item that held none, the write queues its parse again. `KeepFetched` makes a page that a parse fetched the bytes of an item that landed none. When the page is of another type, it retypes the item and queues its new type's parse |
| `redditexport` | the Reddit bookmarklet's export, as a store keeps it, split into one save each and landed as an item with a child per save, registered as the drop format `reddit-export`, with its subreddit registered as its sightings' channel; and in `bookmarklet/`, the bookmarklet and the command that writes the page installing it |
| `extract` | the parse stage. Landing queues the one primary extractor of an item's media type, and another stage queues a secondary one. Each extractor writes versioned text and units. A positional one keeps each blank unit in place. A source may register an extractor that reads its item through the source's client, because the item holds no bytes. Such an extractor may defer its parse to another extractor. `QueueDeferredParse` then queues that one's parse. Docling and Whisper share the slot group `SlotGroupHeavy`. `SlotGroups` holds each group's default limit. `Transcribe` runs `audio-whisper`. `AfterParse` routes the write that ends a parse on the output it wrote. `programs/` holds the Python programs that the image installs at `ProgramDir`. `pdf-text` writes one unit per PDF page. `pdf-docling` writes Docling's markdown of each PDF page from the image's pinned models. `pdf-structure` writes a PDF's profile for `sections`. `audio-whisper` decodes audio with the image's ffmpeg, then transcribes it with faster-whisper from the image's pinned model. `article-trafilatura` writes one unit per paragraph of a web page's main text. `LinkLanding` says how a link lands, for the push route and a feed's poll alike. A link lands at its source where a primary extractor declares its key's `LinkKind`. Its URL is the one that the extractor's `LinkURL` makes, as the source's poll lands it. A link to any other web page lands as a page. A pushed page lands with no bytes, as the media type that `PageMediaType` names. The extractor of that type sets `FetchesPage`, so its parse first GETs the item's page through `publicweb`. It keeps the page through the blob route before it reads it. When an extractor `TakesFetchedPages` of the page's type, as `pdf-text` does of a PDF, that parse ends deferred. The item then takes the page's type and that extractor's parse |
| `sections` | the structure stage and the sections that serve writes from it: the worker's handler that runs `pdf-structure` over a PDF's bytes, the outline rule and the printed-page offset and page labels, `Squashed` and `TitleKey`, which fold a title for any search or match, `FromEntries`, which kinds any producer's entries by that rule, the write that ends the structure task, which writes one `sections` row per outline entry and the book's source time from its creation date, and queues a Docling parse where the profile is code-heavy and math-light, and `Write`, which any producer of sections calls to write them and to make a chapter item per body chapter that takes its book's decisions through `route.PassToChapters` |
| `contents` | the model stage that reads the printed contents pages of a long PDF whose outline gives it no sections: the hook after a structure, which queues the task on a book's first profile and places a stored answer again on a later one, its prompt's data for `modelstage`'s handler, and the placement that serve runs on its answer, which maps each entry's printed page to a PDF page by label or offset, the offset voted by the pages its entries' titles open where the book prints no page numbers, searches near it for the title, and writes producer `contents` sections through `sections.Write` |
| `isolatedprocess` | the one door that starts a program over outsider bytes, in a temporary directory of its own with an environment that holds no secret |
| `scan` | the scanner, which runs YARA-X's `yr` over a parse's text or over one sentence, its stored findings, `build-rules`, which converts Agent Threat Rules and tests each by its own examples, and `Suggests`, which reads the kind that each of ingestion's own rules declares where its match lists what an item suggests |
| `topics` | the model stage that splits a memo into one item per topic. A memo is a recording of the owner's words in a container that `store.MemoMediaTypes` names. Its hook after the scan queues a memo's task once. Its handler keeps an answer only where its topics cover each unit once, in order. Its hook after the answer writes the `topics` rows over the parse that the answer read. It lands each topic through serve's lander under `<memo key>#<n>`, with a parse of its units and a scan. `Replayable` asks again for a memo that never split |
| `qualitygate` | the model stage that reads the opening of each outsider's item that no person placed, a tag's or rule's included. Its answer gives the item's quality, genre, domains, themes, a one-line idea and a side guess, with a sentence of reason on its verdict. Where the worker reads the `fleet` lens's context, the answer gives a fit to it too. The package holds the write after the scan that queues the task, the prompt's data and answer for `modelstage`'s handler, and the decision that the answer appends. That write starts each sighting connector's family. Its arm asks the same prompt of the model that a replay names, over labeled items. The arm's answers stay at the `quality-arm` stage, off the item's line, and decide nothing. The gate never reads an item in the owner's zone, as `store.Item.SkipsQualityGate` names one |
| `gatetrust` | each connector's family under one version of the quality gate's prompt, answered by one model at one effort, declared as an `earnedtrust.Family` at its cautious pace, and the trust it earns: a person's `low-quality` or `read` mark and whether it agrees with the gate's answer, a placement on a destination of a drop the gate showed or hid, which disagrees, a person's promote, the standing that earnedtrust reads from each family's decisions, counting each item once under its newest answer, which `Settle` reads again once serve listens and `Reanswered` once the gate answers an item again, and `Shows`, which `route` asks whether a drop shows in review or is hidden |
| `route` | the side gate and the read gate, which serve runs in the write that ends each stage between the parse and them, once the item's reading parse is scanned, in the write that applies a changed set, and in the write that lands another connector's sighting: a sighting's tags, then a declared rule, then a rule that a person accepted, a keep before a block, whose seeded review share waits in review as `rule-would-hide`, then the quality gate's drop, shown in review as `gate-would-drop` or hidden as `quality-drop` as `gatetrust` decides once per answer, its review, or its guess at or above its confidence floor where no connector declares that a person sorts its side, else review, with a person's placement withdrawing the item's queued quality task; `ReviewReasons`, the one table of why an item waits in review, which the CLI's help and the TUI read; a person's placement from review, or from a push that names its destination, which decides the read as is too, and which only a push or the psyche choice makes on a push-only destination, with a drop for low quality writing the low-quality mark; `Due`, the one predicate for where an item is due a delivery, which holds a chapter until its book's delivery settles, and `ModelReadDue`, the one for where it is due a model read under a lens whose prompt the build holds and that no read answering its note already holds, which each decision's write reconciles, queueing a delivery, the note's draft or a model read and withdrawing each where a drop or a move stops it; `AfterDraft`, the read gate's decision on the note's draft, a read of it by a model at or above `[note] floor` and a review as `note-unclear` below it, and a person's placement with no note of a drafted item, which takes the draft; and each chapter's copy of its book's newest side and read decisions, which every decision on the book passes on; and in `choices.go`, review's choices: `Choices`, the one table of the seven, which the CLI's help, the TUI's keys and the review list read, `Choose`, which places an item by one of the five that place, on the first destination whose deliverer is the choice's, the fleet side's default for suggestion and the destination named psyche for psyche, and `Archive`, which keeps an item on the personal side with no destination and read verdict `archive`, with what the person liked as its cause and the read mark |
| `note` | the model stage that drafts the note of an item read by a model where no person wrote one: its questions for the read, why the item was likely saved, how it bears on the owner's current work, a summary, an idea and a confidence, from the title, the URL and the opening of the reading parse beside the lens's context; its prompt's data and answer for `modelstage`'s handler |
| `lens` | each lens's context, the owner's current work, which the note, the model read and the quality gate's fit take beside an item: the reader that each context source registers once from its own init, as a deliverer is, the cache over it, and `slate`, which reads a slate tracker's focus and active initiatives; and in `testdata/slate/`, slate's published schemas of those two answers, which the stand-in answers hold to |
| `modelread` | the model stage that reads a fleet item whole under its read decision's lens, beside the lens's context, and answers its note, or the note's draft quoted under its own heading, with a summary, a suggestion sentence, or both: its prompt's data and answer for `modelstage`'s handler, the scan of the suggestion before it is kept, and its replay |
| `comments` | the model stage that reads what a video's top comments say when a person asks on its card: `StateOf`, how an item's read stands, which the review list and the route both answer, the route that queues one task per item, queues a failed one again and answers how the read stands, the reader of comments that a source registers by media type, and its prompt's data and answer for `modelstage`'s handler. It sits on no item's line, and its answer ships only as outsider text |
| `modelstage` | what every model stage's worker handler shares: the dispatch-once protocol, which starts a call once for an item or a prompt proposal, stores its dispatch and follows it, and a replay's estimate |
| `promptproposal` | the weekly draft of a change to the quality gate's prompt: the projection of each miss, an answer of the gate that a person's placement from review or mark went against, as keys, enums, numbers and the person's words alone; the scheduler that serve runs on `[gates.proposals] schedule`, which writes an owner `prompt_proposals` row over the misses labeled since the last done proposal, with the prompt's text, and queues its one `prompt-proposal` task; the worker's handler, whose run's exact edits must each apply once to that text; and the route that lists the proposals |
| `accuracy` | how often the quality gate's verdict and side guess agree with each label, a person's placement of an item that the gates sent to review first, overall and by confidence band, connector, model and prompt version; each arm by model beside the gate's answers on the same items; each note's draft by its confidence, kept by a person's placement, applied at the floor with no person, rewritten by a person's note, waiting in review or other; the route that answers it; and the comparisons of one answer with one label, which `promptproposal` reads too |
| `connectors` | the declaration set, checked against its schemas, against each routing rule that a person accepted, by `AcceptedRuleProblems`, which `route` asks too, and against what `sources` registers of each source: whether a worker polls it, whether it deletes, whether it reads its items' metrics again, and its rules for its connectors and their `source_parameters`; and its apply, the scheduler that makes each connector due, the folder scan, the landing of what a poll's worker reports in the write that ends its lease, which lands each arrival that can land and fails the poll on any other, with a delete queued per item where the connector declares one, `Finished`, the one test of a once connector that finished, `Admission`, which a claim asks before it leases a poll, a delete or a refresh of metrics, and which reads the keys that a refresh's lease hands its worker, `InReview`, the keys of the items in review that a source's connectors sighted, `RefreshMetrics`, which writes a refresh's metrics onto those sightings and stamps them read in the write that ends its lease, `RunPasses` and `LatestDue`, which run each schedule that serve keeps of its own, the drop handler each source registers under its format, the deliverer each destination names, and the side default that takes an item placed with no destination |
| `deliver` | each deliverer, registered with what it takes and how a worker opens it, which upserts an item into its destination through that destination's own API under the item's key, and how its answer maps to an outcome |
| `deletion` | the worker's handler for a `delete` task, which hands one connector's sighting to its source's deleter, retries a failed delete with backoff, and fails one that the source refused for good, which then holds off a later poll's delete of the item; and the deleter's interface. A delete sits on no item's line |
| `metricsrefresh` | the daily read, at each source, of the metrics of each item that waits in review, which a poll cannot carry: the scheduler that serve runs on `[review.refresh-metrics] schedule`, which queues one `refresh-metrics` task a day for each source whose connectors sighted an item in review, the worker's handler, which hands the source's refresher the keys that the claim read and reports each item's metrics however the read ends, waits where the source asked it to or gave no answer, and fails after `Attempts` otherwise, and the refresher's interface. A refresh sits on no item's line, and lands, deletes and gates nothing |
| `polling` | the worker's handler for a `poll` task, which hands the poller its connector's declaration and cursor and a way to keep an arrival's bytes through the blob route, and reports what it found, sends a poll back where the source asked it to wait, and fails it otherwise, and the poller's interface |
| `sources` | each source that this build carries, registered once from the source's own init: the rules for its connectors, the scopes of its service client, or for a source that signs in as a person, the opener of its client from a login and the password in `INGESTION_SOURCE_<NAME>_PASSWORD`, and the opener of each stage that a worker runs for it: its poller, its deleter, its refresher of metrics, the extractors that read its items through its client, and the reader of its items' comments, and each deliverer that writes through its client. `connectors` checks a set against it, and `worker run` opens one client per `[source.<name>]` table and asks it for each stage's handler |
| `freshrss` | the FreshRSS source, a client of its Google Reader API. Its client signs in through ClientLogin as the person who owns the reading list. Its poller keeps the newest crawl time as a baseline on its first poll. Each later poll lands each entry crawled since, and reads again the hour behind its cursor. The cursor holds the ids read in that hour. The poller GETs each new entry's page through `publicweb`. Where the page's refusal may clear, it holds the entry for the next poll. Where it will not, the entry lands the feed's content. It lands a YouTube video's link as the video with no bytes. When a poll stops, it reports what it read before the stop. The package also holds the rules for a freshrss connector |
| `publicweb` | the one door that GETs a page that an outsider's link names. It dials public addresses alone, and checks each after its name resolves and on every redirect. It reads no proxy and carries no credential. It refuses an answer that is not the page: a status other than 2xx, a redirect to the site's home, a media type other than HTML or one that its caller accepts, a page past `MaxPageBytes` or a bot's challenge. Each refusal says whether a later GET may reach the page. Both a FreshRSS poll and the parse of a pushed page GET their pages through it. `PageTitle` reads a page's own title as life reads one |
| `ypl` | the YouTube source, a client of ypl's API and never of YouTube: the one client of that API, which the poller, the deleter, the refresher and any extractor of a video share, the poller of a playlist's items and of a channel's uploads after a baseline, the deleter of a landed save's playlist item, the refresher, which reads again the views, likes, comments and length of each video in review in as few details reads as ypl's limit of ids allows, `classify`, the one reading of a failed call through ypl, which each stage maps to its own outcome, the rules for a ypl connector, `MediaTypeVideo`, the names of a video's fields and metrics and the roles it registers for them; the reader of a video's top comments, which `comments` asks; the reads of a video's captions and audio, the `ypl-watch` deliverer, which adds a video to the playlist that its watch picked through ypl's playlist-item insert, and waits on a spent quota without spending an attempt; and the two extractors of a video: `youtube-captions`, which parses the captions as the description and then each chapter or five-minute block, and writes its parse deferred where the video has no caption track, and `youtube-whisper`, which transcribes the audio of such a video in the same layout; and in `testdata/wire/`, the documents that ypl's own suite writes, copied from it, which a test holds the client and the review tab's playlist to |
| `modelport` | the one port that carries every model call, and its `queue` and `claude-cli` adapters |
| `prompts` | each model stage's prompt template and answer schema, one directory per stage, embedded and versioned by a digest of both |
| `outbox` | the relay that ships each logged change past its mark, with outsider text on a channel of its own |
| `server` | the HTTP API that `ingestion serve` answers. Each row of its route table names the route's scope and what a repeat does. The CLI's retry and the router's key check read those rows. `push.go` holds the keyed push route. It lands a link as an outside item through a push connector, only where an extractor reads the link's kind or fetches its page. It places the item where the push names a destination. `review.go` holds the review list, which answers each choice under the set in force, and the place, archive and drop routes. The place route takes a choice or a destination |
| `worker` | `ingestion worker run`, which leases tasks by capability under a fencing token, no more of a capability, or of a slot group's capabilities together, at once than its slot limit |
| `push` | `ingestion push`, which lands a new link through the push route placed at psyche, waits for the item's parse and scan as the item answer's `progress` reads them from their tasks and outputs, writes the scanned parse as the parse-input document that psyche's `relate --parse -` reads, and runs psyche on this machine with it; and in `testdata/`, the copy of psyche's schema that the push's test validates against |
| `views/review` | the review tab of `ingestion tui`: a reader of the review queue and the parse under the cursor, the order of the queue under each of the sorts and groupings that `store` names, a renderer of each row and of the card that a person decides from into a gotermboard frame, and the keys that pick each of review's choices, which `ChoiceBindings` binds one each, with a line each in the bar under the strip, or ask for the read of its comments, through a `Service` port, which `cmd/tui.go` fills with the CLI's own calls. The watch asks for its playlist from the list that `ypl playlists list --json` prints on the person's machine. The psyche choice hands the terminal to psyche relate on this machine while its placement is sent. Archive asks what was liked. A placement, archive or drop keeps the card as presented, and its keys' writes run through gotermboard's `keywrites` queue |

`views/` holds the TUI's tabs, one package each, so a later tab joins as its own package beside
`review`. A tab is a read model, as in CQRS: its reader builds a snapshot from the service's
routes, its renderer turns it into a frame, and `--json` prints the snapshot so a script reads
what a person reads.

`schemas/` holds the JSON Schema of each record the outbox ships, of each API answer and of each
declaration file. Its one Go file embeds and compiles them, so `connectors` checks a declaration,
and `server` the card sent with a decision, against the same file a catalog reads. Each store table's columns are its fields in `ingestion-record.schema.json` and
`ingestion-text.schema.json` together. The second file holds each outsider-written field. A
test in the package that writes a shape validates real output against its file. A data catalog
builds its contracts from these files, so a reader points at them rather than copying the fields.

`examples/declarations/` holds a declaration set to start from. A test reads it whole and checks
each file against its schema.

`store/` holds one `.sql` file per stage, converged on every open, with no numbered migrations.
A new table or index converges through its `CREATE … IF NOT EXISTS`. A file's `CREATE TABLE` never
changes a table that exists, so a column added to one converges through the `ALTER TABLE … ADD
COLUMN` that `converge` runs where the table lacks it, carrying the column's type, `NOT NULL` and
default and no other constraint. SQLite can neither make a column nullable nor change a table
constraint, such as a `CHECK`, in place. So a table whose file does either is rebuilt with every
row kept, after `converge` copies the store's file beside it. Such a change raises
`store.SchemaGeneration`. The store keeps the highest generation that converged it in `PRAGMA
user_version`, and a build of an older generation rebuilds none of its tables, so a reverted
release keeps a later one's columns. A stage declares each table it adds
in `store/tables.go`, naming its key, its stamp, its text columns, and whether it appends or
upserts. `Open` refuses a table that is neither declared there nor listed as unlogged, and the
outbox ships every declared table. Every write runs through `Store.Update`, and each logged
table's triggers refuse a row stamped other than by that write.

`deploy/` holds the Kubernetes manifests, and `release/` the scripts that the release job runs to
pin and publish them. The `Dockerfile` builds the one image that `serve` and `worker run` share.

## Every item passes one line of stages

```text
land → parse → structure → contents → sections → scan → topics → quality gate → side → read → note → model read → deliver
```

Only a memo passes the topics stage, and a memo passes no quality gate. `audio-whisper` parses
its recording, one unit per segment of speech, in the heavy slot group. Its scan then queues
`topics`. That stage's answer splits the transcript into runs of units. serve lands each run as a
topic, an item of the owner's under `<memo key>#<n>` with the memo as its parent. Each topic takes
a sighting from each of the memo's connectors and a parse of its units, and passes the line from
its scan on. The memo itself reads `split` once its topics land, and routes nothing of its own.

A PDF's structure, contents and sections run beside its scan, and only a PDF passes them. Only a
PDF of `sections.WholeFilePages` pages or more whose outline gives it no sections passes the
contents stage. A PDF whose profile is code-heavy and math-light takes a second parse, by
Docling, which its structure queues and which writes beside `pdf-text`'s. A video's parse reads
ypl's captions. Where the video has no caption track, that parse ends `deferred`, which the parse
condition reads as Unknown, so nothing after the parse runs on the description alone. The write
that ends it queues the Whisper parse, which writes beside it. `extract.AfterParse` routes that
write on the parse's own output, so a deferral queues the Whisper parse and a done parse queues
the scan. Once no parse task is open and one failed, a deferral reads False. Sections and contents
read `pdf-text`'s parse alone, and a delivery reads none. Every other reader reads the item's
reading parse: the newest done parse by an extractor that `store.PreferParse` registered, such as
Docling, and the newest done parse otherwise.

- **Each stage writes its own record, and an item's phase is derived from which records exist.** A
  tag, a rule or a person can settle an item before the quality gate reads it, and a model stage
  can wait while later model-free work runs elsewhere. A
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
  The quality gate answers the item too, so its connector's family earns trust from every
  answer, and the rule's placement stands. The model drops an item only above a high confidence.
- **The quality gate never reads the owner's own words.** It judges what an outsider wrote, so
  `store.Item.SkipsQualityGate` names each item in the owner's zone, and no scan queues its task.
  Such an item has no guess to place it. Where no tag or rule places it, the item waits in review
  as `owners-words`. A failed quality task stays `no-guess`, whatever the connector declares.
- **A rule that a person accepted is a row of `routing_rules`, and a hand-written one stays in
  the declaration set.** `AcceptRule` and `RetireRule` append a version through `PUT
  /api/v1/routing-rules/{name}` and its retire route, under `ingestion.routing-rules.write`. The
  rule rung reads each one in force after every declared rule. A block drops what it matches and
  keeps its row. Its `review_share` waits in review as `rule-would-hide` instead, by a seeded draw
  over the rule's name and the item's key, so a wrong rule shows itself and a replay draws the
  same items. Each write reopens the rule's own drops and drawn reviews, so they follow its newest
  version, and a retire brings them back to the rungs below. A rule never weighs in the review
  tab's order.
- **An accepted rule and the declaration set are checked against each other in both
  directions.** `connectors.Set.AcceptedRuleProblems` is the one check, run by `AcceptRule`
  against the set in force. `connectors.Apply` runs it in its write over each rule in force against
  the incoming set. It refuses any set that declares an accepted rule's name, retired ones
  included, so `rule:<name>` names one rule.
- **The gate hides a drop only where every family of the item has earned it.** A family is one
  connector under one prompt version, answered by one model at one effort, and declared as an
  `earnedtrust.Family`. A new model or effort earns its own trust from rung 0. Its standing is what
  earnedtrust reads from the family's `gate_trust_decisions` and its promotion, and `gate_trust`
  caches it. Each family starts on rung 0, which shows every drop in review. A person's
  agreements with its drops and one promote climb it, and any disagreement returns it to rung 0.
- **A drop is soft.** The item keeps its row and its one-line idea, and a person can still send it
  to a model read.
- **A model drafts the note where no person wrote one.** A read by a model answers a note: the
  questions it should answer and why the item was saved. Where the read carries no person's note,
  the note stage drafts one from the title, the URL and the opening of the reading parse. A draft
  at or above `[note] floor` becomes the read's note. Below it, the item waits in review as
  `note-unclear`, showing the draft's summary, relation and idea. A person's note replaces the
  draft, and a placement with no note takes it. A draft never reaches a decision's note, because a
  model wrote it over outsider text. The model read quotes it under its own heading, as questions
  that are not the owner's. Sometimes a draft over an older parse decided the read. Once the gate
  answers over the new reading parse, `route.RedraftAfterAnswer` appends the side's read again
  and drafts the note over that parse.
- **A lens's context is the owner's current work, read through a registered reader.** A
  `[lens.<name>.context]` table names a reader that the build registers, and how a worker reaches
  its API. The note and the model read under that lens read the context beside the item, cached so a
  batch reads it once. With no reader opened, each stage runs without a context. A read that
  fails costs only the context, so the stage runs beside the last context read, or none.
- **Review offers seven choices, each a route.** articles, learning and watch deliver through
  their deliverers as is. suggestion reads the item by a model under the fleet lens. psyche
  places it at psyche, and its client runs psyche relate on the person's machine, as a push does.
  Archive keeps the record with no destination and read verdict `archive`, whose phase is
  `archived`, and writes the read mark. Drop drops it, and a drop for low quality writes the
  low-quality mark. A read decision of verdict `shelf` that an older release's store holds waits
  in review as `shelved`.
- **`ingestion push` starts no model stage.** A push of an item that the service holds starts no
  stage. A push of a new link lands it as the person's, placed at psyche and read as is, so only
  its parse and scan run. psyche's relate is its only model call. Only a person runs it, on the
  machine where psyche runs, and nothing in the service calls psyche.
- **A pushed link lands only where an extractor reads it at its source, or fetches its page.**
  `youtube-captions` reads a YouTube video's link through ypl. Any other link that keys as a `url`
  lands as `text/html` with no bytes. Its `article-trafilatura` parse on the worker then GETs the
  page through `publicweb`, and serve keeps the page as the item's bytes. When the page is a PDF,
  the item becomes a PDF, and `pdf-text` parses it. The service stores only what it fetched
  itself. The push route refuses any other link before it writes. The item lands
  under the URL that its source names it by. Whichever connector lands it first, its URL is the
  same. When a feed's entry links the same page, the entry adds a sighting to the one item.
- **A pushed page's GET runs in its parse on the worker, never in serve's request.** serve is the
  store's one writer, so it sends no request to an outsider's site. `deploy/` lists each one's
  egress. The push answers without waiting on the site. When the page refuses the GET, the parse
  fails with the refusal, and the item's progress shows it. A refusal that may clear, such as a
  503, queues the parse again until its claims reach `extract.ReadAttempts`. When a feed's entry
  later lands the page's content, the failed parse queues again and reads it. When a feed's
  entry lands the page while the parse GETs it, the parse keeps nothing and runs again over the
  feed's bytes.
- **The push route tells a memo from a link by the body's opening.** An iPhone Shortcut sends a
  file's bytes under a Content-Type that it does not choose. So the route reads the first bytes.
  It reads a `{` as a link's JSON. `server.SniffMemo` reads an M4A, MP3 or WAV header as a memo. Any other body
  answers 415 `media_not_read` before anything is stored. A memo's key, `memo/<hash>`, hashes the
  caller and the `Idempotency-Key`, so a retry lands on the first attempt's item. The route
  streams the recording to the blob volume, and fingerprints the connector and the recording
  alone. So a re-share replays, however it spells `recorded_at`.
- **A push-only destination takes a push's placement or the psyche choice's alone**, such as
  `psyche`, whose program the push and the choice hand the item to. No deliverer reaches it, so
  review's destinations leave it out, a placement that names it is refused, and no tag, rule or
  side default may name it.
- **Only the watch choice places an item on the `watch` destination**, because only it picks the
  playlist. The `ypl-watch` registration sets `Playlist`. Review leaves such a destination out.
  The service refuses a placement or a push that names it. No tag, rule or side default may name
  it.
- **One delivery adds the video to one playlist.** A watch is refused where the item's delivery
  holds another playlist.
- **A delivery writes through the destination's own API, and ingestion never queues a settled one
  again.** The destination keeps the item's key as its record's source key. A destination may
  later forget a record it held, so its own conflict answer cannot be the guard against a second
  copy. The `deliveries` row is that guard. It keeps the title, URL and source time that the item
  had when the delivery was first queued, and every attempt sends those, so a retry repeats the
  first request.
- **A deliverer that writes an item by its file reads its root in serve.** Its registration sets
  `FilePath`, and serve reads `root` from its `[deliver.<name>]` table, as one of `[serve.homes]`.
  It takes only an item with a live home below that root. The `deliveries` row keeps that home's
  path beside the title, because serve alone holds the homes and a worker reads none.
- **A book's chapter is delivered only once its book's delivery has settled.** The chapter is
  written as part of its book's record, so it waits until a record holds the book. Only a
  deliverer whose registration sets `Chapters` takes a chapter, and its `Takes` and `FilePath`
  never read one, because a chapter holds no bytes or home. The write that settles the book's
  delivery queues its chapters' deliveries, in their order, and so does a replay over the book. A
  refused book queues none. `serve` queues each undelivered chapter of a delivered book once it
  listens, so a book delivered before its deliverer wrote chapters needs no replay. The
  `deliveries` row keeps the book's record id and the chapter's pages from its newest sections, so
  a retry sends what the first attempt sent.
- **An item's source time is when its artifact was made at its source**, such as a Reddit post's
  creation. A destination's own date, such as life's save date, takes it, and takes the landing
  time where the source gives none. Reddit's saved listing holds no time of the save, so a save is
  dated by its post, the earliest it could have been saved. A source registers the reader of its
  bytes' time with `landing.RegisterSourceTime`, and `serve` fills each item that landed without
  one once it listens.

## The zone records who wrote the content, and the strictest sighting wins

Outsider-written text can carry instructions aimed at whatever model or person reads it next. The
zone tells every reader downstream which text that is.

- **`outside` marks outsider-written content**: a fetched page, a post, a comment, a transcript, a
  feed entry or a book. It stays `outside` when the owner shares the link. **`owner` marks the
  owner's own words**: a voice memo or typed text.
- **The zone names who wrote the content, never who handed it in.** A connector declares its
  content's default zone. The push route sets the zone by what the push carries. It lands a link
  `outside` and a memo's recording as `owner`, so a push connector declares no zone.
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
- **Each model stage's answer names the parse that its prompt quoted.** The answer counts only
  while its stage still quotes that parse. The store's table of model stages, `modelStageQuotes`,
  declares the parse that each one quotes: the reading parse unless it names an extractor. The
  claim names that parse on the task as `parse`. The stage reads it through
  `worker.Client.QuotedParse`, and the output keeps it in `input_producer` and `input_version`.
  serve names it on a report that names none, as a worker of an older build sends.
  `store.ReadsQuotedParse` decides whether a stored answer is current, and every model stage's
  replay asks it. On the worker, `own` in `modelstage` keeps only an output over the parse that
  the claim named. So a parse at a new extractor version makes each answer over the older parse
  due again. The call over the new parse waits beside the done answer in `output_replacements`.
  The done answer stays the item's answer until the call answers. The first open at the
  generation that records inputs gives each stored answer that names no parse the parse its stage
  quotes.
- **A model stage dispatches once, through `modelstage`.** `modelport.Request.IdempotencyKey`
  builds its idempotency key and names each part. The key carries the parse, because a dispatch
  service may answer an old key with the run over the old text. A weekly draft has no item, so
  its task names its proposal. It stores the dispatch id
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
- **A video's extractors read through ypl, because a video's item holds no bytes.** A source
  registers such an extractor in its `sources.Registration`, and the worker opens it on the
  source's client. The Whisper extractor streams ypl's audio into `audio-whisper`, which runs
  through `isolatedprocess` as every program does.
- **A source's metrics are read again by a task of their own, never by a poll.** A capture's
  save has left its playlist once it lands, and a poll's arrival of an item queues a delete
  for a connector that deletes after landing. So serve queues one `refresh-metrics` task a day
  per source, and the claim hands it the keys of the items in review that the source's
  connectors sighted, as they stand then. The task's subject is its source, the `source`
  column of `tasks`. serve writes the metrics onto those sightings alone in the write that
  ends the lease, however the read ended. It stamps them read in `metrics_read_ts` and leaves
  `last_seen_ts`, the last arrival, as it is. A retry's claim hands over only the keys that no
  read took since the task was queued.
- **Docling and Whisper share the slot group `heavy`, whose default `extract.SlotGroups` holds.**
  Each holds gigabytes, so a worker runs one of their parses at a time and leaves its other slots
  to every other stage. A grouped extractor sets no `SlotLimit` of its own. A `[worker.slot_limits]`
  key names a group or an ungrouped capability, and the claim bounds a group's capabilities
  together. A group with one capability on a worker travels as that capability's bound, so a
  service that reads no slot group still answers the claim.
- **A retry that waits on the far end spends no attempt.** A handler sets `worker.Result.Wait` where
  the far end asked for the wait, and a stopping worker sets it on each task it hands back. The
  store takes that claim back off `attempts` and stamps `waiting_since_ts`. The next claim answers
  `waited_seconds`, so a handler can cap a far end's total wait, as `extract.ReadWaitCeiling`
  does.
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
