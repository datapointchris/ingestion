package cmd

import (
	"context"
	"encoding/json"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/datapointchris/gotermboard"
	"github.com/datapointchris/gotermstyle"

	"github.com/datapointchris/ingestion/connectors"
	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/qualitygate"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/scan"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
	"github.com/datapointchris/ingestion/views/review"
	"github.com/datapointchris/ingestion/worker"
)

// hostileSave is a self post whose title, body and one comment carry terminal controls: OSC 52,
// which sets the clipboard, a CSI that clears the screen, and one that colors. Its body also opens
// on a markdown heading and holds a line ending in a backslash, each of which the reading pane must
// draw as typed.
func hostileSave(name, subreddit string) string {
	post := `{"kind":"t3","data":{"name":"` + name + `","author":"op","subreddit":"` + subreddit + `",` +
		`"title":"Hostile \u001b]52;c;ZWNobyBoaQ==\u0007title","selftext":"# not a heading\n\nline \u001b[2Jcleared\ndocker run \\\n  image","is_self":true,` +
		`"permalink":"/r/` + subreddit + `/comments/` + name[3:] + `/a_post/"}}`
	comment := `{"kind":"t1","data":{"author":"stranger","body":"a comment \u001b[31mred"}}`
	return `{"child":` + post + `,"thread":[{"data":{"children":[` + post + `]}},{"data":{"children":[` + comment + `]}}]}`
}

// findingOne is a scan worker that reports one finding on the first unit of each parse it reads.
type findingOne struct{ client *worker.Client }

func (findingOne) Capability() string { return scan.Capability }

func (h findingOne) Run(ctx context.Context, task worker.Task) worker.Result {
	parse, err := h.client.TaskParse(ctx, task.ID, task.LeaseToken, store.ParsePick{})
	if err != nil {
		reason := err.Error()
		return worker.Result{Outcome: server.OutcomeFailed, Error: &reason}
	}
	unit := 1
	return worker.Result{Outcome: server.OutcomeDone, Outputs: []server.ReportOutput{{
		Producer: scan.Producer, ProducerVersion: store.ScanVersion("test-rules", parse.Extractor, parse.ExtractorVersion),
		State: store.OutputDone, InputProducer: &parse.Extractor, InputVersion: &parse.ExtractorVersion,
		Findings: []store.Finding{{Rule: "test-instruction", Severity: "high", Unit: &unit, Offset: 0, Length: 7}},
		Spans:    []store.Span{{Rule: "test-instruction", Span: "Hostile"}},
	}}}
}

// runKey presses key on m and runs the command it starts, as Bubble Tea would, so a decision the
// key queues is sent before the test reads the store.
func runKey(t *testing.T, m *gotermboard.Model, key tea.KeyPressMsg) {
	t.Helper()
	_, cmd := m.Update(key)
	for cmd != nil {
		msg := cmd()
		cmd = nil
		if batch, ok := msg.(tea.BatchMsg); ok {
			for _, c := range batch {
				if c != nil {
					c()
				}
			}
		}
	}
	m.Rebuild()
}

func letter(r rune) tea.KeyPressMsg { return tea.KeyPressMsg{Code: r, Text: string(r)} }

// The review tab lists each item that waits on a person beside the quality gate's verdict, quality
// and side guess, and reads the item under the cursor whole: the gate's answer, the scan's finding,
// and its post and comment, with every terminal control a stranger wrote taken out and its markdown
// drawn as typed. A drop asks its cause and records it, and a note places on the destination its
// digit picks, each as the person's decision through the routes `ingestion review` calls.
func TestTheReviewTabShowsTheGatesAnswerAndDecidesByKey(t *testing.T) {
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	stand := newStandInDispatch(t, p.provider)
	stand.answer = answerBy(map[string]map[string]any{
		"bb2": judged(qualitygate.AnswerDrop, 0.65, connectors.SidePersonal, 0.58),
		"cc3": judged(qualitygate.AnswerUnsure, 0.4, "unsure", 0.3),
	})
	p.landExports(t, map[string][]byte{"first": redditExport(hostileSave("t3_bb2", "golang"), redditSave("t3_cc3", "rust"))})
	p.parse(t, extract.Builtin())
	p.run(t, findingOne{p.worker})
	gate := p.qualityGate(t, stand.models(t, "", ""))
	p.run(t, gate)
	if _, err := runCLI(t, "connectors", "release", "reddit-exports", "--config", p.config); err != nil {
		t.Fatal(err)
	}
	p.run(t, gate)

	out, err := runCLI(t, "tui", "--view", "review", "--json", "--config", p.config)
	if err != nil {
		t.Fatalf("tui --json failed: %v", err)
	}
	holdsTo(t, []byte(out), "tui-review.schema.json")
	var snap review.Snapshot
	if err := json.Unmarshal([]byte(out), &snap); err != nil {
		t.Fatal(err)
	}
	if len(snap.Items) != 2 || snap.Items[0].Item.Key != "reddit/t3_bb2" || snap.Items[0].Source != "r/golang" ||
		snap.Items[0].Group != "channel/r/golang" || snap.Items[0].Item.Reason != store.ReasonQualityReview || len(snap.Items[0].Item.QualityAnswer) == 0 {
		t.Fatalf("the snapshot lists %s, want both saves under the gate's review, the drop first in its channel r/golang with its answer", jsonOf(snap.Items))
	}
	if len(snap.NoteChoices) != 7 || snap.NoteChoices[2].Key != "3" || text(snap.NoteChoices[2].Choice) != route.ChoiceSuggestion ||
		text(snap.NoteChoices[5].Destination) != "reading-list" || text(snap.NoteChoices[6].Destination) != "dropped" {
		t.Errorf("the note's menu is %s, want the five placing choices keyed 1 to 5, suggestion third, then reading-list and dropped, which no choice reaches", jsonOf(snap.NoteChoices))
	}
	keyed := map[string]string{}
	for _, c := range snap.Choices {
		keyed[c.Key] = c.Choice + " → " + orDash(c.Destination)
	}
	if want := map[string]string{
		"A": "articles → —", "L": "learning → learning-plan", "S": "suggestion → work-inbox", "W": "watch → watch",
		"P": "psyche → psyche", "E": "archive → —", "d": "drop → —",
	}; !maps.Equal(keyed, want) {
		t.Errorf("the snapshot keys the choices %v, want %v", keyed, want)
	}

	t.Setenv("COLUMNS", "200")
	t.Setenv("LINES", "80")
	frame, err := runCLI(t, "tui", "--once", "--config", p.config)
	if err != nil {
		t.Fatalf("tui --once failed: %v", err)
	}
	for _, control := range []string{"52;c;", "[2J", "[31m", "\a"} {
		if strings.Contains(frame, control) {
			t.Errorf("the frame draws the stranger's control %q:\n%s", control, frame)
		}
	}
	for _, want := range []string{
		"r/golang", "drop 0.65", "personal 0.58", "unsure 0.40", "Hostile title", "exploratory",
		"test-instruction high in unit 1: Hostile", "# not a heading", "a comment red",
		"docker run \\\n",
	} {
		if !strings.Contains(frame, want) {
			t.Errorf("the frame lacks %q:\n%s", want, frame)
		}
	}
	if strings.Contains(frame, `\\`) || strings.Contains(frame, `run \ image`) {
		t.Errorf("the frame draws a line's closing backslash twice, or joins the line after it:\n%s", frame)
	}

	editor := filepath.Join(t.TempDir(), "editor")
	if err := os.WriteFile(editor, []byte("#!/bin/sh\nprintf 'why save this?\\n' >> \"$1\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("VISUAL", editor)
	editNow := func(m *gotermboard.Model, cmd *exec.Cmd, then func(*gotermboard.Model, error, []byte) tea.Cmd) tea.Cmd {
		return then(m, cmd.Run(), nil)
	}
	board := reviewTab{review.NewBoard(t.Context(), reviewService(tuiOptions{config: p.config}), review.Outside{HandOver: editNow})}
	m := gotermboard.NewModel(board, gotermstyle.PaletteFor(io.Discard))
	m.Update(tea.WindowSizeMsg{Width: 200, Height: 60})
	if err := board.ReadAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	m.Rebuild()
	if m.Cursor() != "reddit/t3_bb2" {
		t.Fatalf("the cursor starts on %q, want the first waiting save", m.Cursor())
	}
	runKey(t, m, letter('d'))
	runKey(t, m, letter('s'))
	_, side, _ := p.decisions(t, "reddit/t3_bb2")
	if side.Destination != nil || side.DecidedBy != "person:person" || side.Cause != "subreddit" ||
		side.ClientID == nil || *side.ClientID != "ingestion-cli-test" {
		t.Errorf("after d then s the hostile save holds %+v, want the person's drop for its subreddit", side)
	}
	if pr := side.Presented; pr == nil || pr.Sort != "newest" || pr.Grouping != store.GroupByChannel || pr.Rank < 1 || pr.Position != 1 || pr.Shown != 2 ||
		pr.Quality == nil || *pr.Quality != 7 || pr.Verdict == nil || *pr.Verdict != qualitygate.AnswerDrop || pr.Side == nil || *pr.Side != connectors.SidePersonal ||
		side.DecidedAfterMS == nil || *side.DecidedAfterMS < 0 {
		t.Errorf("the drop keeps presented %s after %v ms, want the first of two cards under newest and channel with the gate's drop and side guess",
			jsonOf(side.Presented), side.DecidedAfterMS)
	}

	if m.Cursor() != "reddit/t3_cc3" {
		t.Fatalf("after the drop the cursor is on %q, want the next waiting save", m.Cursor())
	}
	runKey(t, m, letter('n'))
	runKey(t, m, letter('6'))
	if _, side, read := p.decisions(t, "reddit/t3_cc3"); side.Verdict != connectors.SidePersonal || text(side.Destination) != "reading-list" ||
		side.Note == nil || *side.Note != "why save this?" || read.Verdict != connectors.ReadAsIs ||
		side.Presented == nil || side.Presented.Position != 1 || side.Presented.Shown != 1 || side.DecidedAfterMS == nil || read.Presented != nil {
		t.Errorf("after n, a note and 6 the save holds %+v and read %+v, want the person's placement on reading-list, which no choice reaches, with the note and its card", side, read)
	}
	if left := board.Snapshot(time.Now()).Items; len(left) != 0 {
		t.Errorf("after both decisions the tab still lists %s", jsonOf(left))
	}
}
