package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/datapointchris/earnedtrust/seededhash"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/route"
	"github.com/datapointchris/ingestion/server"
	"github.com/datapointchris/ingestion/store"
)

// A person's block rule on reddit-exports at review share 0.1 appends a
// version of routing_rules. The saves that waited at the quality gate before
// it, and each save that the connector lands after it, drop as the rule's
// with their rows kept, all but the seeded share. That share waits in review
// as rule-would-hide, naming the rule. A service whose token lacks
// ingestion.routing-rules.write is refused with 403. One holding it writes
// the rule for the person it names, and a second copy appends nothing. A
// retired rule decides no later save.
func TestABlockRuleDropsAConnectorsSavesButItsReviewShare(t *testing.T) {
	ctx := t.Context()
	p := newGatedPipeline(t, mustAbs(t, "../examples/declarations"))
	next := 0
	land := func(n int) []string {
		saves, keys := make([]string, 0, n), make([]string, 0, n)
		for range n {
			id := fmt.Sprintf("t3_r%03d", next)
			saves, keys, next = append(saves, redditSave(id, "misc")), append(keys, "reddit/"+id), next+1
		}
		p.landExports(t, map[string][]byte{fmt.Sprintf("export-%d", next): redditExport(saves...)})
		p.parse(t, extract.Builtin())
		p.scanFindingNothing(t)
		return keys
	}
	side := func(key string) (store.Decision, bool, string) {
		t.Helper()
		records, err := p.st.Item(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := records.NewestDecision(store.GateSide)
		return d, ok, records.Phase()
	}
	before := land(3)
	for _, key := range before {
		if d, ok, _ := side(key); ok {
			t.Fatalf("before any rule %s is %s by %s, want it waiting at the quality gate", key, d.Verdict, d.DecidedBy)
		}
	}

	out, err := runCLI(t, "gates", "routing-rules", "put", "quiet-misc", "--block", "--connector", "reddit-exports", "--review-share", "0.1",
		"--json", "--config", p.config)
	var written server.RoutingRuleAnswer
	if err != nil || json.Unmarshal([]byte(out), &written) != nil {
		t.Fatalf("routing-rules put printed %q, %v", out, err)
	}
	holdsTo(t, []byte(out), "routing-rule-version.schema.json")
	if r := written.Rule; !written.Appended || r.Sequence != 1 || r.Action != store.RuleBlock || r.DecidedBy != "person:person" ||
		p.count(t, `SELECT count(*) FROM routing_rules`) != 1 {
		t.Fatalf("routing-rules put answered %s, want version 1 of a block by the person, appended", out)
	}

	shown := 0
	for _, key := range append(before, land(60)...) {
		d, ok, phase := side(key)
		drawn := seededhash.Fraction("rule:quiet-misc", key) < 0.1
		switch {
		case !ok || d.DecidedBy != "rule:quiet-misc":
			t.Errorf("%s is decided by %s, want the rule", key, d.DecidedBy)
		case drawn && (d.Verdict != route.VerdictReview || d.Cause != route.CauseRuleWouldHide || phase != store.PhaseReview):
			t.Errorf("%s falls in the rule's share, and is %s for %s in phase %s, want review as %s", key, d.Verdict, d.Cause, phase, route.CauseRuleWouldHide)
		case !drawn && (d.Verdict != route.VerdictDrop || d.Cause != route.CauseRule || phase != "dropped"):
			t.Errorf("%s is %s for %s in phase %s, want dropped by the rule", key, d.Verdict, d.Cause, phase)
		}
		if drawn {
			shown++
		}
	}
	if shown == 0 || shown == 63 {
		t.Fatalf("the rule's share drew %d of 63 saves, so the draw shows nothing either way", shown)
	}
	out, err = runCLI(t, "review", "list", "--json", "--config", p.config)
	var waiting server.ReviewAnswer
	if err != nil || json.Unmarshal([]byte(out), &waiting) != nil {
		t.Fatalf("review list printed %q, %v", out, err)
	}
	holdsTo(t, []byte(out), "review-list.schema.json")
	marked := 0
	for _, it := range waiting.Items {
		if it.Reason != route.CauseRuleWouldHide {
			continue
		}
		marked++
		if it.Rule == nil || it.Rule.Name != "quiet-misc" || it.Rule.Destination != nil {
			t.Errorf("%s waits as %s beside rule %s, want quiet-misc, which drops", it.Key, it.Reason, jsonOf(it.Rule))
		}
	}
	if marked != shown {
		t.Errorf("review lists %d items as %s, want the %d that the share drew", marked, route.CauseRuleWouldHide, shown)
	}

	service := func(id string, scopes ...string) *http.Client {
		p.provider.Register(id, id+"-secret", scopes...)
		return (&clientcredentials.Config{ClientID: id, ClientSecret: id + "-secret", TokenURL: p.provider.URL + "/token", Scopes: scopes}).Client(ctx)
	}
	put := func(c *http.Client, body string) (int, server.RoutingRuleAnswer) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodPut, p.api.URL+"/api/v1/routing-rules/quiet-misc", strings.NewReader(body))
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		var answer server.RoutingRuleAnswer
		if resp.StatusCode == http.StatusOK {
			holdsTo(t, raw, "routing-rule-version.schema.json")
			_ = json.Unmarshal(raw, &answer)
		}
		return resp.StatusCode, answer
	}
	body := `{"action":"block","match":{"connector":"reddit-exports"},"review_share":0.1,"login":"alice","proposal":"proposal-7"}`
	if status, _ := put(service("ingestion-svc-reviewer", server.ScopeReviewRead), body); status != http.StatusForbidden {
		t.Errorf("a token without %s answered %d, want 403", server.ScopeRoutingRulesWrite, status)
	}
	writer := service("ingestion-svc-proposals", server.ScopeRoutingRulesWrite)
	status, answer := put(writer, body)
	if r := answer.Rule; status != http.StatusOK || !answer.Appended || r.Sequence != 2 || r.DecidedBy != "person:alice" ||
		r.ClientID == nil || *r.ClientID != "ingestion-svc-proposals" || r.Proposal == nil || *r.Proposal != "proposal-7" {
		t.Errorf("the service's rule for alice answered %d with %s, want version 2 by person:alice from its client", status, jsonOf(answer))
	}
	if status, answer := put(writer, body); status != http.StatusOK || answer.Appended || answer.Rule.Sequence != 2 {
		t.Errorf("the same rule again answered %d with %s, want version 2 and nothing appended", status, jsonOf(answer))
	}

	if out, err := runCLI(t, "gates", "routing-rules", "retire", "quiet-misc", "--config", p.config); err != nil ||
		!strings.HasPrefix(out, "Retired routing rule quiet-misc as version 3.") {
		t.Fatalf("routing-rules retire printed %q, %v", out, err)
	}
	if d, ok, _ := side(land(1)[0]); ok {
		t.Errorf("a save landed after the rule retired is %s by %s, want it waiting at the quality gate", d.Verdict, d.DecidedBy)
	}
	out, err = runCLI(t, "gates", "routing-rules", "list", "--json", "--config", p.config)
	var listed server.RoutingRulesAnswer
	if err != nil || json.Unmarshal([]byte(out), &listed) != nil {
		t.Fatalf("routing-rules list printed %q, %v", out, err)
	}
	holdsTo(t, []byte(out), "routing-rules-list.schema.json")
	if len(listed.Rules) != 1 || !listed.Rules[0].Retired || listed.Rules[0].Sequence != 3 {
		t.Errorf("routing-rules list answered %s, want quiet-misc retired at version 3", out)
	}
}
