package extract_test

import (
	"testing"

	"github.com/datapointchris/ingestion/extract"
	"github.com/datapointchris/ingestion/itemkey"
	_ "github.com/datapointchris/ingestion/ypl"
)

// The phone shares, or a feed links, a channel's page on YouTube. The page
// names no single video, so neither the push route nor a feed's poll lands
// it, and the push's refusal names the site.
func TestAChannelsPageLandsFromNeitherAPushNorAFeed(t *testing.T) {
	link, err := itemkey.CanonicalURL("https://www.youtube.com/@a-channel")
	if err != nil {
		t.Fatal(err)
	}
	key, err := itemkey.FromURL(link)
	if err != nil {
		t.Fatal(err)
	}
	if linked := extract.Builtin().LinkLanding(key, link); linked.Lands != "" || linked.Site != itemkey.YouTube {
		t.Errorf("a channel's page lands as %+v, want it refused on the youtube site", linked)
	}
}
