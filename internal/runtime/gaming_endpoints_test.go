package runtime

import (
	"regexp"
	"testing"

	"github.com/companyzero/bisonrelay/client/clientdb"
	gamingwire "github.com/karamble/dcrgaming-sdk/pkg/gaming/wire"
)

func TestGamingFramesArePartitionedFromChat(t *testing.T) {
	human := "we should document --gaming[ without hiding this sentence"
	malformed := "--gaming[v=2,game=poker]--not-base64!"
	entries := []clientdb.PMLogEntry{
		{Message: human},
		{Message: gamingwire.SampleEnvelope},
		{Message: malformed},
	}

	chat := withoutGamingFrames(append([]clientdb.PMLogEntry(nil), entries...))
	if len(chat) != 2 || chat[0].Message != human || chat[1].Message != malformed {
		t.Fatalf("chat partition = %#v", chat)
	}
}

func TestGamingFilterGuardSampleMatchesBroadRules(t *testing.T) {
	for _, pattern := range []string{`gaming`, `^--gaming\[`, `.*`} {
		re := regexp.MustCompile(pattern)
		if !re.MatchString(gamingSampleEnvelope) {
			t.Fatalf("sample did not guard pattern %q", pattern)
		}
	}
}
