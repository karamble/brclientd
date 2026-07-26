// Package gaming recognises the --gaming[…]-- wire envelope that games carry
// over Bison Relay.
//
// It only recognises. Deciding what a frame means belongs to whichever game
// sent it, and brclientd deliberately knows none of them: its job is to keep
// protocol traffic out of the chat surface, and to refuse to let a user filter
// silently destroy it. Both need the framing and nothing else, so this package
// has no dependency on any game's protocol.
//
// The envelope is the sibling of brmcp's --mcp[…]--, and shares its shape on
// purpose. It differs in two ways that matter here:
//
//   - It rides group chats as well as private messages. A table is 2-6 players,
//     so table traffic is a GC; invites arrive as PMs before the GC exists.
//     Guards therefore have to cover both classes, which the MCP ones do not
//     need to - brmcp is one-to-one only.
//   - It is a namespace over games rather than one tag per game, so a frame for
//     a game this installation does not have is still recognisable as protocol
//     traffic and still hidden, rather than surfacing as noise in a chat.
package gaming

import (
	"encoding/base64"
	"regexp"
	"strings"
)

// partRE matches a whole message body that is one envelope part, anchored so a
// human who mentions --gaming[ in conversation is not silently swallowed. The
// payload alphabet is restricted to base64 so a crafted body cannot smuggle a
// second tag or trailing content past the match.
//
// This mirrors brmcp's partRE. Attributes are not parsed here: recognising a
// frame must not depend on understanding its version or its game, or an old
// build would surface a newer game's traffic as chat.
var partRE = regexp.MustCompile(`^--gaming\[([^\]]*)\]--([A-Za-z0-9+/=\s]*)$`)

// SampleEnvelope is a representative frame. Hosts that let users define content
// filters must refuse any rule matching it.
//
// The consequence of getting this wrong is worse than for chat. Bison Relay
// applies filters on the live receive path, before notifications fire, so a
// rule that matches gaming frames drops them where nothing downstream can see
// it happened - taking a player out of a hand with funds escrowed, looking
// exactly like abandonment, and costing them the hand and their bond.
const SampleEnvelope = `--gaming[v=1,game=poker,gv=1,sid=0123456789abcdef,mid=0123456789abcdef,seq=1/1,exp=1783000000]--eyJhY3Rpb24iOiJmb2xkIn0=`

// IsEnvelope reports whether a message body is gaming protocol traffic.
//
// The shape alone is not enough. The payload class admits letters and
// whitespace, so a chat message beginning with something frame-shaped and
// continuing in prose would match the pattern - and being wrong here means a
// human's message silently disappearing. Requiring the payload to decode as
// base64 is what separates the two, and is the same bar brmcp sets.
//
// It stops there. Attributes are not validated and the version is not checked,
// because an installation must recognise - and so hide - traffic for a game or
// a version it does not have. Frame shape is exactly the part that never
// changes.
func IsEnvelope(text string) bool {
	m := partRE.FindStringSubmatch(strings.TrimSpace(text))
	if m == nil {
		return false
	}
	payload := strings.Join(strings.Fields(m[2]), "")
	if payload == "" {
		return false
	}
	_, err := base64.StdEncoding.DecodeString(payload)
	return err == nil
}
