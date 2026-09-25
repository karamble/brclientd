// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"errors"
	"testing"

	"github.com/companyzero/bisonrelay/zkidentity"
)

func testUID(t *testing.T, b byte) zkidentity.ShortID {
	t.Helper()
	var id zkidentity.ShortID
	for i := range id {
		id[i] = b
	}
	return id
}

// A contact can take a nick shaped like another contact's uid. Money and
// messages addressed by uid must still reach the uid's owner.
func TestMatchRecipientResolvesAUidToItsOwnerOnly(t *testing.T) {
	victim := testUID(t, 0xab)
	attacker := testUID(t, 0xcd)
	candidates := []recipientCandidate{
		{uid: attacker, nick: victim.String()},
		{uid: attacker, nick: victim.String() + "​"},
		{uid: victim, nick: "victim"},
	}
	for i := 0; i < 20; i++ {
		got, err := matchRecipient(victim.String(), candidates)
		if err != nil || got != victim {
			t.Fatalf("uid resolved to %s, %v; want %s", got, err, victim)
		}
	}
}

func TestMatchRecipientRefusesPrefixesAndLookalikes(t *testing.T) {
	victim := testUID(t, 0xab)
	candidates := []recipientCandidate{{uid: victim, nick: "victim"}}
	for _, s := range []string{
		victim.String()[:5],
		victim.String()[:63],
		victim.String() + "​",
		"victim­",
		"Victim",
	} {
		if got, err := matchRecipient(s, candidates); err == nil {
			t.Errorf("%q resolved to %s; want no match", s, got)
		}
	}
}

func TestMatchRecipientNames(t *testing.T) {
	a, b := testUID(t, 0x01), testUID(t, 0x02)
	got, err := matchRecipient("alice", []recipientCandidate{{uid: a, nick: "alice"}, {uid: b, nick: "bob"}})
	if err != nil || got != a {
		t.Fatalf("exact nick resolved to %s, %v", got, err)
	}
	if _, err := matchRecipient("alice", []recipientCandidate{{uid: a, nick: "alice"}, {uid: b, nick: "alice"}}); !errors.Is(err, errAmbiguousRecipient) {
		t.Fatalf("duplicate nick: err = %v, want ambiguous", err)
	}
	if _, err := matchRecipient(testUID(t, 0x03).String(), []recipientCandidate{{uid: a, nick: "alice"}}); err == nil {
		t.Fatal("unknown uid resolved")
	}
}
