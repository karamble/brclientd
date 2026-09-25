// Copyright (c) 2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"errors"
	"fmt"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/zkidentity"
)

var errAmbiguousRecipient = errors.New("more than one contact has that nick; name the contact by uid")

// recipientCandidate is one contact as the recipient match sees it.
type recipientCandidate struct {
	uid  zkidentity.ShortID
	nick string
}

// matchRecipient picks the contact a request names. A full hex uid matches
// only that uid; anything else must equal exactly one contact's nick. Upstream
// UserByNick also accepts uid prefixes and collator-equal nicks, which lets a
// contact register a nick that shadows another contact's uid.
func matchRecipient(s string, candidates []recipientCandidate) (zkidentity.ShortID, error) {
	var uid zkidentity.ShortID
	if err := uid.FromString(s); err == nil {
		for _, c := range candidates {
			if c.uid == uid {
				return uid, nil
			}
		}
		return uid, fmt.Errorf("no contact with uid %s", s)
	}
	var found []zkidentity.ShortID
	for _, c := range candidates {
		if c.nick == s {
			found = append(found, c.uid)
		}
	}
	switch len(found) {
	case 0:
		return uid, fmt.Errorf("no contact named %q", s)
	case 1:
		return found[0], nil
	}
	return uid, errAmbiguousRecipient
}

// resolveRecipient finds the contact a request names, by uid or by exact nick
// (the alias when one is set).
func resolveRecipient(c *client.Client, s string) (*client.RemoteUser, error) {
	var uid zkidentity.ShortID
	if err := uid.FromString(s); err == nil {
		return c.UserByID(uid)
	}
	var candidates []recipientCandidate
	for _, e := range c.AddressBook() {
		ru, err := c.UserByID(e.ID.Identity)
		if err != nil {
			continue
		}
		candidates = append(candidates, recipientCandidate{uid: ru.ID(), nick: ru.Nick()})
	}
	uid, err := matchRecipient(s, candidates)
	if err != nil {
		return nil, err
	}
	return c.UserByID(uid)
}
