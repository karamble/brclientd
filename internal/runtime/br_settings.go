// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// brSettings is the persisted shape of settings.json in the data dir. Pointer
// fields distinguish "unset" (the Bison Relay ship default applies) from an
// explicit value, which JSON's zero value cannot. Every field maps to a
// client.Config value fixed at BR-client construction, so a change persists
// here and only takes effect on the next daemon restart.
type brSettings struct {
	SendReceiveReceipts *bool     `json:"send_receive_receipts,omitempty"`
	IdleRemoveDays      *int      `json:"idle_remove_days,omitempty"`
	IdleRemoveIgnore    *[]string `json:"idle_remove_ignore,omitempty"`
	AutoSubscribePosts  *bool     `json:"auto_subscribe_posts,omitempty"`
	AutoHandshakeDays   *int      `json:"auto_handshake_days,omitempty"`
	GCInviteDays        *int      `json:"gc_invite_days,omitempty"`
	TrackRTDTChat       *bool     `json:"track_rtdt_chat,omitempty"`

	// Advanced tuning (client.Config timing/level values).
	CompressLevel       *int `json:"compress_level,omitempty"`
	ReconnectSecs       *int `json:"reconnect_secs,omitempty"`
	GcmqMaxLifetimeSecs *int `json:"gcmq_max_lifetime_secs,omitempty"`
	GcmqUpdateSecs      *int `json:"gcmq_update_secs,omitempty"`
	GcmqInitialSecs     *int `json:"gcmq_initial_secs,omitempty"`
	TipRestartSecs      *int `json:"tip_restart_secs,omitempty"`
	TipRerequestHours   *int `json:"tip_rerequest_hours,omitempty"`
	TipMaxLifetimeHours *int `json:"tip_max_lifetime_hours,omitempty"`
	TipPayRetrySecs     *int `json:"tip_pay_retry_secs,omitempty"`
	MediateCooldownDays *int `json:"mediate_cooldown_days,omitempty"`
	MaxAutoMediate      *int `json:"max_auto_mediate,omitempty"`
	UnkxdWarnHours      *int `json:"unkxd_warn_hours,omitempty"`
}

// Defaults MUST match what Bison Relay ships (brclient/bruig) so an untouched
// install behaves identically. Anchors: brclient/config.go flag defaults and
// client/client.go. A days value of 0 disables the corresponding auto-behavior
// (client checks Interval <= 0), except GCInviteDays where the client applies
// its own 7-day default when 0.
const (
	defaultSendReceiveReceipts = true // brclient/bruig ship sendrecvreceipts on
	defaultIdleRemoveDays      = 60   // brclient autoremoveidle interval; 0 = off
	defaultAutoSubscribePosts  = true // brclient autosubposts=1
	defaultAutoHandshakeDays   = 21   // brclient autohandshake interval; 0 = off
	defaultGCInviteDays        = 7    // client.Config.GCInviteExpiration default
	// defaultTrackRTDTChat is a deliberate dcrpulse deviation from BR's false
	// default: the dashboard's in-call chat history needs the client to store
	// RTDT messages (client.Config.TrackRTDTChatMessages).
	defaultTrackRTDTChat = true

	// Advanced tuning defaults; MUST equal the client's setDefaults
	// (client/client.go). 0 is not a disable here.
	defaultCompressLevel       = 4
	defaultReconnectSecs       = 5
	defaultGcmqMaxLifetimeSecs = 10
	defaultGcmqUpdateSecs      = 1
	defaultGcmqInitialSecs     = 10
	defaultTipRestartSecs      = 60
	defaultTipRerequestHours   = 24
	defaultTipMaxLifetimeHours = 72
	defaultTipPayRetrySecs     = 12
	defaultMediateCooldownDays = 7
	defaultMaxAutoMediate      = 3
	defaultUnkxdWarnHours      = 24
)

// defaultIdleRemoveIgnore mirrors brclient's well-known bots, which must not be
// auto-unsubscribed or auto-kicked even when idle.
var defaultIdleRemoveIgnore = []string{
	"86abd31f2141b274196d481edd061a00ab7a56b61a31656775c8a590d612b966", // Oprah
	"ad716557157c1f191d8b5f8c6757ea41af49de27dc619fc87f337ca85be325ee", // GC bot
}

// brBehavior is the resolved (defaults-applied) set of runtime-changeable BR
// behavior settings, in the units the dashboard uses. It is the wire shape of
// GET /settings/behavior (returned as both "saved" and "effective") and the
// snapshot the daemon reads at construction to build client.Config.
type brBehavior struct {
	SendReceiveReceipts bool     `json:"sendReceiveReceipts"`
	IdleRemoveDays      int      `json:"idleRemoveDays"`
	IdleRemoveIgnore    []string `json:"idleRemoveIgnore"`
	AutoSubscribePosts  bool     `json:"autoSubscribePosts"`
	AutoHandshakeDays   int      `json:"autoHandshakeDays"`
	GCInviteDays        int      `json:"gcInviteDays"`
	TrackRTDTChat       bool     `json:"trackRtdtChat"`

	CompressLevel       int `json:"compressLevel"`
	ReconnectSecs       int `json:"reconnectSecs"`
	GcmqMaxLifetimeSecs int `json:"gcmqMaxLifetimeSecs"`
	GcmqUpdateSecs      int `json:"gcmqUpdateSecs"`
	GcmqInitialSecs     int `json:"gcmqInitialSecs"`
	TipRestartSecs      int `json:"tipRestartSecs"`
	TipRerequestHours   int `json:"tipRerequestHours"`
	TipMaxLifetimeHours int `json:"tipMaxLifetimeHours"`
	TipPayRetrySecs     int `json:"tipPayRetrySecs"`
	MediateCooldownDays int `json:"mediateCooldownDays"`
	MaxAutoMediate      int `json:"maxAutoMediate"`
	UnkxdWarnHours      int `json:"unkxdWarnHours"`
}

// brBehaviorUpdate is the partial POST body: only non-nil fields are changed.
type brBehaviorUpdate struct {
	SendReceiveReceipts *bool     `json:"sendReceiveReceipts,omitempty"`
	IdleRemoveDays      *int      `json:"idleRemoveDays,omitempty"`
	IdleRemoveIgnore    *[]string `json:"idleRemoveIgnore,omitempty"`
	AutoSubscribePosts  *bool     `json:"autoSubscribePosts,omitempty"`
	AutoHandshakeDays   *int      `json:"autoHandshakeDays,omitempty"`
	GCInviteDays        *int      `json:"gcInviteDays,omitempty"`
	TrackRTDTChat       *bool     `json:"trackRtdtChat,omitempty"`

	CompressLevel       *int `json:"compressLevel,omitempty"`
	ReconnectSecs       *int `json:"reconnectSecs,omitempty"`
	GcmqMaxLifetimeSecs *int `json:"gcmqMaxLifetimeSecs,omitempty"`
	GcmqUpdateSecs      *int `json:"gcmqUpdateSecs,omitempty"`
	GcmqInitialSecs     *int `json:"gcmqInitialSecs,omitempty"`
	TipRestartSecs      *int `json:"tipRestartSecs,omitempty"`
	TipRerequestHours   *int `json:"tipRerequestHours,omitempty"`
	TipMaxLifetimeHours *int `json:"tipMaxLifetimeHours,omitempty"`
	TipPayRetrySecs     *int `json:"tipPayRetrySecs,omitempty"`
	MediateCooldownDays *int `json:"mediateCooldownDays,omitempty"`
	MaxAutoMediate      *int `json:"maxAutoMediate,omitempty"`
	UnkxdWarnHours      *int `json:"unkxdWarnHours,omitempty"`
}

// brSettingsStore persists daemon settings the dashboard can change at runtime.
// Values consumed by client.Config are fixed at client construction, so a change
// takes effect only on the next daemon restart; the relaunch reads its new
// values here.
type brSettingsStore struct {
	path string
}

func newBRSettingsStore(dataDir string) *brSettingsStore {
	return &brSettingsStore{path: filepath.Join(dataDir, "settings.json")}
}

func (s *brSettingsStore) load() brSettings {
	var out brSettings
	data, err := os.ReadFile(s.path)
	if err != nil {
		return out
	}
	_ = json.Unmarshal(data, &out)
	return out
}

func (s *brSettingsStore) save(cur brSettings) error {
	data, err := json.MarshalIndent(cur, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o600)
}

// behavior resolves the persisted settings into concrete values, applying the
// BR-shipped default for any unset field.
func (s *brSettingsStore) behavior() brBehavior {
	cur := s.load()
	b := brBehavior{
		SendReceiveReceipts: defaultSendReceiveReceipts,
		IdleRemoveDays:      defaultIdleRemoveDays,
		IdleRemoveIgnore:    append([]string(nil), defaultIdleRemoveIgnore...),
		AutoSubscribePosts:  defaultAutoSubscribePosts,
		AutoHandshakeDays:   defaultAutoHandshakeDays,
		GCInviteDays:        defaultGCInviteDays,
		TrackRTDTChat:       defaultTrackRTDTChat,

		CompressLevel:       defaultCompressLevel,
		ReconnectSecs:       defaultReconnectSecs,
		GcmqMaxLifetimeSecs: defaultGcmqMaxLifetimeSecs,
		GcmqUpdateSecs:      defaultGcmqUpdateSecs,
		GcmqInitialSecs:     defaultGcmqInitialSecs,
		TipRestartSecs:      defaultTipRestartSecs,
		TipRerequestHours:   defaultTipRerequestHours,
		TipMaxLifetimeHours: defaultTipMaxLifetimeHours,
		TipPayRetrySecs:     defaultTipPayRetrySecs,
		MediateCooldownDays: defaultMediateCooldownDays,
		MaxAutoMediate:      defaultMaxAutoMediate,
		UnkxdWarnHours:      defaultUnkxdWarnHours,
	}
	if cur.SendReceiveReceipts != nil {
		b.SendReceiveReceipts = *cur.SendReceiveReceipts
	}
	if cur.IdleRemoveDays != nil {
		b.IdleRemoveDays = *cur.IdleRemoveDays
	}
	if cur.IdleRemoveIgnore != nil {
		b.IdleRemoveIgnore = append([]string(nil), *cur.IdleRemoveIgnore...)
	}
	if cur.AutoSubscribePosts != nil {
		b.AutoSubscribePosts = *cur.AutoSubscribePosts
	}
	if cur.AutoHandshakeDays != nil {
		b.AutoHandshakeDays = *cur.AutoHandshakeDays
	}
	if cur.GCInviteDays != nil {
		b.GCInviteDays = *cur.GCInviteDays
	}
	if cur.TrackRTDTChat != nil {
		b.TrackRTDTChat = *cur.TrackRTDTChat
	}
	if cur.CompressLevel != nil {
		b.CompressLevel = *cur.CompressLevel
	}
	if cur.ReconnectSecs != nil {
		b.ReconnectSecs = *cur.ReconnectSecs
	}
	if cur.GcmqMaxLifetimeSecs != nil {
		b.GcmqMaxLifetimeSecs = *cur.GcmqMaxLifetimeSecs
	}
	if cur.GcmqUpdateSecs != nil {
		b.GcmqUpdateSecs = *cur.GcmqUpdateSecs
	}
	if cur.GcmqInitialSecs != nil {
		b.GcmqInitialSecs = *cur.GcmqInitialSecs
	}
	if cur.TipRestartSecs != nil {
		b.TipRestartSecs = *cur.TipRestartSecs
	}
	if cur.TipRerequestHours != nil {
		b.TipRerequestHours = *cur.TipRerequestHours
	}
	if cur.TipMaxLifetimeHours != nil {
		b.TipMaxLifetimeHours = *cur.TipMaxLifetimeHours
	}
	if cur.TipPayRetrySecs != nil {
		b.TipPayRetrySecs = *cur.TipPayRetrySecs
	}
	if cur.MediateCooldownDays != nil {
		b.MediateCooldownDays = *cur.MediateCooldownDays
	}
	if cur.MaxAutoMediate != nil {
		b.MaxAutoMediate = *cur.MaxAutoMediate
	}
	if cur.UnkxdWarnHours != nil {
		b.UnkxdWarnHours = *cur.UnkxdWarnHours
	}
	return b
}

// applyBehavior read-modify-writes only the fields present in the update. A
// field set to its BR-shipped default is stored as unset (nil) so settings.json
// holds only genuine overrides and revert-to-default clears the key.
func (s *brSettingsStore) applyBehavior(u brBehaviorUpdate) error {
	cur := s.load()
	if u.SendReceiveReceipts != nil {
		cur.SendReceiveReceipts = clearIfDefaultBool(*u.SendReceiveReceipts, defaultSendReceiveReceipts)
	}
	if u.IdleRemoveDays != nil {
		cur.IdleRemoveDays = clearIfDefaultInt(*u.IdleRemoveDays, defaultIdleRemoveDays)
	}
	if u.IdleRemoveIgnore != nil {
		if equalStrings(*u.IdleRemoveIgnore, defaultIdleRemoveIgnore) {
			cur.IdleRemoveIgnore = nil
		} else {
			v := append([]string(nil), *u.IdleRemoveIgnore...)
			cur.IdleRemoveIgnore = &v
		}
	}
	if u.AutoSubscribePosts != nil {
		cur.AutoSubscribePosts = clearIfDefaultBool(*u.AutoSubscribePosts, defaultAutoSubscribePosts)
	}
	if u.AutoHandshakeDays != nil {
		cur.AutoHandshakeDays = clearIfDefaultInt(*u.AutoHandshakeDays, defaultAutoHandshakeDays)
	}
	if u.GCInviteDays != nil {
		cur.GCInviteDays = clearIfDefaultInt(*u.GCInviteDays, defaultGCInviteDays)
	}
	if u.TrackRTDTChat != nil {
		cur.TrackRTDTChat = clearIfDefaultBool(*u.TrackRTDTChat, defaultTrackRTDTChat)
	}
	if u.CompressLevel != nil {
		cur.CompressLevel = clearIfDefaultInt(*u.CompressLevel, defaultCompressLevel)
	}
	if u.ReconnectSecs != nil {
		cur.ReconnectSecs = clearIfDefaultInt(*u.ReconnectSecs, defaultReconnectSecs)
	}
	if u.GcmqMaxLifetimeSecs != nil {
		cur.GcmqMaxLifetimeSecs = clearIfDefaultInt(*u.GcmqMaxLifetimeSecs, defaultGcmqMaxLifetimeSecs)
	}
	if u.GcmqUpdateSecs != nil {
		cur.GcmqUpdateSecs = clearIfDefaultInt(*u.GcmqUpdateSecs, defaultGcmqUpdateSecs)
	}
	if u.GcmqInitialSecs != nil {
		cur.GcmqInitialSecs = clearIfDefaultInt(*u.GcmqInitialSecs, defaultGcmqInitialSecs)
	}
	if u.TipRestartSecs != nil {
		cur.TipRestartSecs = clearIfDefaultInt(*u.TipRestartSecs, defaultTipRestartSecs)
	}
	if u.TipRerequestHours != nil {
		cur.TipRerequestHours = clearIfDefaultInt(*u.TipRerequestHours, defaultTipRerequestHours)
	}
	if u.TipMaxLifetimeHours != nil {
		cur.TipMaxLifetimeHours = clearIfDefaultInt(*u.TipMaxLifetimeHours, defaultTipMaxLifetimeHours)
	}
	if u.TipPayRetrySecs != nil {
		cur.TipPayRetrySecs = clearIfDefaultInt(*u.TipPayRetrySecs, defaultTipPayRetrySecs)
	}
	if u.MediateCooldownDays != nil {
		cur.MediateCooldownDays = clearIfDefaultInt(*u.MediateCooldownDays, defaultMediateCooldownDays)
	}
	if u.MaxAutoMediate != nil {
		cur.MaxAutoMediate = clearIfDefaultInt(*u.MaxAutoMediate, defaultMaxAutoMediate)
	}
	if u.UnkxdWarnHours != nil {
		cur.UnkxdWarnHours = clearIfDefaultInt(*u.UnkxdWarnHours, defaultUnkxdWarnHours)
	}
	return s.save(cur)
}

func clearIfDefaultBool(v, def bool) *bool {
	if v == def {
		return nil
	}
	return &v
}

func clearIfDefaultInt(v, def int) *int {
	if v == def {
		return nil
	}
	return &v
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
