// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/companyzero/bisonrelay/client"
	"github.com/companyzero/bisonrelay/client/clientintf"
	"github.com/companyzero/bisonrelay/client/resources"
	"github.com/companyzero/bisonrelay/rpc"
	"github.com/decred/slog"
)

// defaultRemoteFulfillTimeout bounds how long an inbound resource fetch waits
// for the docked service to answer before the node falls back to serving
// nothing (ErrProviderNotFound), matching the un-hosted behaviour.
const defaultRemoteFulfillTimeout = 20 * time.Second

// resourceRequestEvent is one inbound BR resource fetch streamed to the docked
// service over GET /resources/requests. It carries the requesting user so the
// service can render per-user pages, and the form Data for --form-- submits.
type resourceRequestEvent struct {
	ID   uint64            `json:"id"`
	UID  string            `json:"uid"`
	Nick string            `json:"nick,omitempty"`
	Path []string          `json:"path"`
	Meta map[string]string `json:"meta,omitempty"`
	Data json.RawMessage   `json:"data,omitempty"`
}

// resourceReply is the docked service's answer, posted to /resources/fulfill.
// Data is the page bytes (Markdown + BR markup); Status defaults to 200.
type resourceReply struct {
	ID     uint64            `json:"id"`
	Status uint16            `json:"status,omitempty"`
	Meta   map[string]string `json:"meta,omitempty"`
	Data   []byte            `json:"data,omitempty"`
	Error  string            `json:"error,omitempty"`
}

// remoteProvider is a resources.Provider that forwards each inbound fetch to a
// single docked service and blocks for a correlated reply. With no service
// docked it returns ErrProviderNotFound, so the node serves nothing (exactly
// the un-hosted behaviour). It is the delegate the switchableProvider's
// override slot points at while a service is connected.
type remoteProvider struct {
	log     slog.Logger
	client  *client.Client
	timeout time.Duration

	mu      sync.Mutex
	sub     chan resourceRequestEvent // non-nil while a service is docked
	nextID  uint64
	pending map[uint64]chan resourceReply
}

func newRemoteProvider(log slog.Logger, c *client.Client, timeout time.Duration) *remoteProvider {
	if timeout <= 0 {
		timeout = defaultRemoteFulfillTimeout
	}
	return &remoteProvider{
		log:     log,
		client:  c,
		timeout: timeout,
		pending: make(map[uint64]chan resourceReply),
	}
}

// attach registers the docked service's event channel. It returns false when a
// service is already docked (single occupant).
func (rp *remoteProvider) attach(ch chan resourceRequestEvent) bool {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.sub != nil {
		return false
	}
	rp.sub = ch
	return true
}

// detach clears the docked service if ch is the current one. Pending fetches
// are left to time out (no channel is closed, avoiding a send/close race).
func (rp *remoteProvider) detach(ch chan resourceRequestEvent) {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	if rp.sub == ch {
		rp.sub = nil
	}
}

// docked reports whether a service is currently connected.
func (rp *remoteProvider) docked() bool {
	rp.mu.Lock()
	defer rp.mu.Unlock()
	return rp.sub != nil
}

// deliverReply routes a service reply to the waiting Fulfill call.
func (rp *remoteProvider) deliverReply(rep resourceReply) {
	rp.mu.Lock()
	rc := rp.pending[rep.ID]
	delete(rp.pending, rep.ID)
	rp.mu.Unlock()
	if rc != nil {
		rc <- rep // buffered, cap 1
	}
}

// Fulfill forwards the fetch to the docked service and awaits its reply.
func (rp *remoteProvider) Fulfill(ctx context.Context, uid clientintf.UserID,
	req *rpc.RMFetchResource) (*rpc.RMFetchResourceReply, error) {

	rp.mu.Lock()
	sub := rp.sub
	if sub == nil {
		rp.mu.Unlock()
		return nil, resources.ErrProviderNotFound
	}
	rp.nextID++
	id := rp.nextID
	rc := make(chan resourceReply, 1)
	rp.pending[id] = rc
	rp.mu.Unlock()

	nick, _ := rp.client.UserNick(uid)
	evt := resourceRequestEvent{
		ID:   id,
		UID:  uid.String(),
		Nick: nick,
		Path: req.Path,
		Meta: req.Meta,
		Data: req.Data,
	}

	timer := time.NewTimer(rp.timeout)
	defer timer.Stop()

	// Hand the event to the docked service.
	select {
	case sub <- evt:
	case <-timer.C:
		rp.cancel(id)
		return nil, resources.ErrProviderNotFound
	case <-ctx.Done():
		rp.cancel(id)
		return nil, ctx.Err()
	}

	// Await the correlated reply.
	select {
	case rep := <-rc:
		if rep.Error != "" {
			rp.log.Debugf("remote provider error for %v: %s", req.Path, rep.Error)
			return &rpc.RMFetchResourceReply{Status: rpc.ResourceStatusNotFound}, nil
		}
		status := rpc.ResourceStatus(rep.Status)
		if status == 0 {
			status = rpc.ResourceStatusOk
		}
		return &rpc.RMFetchResourceReply{Status: status, Meta: rep.Meta, Data: rep.Data}, nil
	case <-timer.C:
		rp.cancel(id)
		return nil, resources.ErrProviderNotFound
	case <-ctx.Done():
		rp.cancel(id)
		return nil, ctx.Err()
	}
}

func (rp *remoteProvider) cancel(id uint64) {
	rp.mu.Lock()
	delete(rp.pending, id)
	rp.mu.Unlock()
}
