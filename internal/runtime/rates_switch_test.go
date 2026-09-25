// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRatesSwitchStartsStopsAndClears(t *testing.T) {
	var running, starts, clears atomic.Int32
	run := func(ctx context.Context) {
		starts.Add(1)
		running.Add(1)
		<-ctx.Done()
		time.Sleep(10 * time.Millisecond) // a fetch finishing after the cancel
		running.Add(-1)
	}
	clear := func() {
		if running.Load() != 0 {
			t.Error("prices cleared while the fetcher still ran")
		}
		clears.Add(1)
	}
	r := newRatesSwitch(context.Background(), run, clear)

	r.Set(true)
	r.Set(true)
	if !r.On() {
		t.Fatal("not on")
	}
	deadline := time.Now().Add(time.Second)
	for running.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if starts.Load() != 1 {
		t.Fatalf("started %d fetchers", starts.Load())
	}
	r.Set(false)
	if r.On() || running.Load() != 0 || clears.Load() != 1 {
		t.Fatalf("on=%v running=%d clears=%d", r.On(), running.Load(), clears.Load())
	}
	r.Set(false)
	if clears.Load() != 1 {
		t.Fatal("stopping a stopped fetcher cleared again")
	}
	r.Set(true)
	for starts.Load() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if starts.Load() != 2 || !r.On() {
		t.Fatal("did not start again")
	}
	r.Set(false)
}

func TestExchangeRatesSettingDefaultsOnAndPersists(t *testing.T) {
	s := newBRSettingsStore(t.TempDir())
	if !s.behavior().ExchangeRates {
		t.Fatal("exchange rates off by default")
	}
	off := false
	if err := s.applyBehavior(brBehaviorUpdate{ExchangeRates: &off}); err != nil {
		t.Fatal(err)
	}
	if s.behavior().ExchangeRates {
		t.Fatal("off did not persist")
	}
	raw, _ := os.ReadFile(s.path)
	if !strings.Contains(string(raw), `"exchange_rates": false`) {
		t.Fatalf("settings.json %s", raw)
	}
	on := true
	if err := s.applyBehavior(brBehaviorUpdate{ExchangeRates: &on}); err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(s.path); strings.Contains(string(raw), "exchange_rates") {
		t.Fatalf("the default was stored: %s", raw)
	}
}

func TestRatesOffAnswersZerosWithoutAskingKraken(t *testing.T) {
	var calls atomic.Int32
	kraken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
	}))
	defer kraken.Close()
	rateState.mu.Lock()
	rateState.dcrUSD, rateState.source = 20, "kraken"
	rateState.mu.Unlock()

	s := &StatusServer{Outbound: kraken.Client()}
	s.SetRatesSwitch(newRatesSwitch(context.Background(), func(context.Context) {}, func() {}))
	rec := httptest.NewRecorder()
	s.handleRates(rec, httptest.NewRequest(http.MethodGet, "/rates", nil))

	var out struct {
		DCRUSD float64 `json:"dcr_usd"`
		BTCUSD float64 `json:"btc_usd"`
		Source string  `json:"source"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if rec.Code != http.StatusOK || out.DCRUSD != 0 || out.BTCUSD != 0 || out.Source != "" || calls.Load() != 0 {
		t.Fatalf("status %d, %+v, %d outbound calls", rec.Code, out, calls.Load())
	}
	rateState.mu.Lock()
	stale := rateState.dcrUSD
	rateState.mu.Unlock()
	if stale != 0 {
		t.Fatal("the last served price was kept")
	}
}

func TestBehaviorEndpointSwitchesExchangeRatesLive(t *testing.T) {
	s := &StatusServer{Settings: newBRSettingsStore(t.TempDir()), EffectiveBehavior: brBehavior{ExchangeRates: true}}
	r := newRatesSwitch(context.Background(), func(ctx context.Context) { <-ctx.Done() }, func() {})
	r.Set(true)
	s.SetRatesSwitch(r)

	rec := httptest.NewRecorder()
	s.handleBehavior(rec, httptest.NewRequest(http.MethodPost, "/settings/behavior", strings.NewReader(`{"exchangeRates":false}`)))
	if rec.Code != http.StatusNoContent || r.On() {
		t.Fatalf("status %d, fetcher on %v", rec.Code, r.On())
	}

	rec = httptest.NewRecorder()
	s.handleBehavior(rec, httptest.NewRequest(http.MethodGet, "/settings/behavior", nil))
	var got struct {
		Saved, Effective brBehavior
	}
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Saved.ExchangeRates || got.Effective.ExchangeRates {
		t.Fatalf("saved %v effective %v", got.Saved.ExchangeRates, got.Effective.ExchangeRates)
	}
}
