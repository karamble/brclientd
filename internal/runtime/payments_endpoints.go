// Copyright (c) 2015-2026 The Decred developers
// Use of this source code is governed by an ISC
// license that can be found in the LICENSE file.

package runtime

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// mintInvoiceReq is the POST /payments/invoice body.
type mintInvoiceReq struct {
	AmountAtoms int64  `json:"amountAtoms"`
	Memo        string `json:"memo,omitempty"`
}

// mintInvoiceResp is the minted invoice: the bolt11 to pay, the lnpay:// URI a
// BR client renders, and the payment hash that binds settlement to the order.
type mintInvoiceResp struct {
	PayReq      string `json:"payReq"`
	LNPayURI    string `json:"lnpayUri"`
	RHash       string `json:"rHash,omitempty"`
	AmountAtoms int64  `json:"amountAtoms"`
	Expiry      string `json:"expiry,omitempty"`
}

// invoiceStatusResp reports whether an invoice has settled.
type invoiceStatusResp struct {
	Paid      bool   `json:"paid"`
	AtomsPaid int64  `json:"atomsPaid,omitempty"`
	Error     string `json:"error,omitempty"`
}

// handlePaymentsInvoice mints an LN invoice for a docked service's page order.
// The HTTP surface (mTLS) is the trust boundary; this endpoint only creates an
// invoice - it moves no funds and reads no wallet state.
func (s *StatusServer) handlePaymentsInvoice(w http.ResponseWriter, r *http.Request) {
	pc := s.currentLNPay()
	if pc == nil {
		http.Error(w, "lightning not available", http.StatusServiceUnavailable)
		return
	}
	var req mintInvoiceReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		http.Error(w, "invalid request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.AmountAtoms <= 0 {
		http.Error(w, "amountAtoms must be positive", http.StatusBadRequest)
		return
	}
	ctx := r.Context()
	// GetInvoice takes milliatoms; cb nil (settlement is tracked via
	// /payments/invoice/wait), matching simplestore's usage.
	payReq, err := pc.GetInvoice(ctx, req.AmountAtoms*1000, nil)
	if err != nil {
		http.Error(w, "mint invoice: "+err.Error(), http.StatusBadGateway)
		return
	}
	resp := mintInvoiceResp{
		PayReq:      payReq,
		LNPayURI:    "lnpay://" + payReq,
		AmountAtoms: req.AmountAtoms,
	}
	if dec, derr := pc.DecodeInvoice(ctx, payReq); derr == nil {
		resp.RHash = hex.EncodeToString(dec.ID)
		if !dec.ExpiryTime.IsZero() {
			resp.Expiry = dec.ExpiryTime.UTC().Format(time.RFC3339)
		}
	}
	writeJSONResp(w, resp)
}

// handlePaymentsInvoiceWait long-polls until the invoice settles, expires, or
// is canceled (or the caller disconnects). Wraps DcrlnPaymentClient.TrackInvoice.
func (s *StatusServer) handlePaymentsInvoiceWait(w http.ResponseWriter, r *http.Request) {
	pc := s.currentLNPay()
	if pc == nil {
		http.Error(w, "lightning not available", http.StatusServiceUnavailable)
		return
	}
	invoice := r.URL.Query().Get("invoice")
	if invoice == "" {
		http.Error(w, "invoice is required", http.StatusBadRequest)
		return
	}
	minAtoms, _ := strconv.ParseInt(r.URL.Query().Get("minAtoms"), 10, 64)
	paid, err := pc.TrackInvoice(r.Context(), invoice, minAtoms*1000)
	resp := invoiceStatusResp{}
	if err != nil {
		resp.Error = err.Error()
	} else {
		resp.Paid = true
		resp.AtomsPaid = paid / 1000
	}
	writeJSONResp(w, resp)
}

// handlePaymentsInvoiceStatus is a one-shot settlement poll (IsInvoicePaid).
func (s *StatusServer) handlePaymentsInvoiceStatus(w http.ResponseWriter, r *http.Request) {
	pc := s.currentLNPay()
	if pc == nil {
		http.Error(w, "lightning not available", http.StatusServiceUnavailable)
		return
	}
	invoice := r.URL.Query().Get("invoice")
	if invoice == "" {
		http.Error(w, "invoice is required", http.StatusBadRequest)
		return
	}
	minAtoms, _ := strconv.ParseInt(r.URL.Query().Get("minAtoms"), 10, 64)
	resp := invoiceStatusResp{}
	if err := pc.IsInvoicePaid(r.Context(), minAtoms*1000, invoice); err != nil {
		resp.Error = err.Error()
	} else {
		resp.Paid = true
	}
	writeJSONResp(w, resp)
}

func writeJSONResp(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
