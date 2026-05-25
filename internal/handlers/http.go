package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"time"
)

// RegisterRoutes wires all HTTP routes on the given mux.
func RegisterRoutes(mux *http.ServeMux, eng *Engine) {
	mux.HandleFunc("/acquire", eng.handleAcquire)
	mux.HandleFunc("/report", eng.handleReport)
	mux.HandleFunc("/report-and-acquire", eng.handleReportAndAcquire)
	mux.HandleFunc("/status", eng.handleStatus)
	mux.HandleFunc("/healthz", eng.handleHealthz)
}

// ---- POST /acquire ----

func (e *Engine) handleAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req AcquireRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ApplianceID == "" {
		http.Error(w, "bad request: appliance_id required", http.StatusBadRequest)
		return
	}
	if req.PriorityClass == "" {
		req.PriorityClass = "P2_MEDIUM"
	}

	token, ch, code := e.TryAcquire(req)

	switch code {
	case 409:
		http.Error(w, `{"error":"appliance already holds an active ticket"}`, http.StatusConflict)
		return
	case 503:
		http.Error(w, `{"error":"queue full"}`, http.StatusServiceUnavailable)
		return
	}

	if token != "" {
		// Immediate grant.
		writeJSON(w, http.StatusOK, map[string]string{"action": "PROCEED", "token": token})
		return
	}

	// Long-poll: wait up to 30 s for a slot.
	const pollTimeout = 30 * time.Second
	timer := time.NewTimer(pollTimeout)
	defer timer.Stop()

	select {
	case granted, ok := <-ch:
		if !ok || !granted {
			e.RemoveFromQueue(req.ApplianceID)
			http.Error(w, `{"error":"no slot available"}`, http.StatusServiceUnavailable)
			return
		}
		// Retrieve token stored by dispatch loop.
		tok, found := e.ConsumeQueueToken(req.ApplianceID)
		if !found {
			// Race: dispatch loop stored nothing (should not happen).
			log.Printf("[acquire] token missing after grant appliance=%s", req.ApplianceID)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"action": "PROCEED", "token": tok})

	case <-timer.C:
		e.RemoveFromQueue(req.ApplianceID)
		http.Error(w, `{"error":"timeout waiting for slot"}`, http.StatusServiceUnavailable)

	case <-r.Context().Done():
		e.RemoveFromQueue(req.ApplianceID)
		// Client disconnected; nothing to send.
	}
}

// ---- POST /report ----

func (e *Engine) handleReport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var rep Report
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if rep.Token == "" || rep.ApplianceID == "" {
		http.Error(w, "bad request: token and appliance_id required", http.StatusBadRequest)
		return
	}

	// Fire-and-forget: respond immediately, process asynchronously.
	w.WriteHeader(http.StatusNoContent)
	go e.HandleReport(rep)
}

// ---- POST /report-and-acquire ----

func (e *Engine) handleReportAndAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var rep Report
	if err := json.NewDecoder(r.Body).Decode(&rep); err != nil {
		http.Error(w, "bad request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if rep.Token == "" || rep.ApplianceID == "" {
		http.Error(w, "bad request: token and appliance_id required", http.StatusBadRequest)
		return
	}

	token, ch, code := e.TryReportAndAcquire(rep)

	switch code {
	case 409:
		http.Error(w, `{"error":"report failed: unmatched token"}`, http.StatusConflict)
		return
	case 429:
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"action": "LOCKDOWN"})
		return
	case 503:
		http.Error(w, `{"error":"queue full"}`, http.StatusServiceUnavailable)
		return
	}

	if token != "" {
		writeJSON(w, http.StatusOK, map[string]string{"action": "PROCEED", "token": token})
		return
	}

	// Long-poll for the queued acquire.
	const pollTimeout = 30 * time.Second
	timer := time.NewTimer(pollTimeout)
	defer timer.Stop()

	select {
	case granted, ok := <-ch:
		if !ok || !granted {
			e.RemoveFromQueue(rep.ApplianceID)
			http.Error(w, `{"error":"no slot available"}`, http.StatusServiceUnavailable)
			return
		}
		tok, found := e.ConsumeQueueToken(rep.ApplianceID)
		if !found {
			log.Printf("[report-and-acquire] token missing after grant appliance=%s", rep.ApplianceID)
			http.Error(w, `{"error":"internal error"}`, http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"action": "PROCEED", "token": tok})

	case <-timer.C:
		e.RemoveFromQueue(rep.ApplianceID)
		http.Error(w, `{"error":"timeout waiting for slot"}`, http.StatusServiceUnavailable)

	case <-r.Context().Done():
		e.RemoveFromQueue(rep.ApplianceID)
	}
}

// ---- GET /status ----

type statusResponse struct {
	Calibrated      bool    `json:"calibrated"`
	BucketLimit     int     `json:"bucket_limit"`
	LocalRemaining  int     `json:"local_remaining"`
	InFlight        int     `json:"in_flight"`
	Available       int     `json:"available"`
	QueueDepth      int     `json:"queue_depth"`
	LockdownActive  bool    `json:"lockdown_active"`
	LockdownUntil   *string `json:"lockdown_until,omitempty"`
	WindowActive    bool    `json:"window_active"`
	WindowEndTime   *string `json:"window_end_time,omitempty"`
	ActiveAppliances []string `json:"active_appliances"`
}

func (e *Engine) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	e.State.Lock()
	defer e.State.Unlock()

	avail := e.State.LocalRemaining - e.State.InFlight
	if avail < 0 {
		avail = 0
	}

	resp := statusResponse{
		Calibrated:     e.State.Calibrated,
		BucketLimit:    e.State.BucketLimit,
		LocalRemaining: e.State.LocalRemaining,
		InFlight:       e.State.InFlight,
		Available:      avail,
		QueueDepth:     e.Queue.Len(),
		LockdownActive: e.State.LockdownActive,
		WindowActive:   e.State.WindowActive,
	}

	if e.State.LockdownActive {
		s := e.State.LockdownUntil.UTC().Format(time.RFC3339)
		resp.LockdownUntil = &s
	}
	if e.State.WindowActive {
		s := e.State.WindowEndTime.UTC().Format(time.RFC3339)
		resp.WindowEndTime = &s
	}

	for appID := range e.State.ActiveTickets {
		resp.ActiveAppliances = append(resp.ActiveAppliances, appID)
	}

	writeJSON(w, http.StatusOK, resp)
}

// ---- GET /healthz ----

func (e *Engine) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[http] json encode error: %v", err)
	}
}
