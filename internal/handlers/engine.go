// Package handlers contains all HTTP handlers for the CAS.
// The handlers share a single *Engine which coordinates State, Queue, and DB.
package handlers

import (
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/rotenaple/ns-cas/internal/db"
	"github.com/rotenaple/ns-cas/internal/queue"
	"github.com/rotenaple/ns-cas/internal/state"
)

// Report is the payload of POST /report.
type Report struct {
	Token             string    `json:"token"`
	ApplianceID       string    `json:"appliance_id"`
	PriorityClass     string    `json:"priority_class"`
	NextPriorityClass string    `json:"next_priority_class,omitempty"`
	Backlog           int       `json:"backlog,omitempty"` // pending requests for the next acquire (combined flow)
	QueuedAt          time.Time `json:"queued_at"`
	AcquiredAt        time.Time `json:"acquired_at"`
	APISentAt         time.Time `json:"api_sent_at"`
	APIRecvAt         time.Time `json:"api_recv_at"`
	NSRemaining       int       `json:"ns_remaining"`
	NSReset           int       `json:"ns_reset"`
	NSRateLimit       int       `json:"ns_rate_limit"`
	StatusCode        int       `json:"status_code"`
	RetryAfter        int       `json:"retry_after"`
}

// AcquireRequest is the payload of POST /acquire.
type AcquireRequest struct {
	ApplianceID   string `json:"appliance_id"`
	PriorityClass string `json:"priority_class"`
	Backlog       int    `json:"backlog,omitempty"` // pending requests behind this one
}

// Engine coordinates shared state, the priority queue, and analytics DB.
type Engine struct {
	State    *state.State
	Queue    *queue.Queue
	DB       *db.DB
	MaxQueue int

	// Notifier broadcasts state-change signals to SSE subscribers.
	Notifier *Notifier

	// notifyChan is buffered; a send unblocks the dispatch loop after state changes.
	notifyChan chan struct{}
}

// NewEngine constructs an Engine and launches background goroutines.
func NewEngine(s *state.State, q *queue.Queue, database *db.DB, maxQueue int) *Engine {
	e := &Engine{
		State:      s,
		Queue:      q,
		DB:         database,
		MaxQueue:   maxQueue,
		Notifier:   NewNotifier(),
		notifyChan: make(chan struct{}, 64),
	}
	go e.ticketReaper()
	go e.dispatchLoop()
	return e
}

// notify pings the dispatch loop non-blockingly.
func (e *Engine) notify() {
	select {
	case e.notifyChan <- struct{}{}:
	default:
	}
}

// ---- Acquire ----

// TryAcquire attempts to immediately grant a ticket when quota is available,
// or enqueues a long-poll waiter. It returns a channel that will receive true
// (ticket granted) or false (timeout / shutdown / queue full).
//
// Returns:
//
//	token    – UUID string if immediately granted, empty if queued
//	ch       – nil if immediately granted, otherwise the waiter channel
//	code     – 409 if appliance already holds ticket, 503 if queue full, 0 otherwise
func (e *Engine) TryAcquire(req AcquireRequest) (token string, ch chan bool, code int) {
	e.State.Lock()
	defer e.State.Unlock()

	// Reject if appliance already has an active ticket.
	if _, exists := e.State.ActiveTickets[req.ApplianceID]; exists {
		return "", nil, 409
	}

	// Also reject if appliance is already in the wait queue (duplicate long-poll).
	if e.Queue.HasAppliance(req.ApplianceID) {
		return "", nil, 409
	}

	bp, ok := state.BasePriorities[req.PriorityClass]
	if !ok {
		bp = state.BasePriorities[state.PriorityMedium]
	}

	// Track demand for class budget allocation at the next window boundary.
	// Use the normalised class name so unknown values don't leak into DemandSum.
	demandClass := req.PriorityClass
	if _, valid := state.BasePriorities[demandClass]; !valid {
		demandClass = state.PriorityMedium
	}
	e.State.DemandSum[demandClass] += 1 + req.Backlog

	if e.State.CanDispatch() {
		// Immediate grant.
		tok := uuid.New().String()
		ticket := &state.ActiveTicket{
			Token:       tok,
			ApplianceID: req.ApplianceID,
			IssuedAt:    time.Now(),
		}
		e.State.InFlight++
		ticket.ExpectedRemaining = e.State.LocalRemaining - e.State.InFlight
		e.State.ActiveTickets[req.ApplianceID] = ticket
		log.Printf("[acquire] immediate grant appliance=%s token=%s in_flight=%d remaining=%d",
			req.ApplianceID, tok, e.State.InFlight, e.State.LocalRemaining)
		e.Notifier.Notify()
		return tok, nil, 0
	}

	// Check queue capacity.
	if e.Queue.Len() >= e.MaxQueue {
		return "", nil, 503
	}

	// Enqueue long-poll waiter.
	ch = make(chan bool, 1)
	e.Queue.Enqueue(&queue.Item{
		ApplianceID:   req.ApplianceID,
		PriorityClass: req.PriorityClass,
		BasePriority:  bp,
		QueuedAt:      time.Now(),
		Backlog:       req.Backlog,
		ResponseChan:  ch,
	})
	log.Printf("[acquire] queued appliance=%s priority=%s queue_depth=%d", req.ApplianceID, req.PriorityClass, e.Queue.Len())
	e.Notifier.Notify()
	return "", ch, 0
}

// GrantFromQueue grants a ticket to the item retrieved from the queue.
// Returns the issued token.
func (e *Engine) GrantFromQueue(item *queue.Item) string {
	tok := uuid.New().String()
	ticket := &state.ActiveTicket{
		Token:       tok,
		ApplianceID: item.ApplianceID,
		IssuedAt:    time.Now(),
	}
	e.State.InFlight++
	ticket.ExpectedRemaining = e.State.LocalRemaining - e.State.InFlight
	e.State.ActiveTickets[item.ApplianceID] = ticket
	log.Printf("[dispatch] grant appliance=%s token=%s in_flight=%d remaining=%d",
		item.ApplianceID, tok, e.State.InFlight, e.State.LocalRemaining)
	return tok
}

// RemoveFromQueue removes an appliance from the wait queue (long-poll timed out).
func (e *Engine) RemoveFromQueue(applianceID string) {
	e.State.Lock()
	e.Queue.Remove(applianceID)
	e.State.Unlock()
}

// ---- Shared report processing ----

type reportResult struct {
	ok         bool
	isLockdown bool
	logEntry   db.LogEntry
}

// processReportUnderLock handles the report processing while caller holds state.Lock().
// If ok=false, the token was unmatched and the lock has been released.
// If ok=true, the lock is still held and the caller must unlock.
func (e *Engine) processReportUnderLock(r Report) reportResult {
	ticket, ok := e.State.ActiveTickets[r.ApplianceID]
	if !ok || ticket.Token != r.Token {
		e.State.Unlock()
		log.Printf("[report] anomaly: unmatched token appliance=%s token=%s", r.ApplianceID, r.Token)
		go e.DB.WriteAnomaly("unmatched_token", r.ApplianceID, r.Token,
			fmt.Sprintf("known_token=%v", func() string {
				if ticket != nil {
					return ticket.Token
				}
				return "none"
			}()))
		return reportResult{ok: false}
	}

	delete(e.State.ActiveTickets, r.ApplianceID)
	e.State.InFlight--

	if r.NSRateLimit > 0 {
		e.State.BucketLimit = r.NSRateLimit
	}

	if !e.State.Calibrated {
		e.State.LocalRemaining = r.NSRemaining
		e.State.Calibrated = true
		log.Printf("[report] calibrated: remaining=%d bucket=%d", r.NSRemaining, e.State.BucketLimit)
	}

	if r.StatusCode == 429 {
		e.State.LockdownActive = true
		e.State.LockdownUntil = time.Now().Add(time.Duration(r.RetryAfter) * time.Second)
		e.State.LocalRemaining = 0
		log.Printf("[report] 429 lockdown for %ds", r.RetryAfter)
		snapTicket := *ticket
		return reportResult{
			ok:         true,
			isLockdown: true,
			logEntry:   buildEntry(r, snapTicket, 0),
		}
	}

	impliedLegacy := 0
	if v := ticket.ExpectedRemaining - 1 - r.NSRemaining; v > 0 {
		impliedLegacy = v
	}
	if impliedLegacy > 0 {
		log.Printf("[report] legacy usage detected: implied=%d appliance=%s", impliedLegacy, r.ApplianceID)
	}

	if r.NSRemaining < e.State.LocalRemaining {
		e.State.LocalRemaining = r.NSRemaining
	}

	windowDetected := e.tryPrimaryWindowDetection(r)
	if !windowDetected {
		e.tryFallbackWindowDetection(r)
	}

	snapTicket := *ticket
	return reportResult{
		ok:         true,
		isLockdown: false,
		logEntry:   buildEntry(r, snapTicket, impliedLegacy),
	}
}

// ---- dispatchLoop ----

// dispatchLoop drains the priority queue whenever state changes are signalled.
// It runs in a dedicated goroutine and is the only place Queue.PopBest is called.
// Each iteration checks both the overall CanDispatch gate and the per-class quota.
// If the best-priority item's class (and the shared pool) are both exhausted the loop
// breaks immediately — remaining items wait for the next window reset.
func (e *Engine) dispatchLoop() {
	for range e.notifyChan {
		e.State.Lock()
		for e.State.CanDispatch() && e.Queue.Len() > 0 {
			item := e.Queue.PopBest()

			if e.State.HasClassQuota(item.PriorityClass) {
				e.State.DeductClassQuota(item.PriorityClass)
			} else if e.State.HasSharedQuota() {
				e.State.DeductShared()
			} else {
				// Neither class quota nor shared pool has capacity; re-queue the
				// item and stop — no further dispatches until the next window.
				e.Queue.Enqueue(item)
				break
			}

			tok := e.GrantFromQueue(item)
			select {
			case item.ResponseChan <- true:
				e.State.Unlock()
				e.storeQueueToken(item.ApplianceID, tok)
				e.State.Lock()
			default:
				// Handler already gone (timed out); reclaim slot.
				delete(e.State.ActiveTickets, item.ApplianceID)
				e.State.InFlight--
			}
		}
		e.State.Unlock()
		e.Notifier.Notify()
	}
}

// queueTokens maps applianceID → token for items granted by the dispatch loop.
// Protected by queueTokenMu.
var (
	queueTokensMu = make(chan struct{}, 1)
	queueTokenMap = map[string]string{}
)

func init() { queueTokensMu <- struct{}{} }

func (e *Engine) storeQueueToken(applianceID, token string) {
	<-queueTokensMu
	queueTokenMap[applianceID] = token
	queueTokensMu <- struct{}{}
}

// ConsumeQueueToken retrieves and deletes the token stored for a just-woken waiter.
func (e *Engine) ConsumeQueueToken(applianceID string) (string, bool) {
	<-queueTokensMu
	tok, ok := queueTokenMap[applianceID]
	if ok {
		delete(queueTokenMap, applianceID)
	}
	queueTokensMu <- struct{}{}
	return tok, ok
}

// ---- Telemetry ----

// HandleReport processes a /report payload. It updates State, logs to DB,
// detects window boundaries, and notifies the dispatch loop.
func (e *Engine) HandleReport(r Report) {
	e.State.Lock()
	res := e.processReportUnderLock(r)
	if !res.ok {
		return
	}
	e.State.Unlock()
	go e.DB.WriteLog(res.logEntry)
	e.notify()
	e.Notifier.Notify()
}

// TryReportAndAcquire processes a report and immediately attempts to acquire
// the next ticket for the same appliance, all under a single lock acquisition.
// Returns the same shape as TryAcquire: (token, ch, code).
func (e *Engine) TryReportAndAcquire(r Report) (token string, ch chan bool, code int) {
	if r.NextPriorityClass == "" {
		r.NextPriorityClass = "P2_MEDIUM"
	}

	e.State.Lock()
	res := e.processReportUnderLock(r)
	if !res.ok {
		return "", nil, 409
	}
	if res.isLockdown {
		e.State.Unlock()
		go e.DB.WriteLog(res.logEntry)
		e.notify()
		e.Notifier.Notify()
		return "", nil, 429
	}

	// Track demand for the next acquire's class so the window budget is informed.
	nextClass := r.NextPriorityClass
	if _, valid := state.BasePriorities[nextClass]; !valid {
		nextClass = state.PriorityMedium
	}
	e.State.DemandSum[nextClass] += 1 + r.Backlog

	// Acquire logic — lock still held, active ticket already freed by report.
	if e.State.CanDispatch() {
		tok := uuid.New().String()
		ticket := &state.ActiveTicket{
			Token:       tok,
			ApplianceID: r.ApplianceID,
			IssuedAt:    time.Now(),
		}
		e.State.InFlight++
		ticket.ExpectedRemaining = e.State.LocalRemaining - e.State.InFlight
		e.State.ActiveTickets[r.ApplianceID] = ticket
		log.Printf("[acquire] immediate grant (combined) appliance=%s token=%s in_flight=%d remaining=%d",
			r.ApplianceID, tok, e.State.InFlight, e.State.LocalRemaining)
		e.State.Unlock()
		go e.DB.WriteLog(res.logEntry)
		e.notify()
		e.Notifier.Notify()
		return tok, nil, 0
	}

	if e.Queue.Len() >= e.MaxQueue {
		e.State.Unlock()
		go e.DB.WriteLog(res.logEntry)
		e.notify()
		e.Notifier.Notify()
		return "", nil, 503
	}

	bp, ok := state.BasePriorities[r.NextPriorityClass]
	if !ok {
		bp = state.BasePriorities[state.PriorityMedium]
	}
	ch = make(chan bool, 1)
	e.Queue.Enqueue(&queue.Item{
		ApplianceID:   r.ApplianceID,
		PriorityClass: r.NextPriorityClass,
		BasePriority:  bp,
		QueuedAt:      time.Now(),
		Backlog:       r.Backlog,
		ResponseChan:  ch,
	})
	log.Printf("[acquire] queued (combined) appliance=%s priority=%s queue_depth=%d",
		r.ApplianceID, r.NextPriorityClass, e.Queue.Len())
	e.State.Unlock()
	go e.DB.WriteLog(res.logEntry)
	e.notify()
	e.Notifier.Notify()
	return "", ch, 0
}

func buildEntry(r Report, ticket state.ActiveTicket, impliedLegacy int) db.LogEntry {
	var latMs float64
	if !r.APISentAt.IsZero() && !r.APIRecvAt.IsZero() {
		latMs = float64(r.APIRecvAt.Sub(r.APISentAt).Milliseconds())
	}
	return db.LogEntry{
		Token:              r.Token,
		ApplianceID:        r.ApplianceID,
		PriorityClass:      r.PriorityClass,
		QueuedAt:           r.QueuedAt,
		AcquiredAt:         r.AcquiredAt,
		APISentAt:          r.APISentAt,
		APIRecvAt:          r.APIRecvAt,
		NSRemaining:        r.NSRemaining,
		NSReset:            r.NSReset,
		StatusCode:         r.StatusCode,
		EstimatedLatencyMs: latMs,
		ExpectedRemaining:  ticket.ExpectedRemaining,
		ImpliedLegacyUsage: impliedLegacy,
	}
}

// ---- Window detection (called with state.Lock held) ----

func (e *Engine) tryPrimaryWindowDetection(r Report) bool {
	if r.NSRemaining != 49 || e.State.WindowActive {
		return false
	}
	oneWay := r.APIRecvAt.Sub(r.APISentAt) / 2
	e.State.WindowStartTime = r.APIRecvAt.Add(-oneWay)
	e.State.WindowEndTime = e.State.WindowStartTime.Add(30 * time.Second)
	e.State.WindowActive = true
	e.State.WindowDetectedVia = "49_trigger"
	log.Printf("[window] primary (49-trigger) start=%s end=%s",
		e.State.WindowStartTime.Format(time.RFC3339Nano),
		e.State.WindowEndTime.Format(time.RFC3339Nano))
	go e.DB.WriteWindow(db.WindowEntry{
		WindowStart:  e.State.WindowStartTime,
		WindowEnd:    e.State.WindowEndTime,
		DetectedVia:  "49_trigger",
		TriggerToken: r.Token,
	})
	return true
}

func (e *Engine) tryFallbackWindowDetection(r Report) bool {
	if e.State.WindowActive {
		return false
	}
	e.State.WindowEndTime = r.APIRecvAt.Add(time.Duration(r.NSReset) * time.Second)
	e.State.WindowStartTime = e.State.WindowEndTime.Add(-30 * time.Second)
	e.State.WindowActive = true
	e.State.WindowDetectedVia = "reset_fallback"
	log.Printf("[window] fallback (reset_fallback) start=%s end=%s",
		e.State.WindowStartTime.Format(time.RFC3339Nano),
		e.State.WindowEndTime.Format(time.RFC3339Nano))
	go e.DB.WriteWindow(db.WindowEntry{
		WindowStart:  e.State.WindowStartTime,
		WindowEnd:    e.State.WindowEndTime,
		DetectedVia:  "reset_fallback",
		TriggerToken: r.Token,
	})
	return true
}

// ---- Stale ticket reaper ----

func (e *Engine) ticketReaper() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		e.State.Lock()
		now := time.Now()
		for applianceID, ticket := range e.State.ActiveTickets {
			if now.Sub(ticket.IssuedAt) > state.TicketExpiryThreshold {
				log.Printf("[reaper] expired ticket appliance=%s token=%s age=%s",
					applianceID, ticket.Token, now.Sub(ticket.IssuedAt))
				snapTicket := *ticket
				go e.DB.WriteAnomaly("ticket_expired", snapTicket.ApplianceID, snapTicket.Token,
					fmt.Sprintf("age_ms=%d", now.Sub(snapTicket.IssuedAt).Milliseconds()))
				delete(e.State.ActiveTickets, applianceID)
				e.State.InFlight--
			}
		}
		e.State.Unlock()
		e.notify()
		e.Notifier.Notify()
	}
}
