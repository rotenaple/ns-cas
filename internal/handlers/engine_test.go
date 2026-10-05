package handlers

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/rotenaple/ns-cas/internal/db"
	"github.com/rotenaple/ns-cas/internal/queue"
	"github.com/rotenaple/ns-cas/internal/state"
)

func testEngine(t *testing.T) (*Engine, *state.State) {
	t.Helper()

	// The database is optional. go-sqlite3 requires cgo, so on a machine without
	// a C toolchain these tests still exercise the quota logic and only the
	// persistence assertions skip.
	database, err := db.Open(filepath.Join(t.TempDir(), "cas.db"))
	if err != nil {
		t.Logf("running without analytics db (%v); persistence assertions will skip", err)
		database = nil
	} else {
		t.Cleanup(database.Close)
	}

	cfg := state.Config{
		BucketLimit:        50,
		PolicyWindowSec:    30,
		SustainedLimit:     10,
		SustainedWindowSec: 900,
		SustainedMinLimit:  2,
		SustainedMaxLimit:  20,
	}
	st := state.New(cfg)
	// Build the Engine directly rather than via NewEngine so the background
	// reaper and dispatch loop do not run: these tests drive the grant paths
	// themselves and a concurrent dispatcher would make them racy.
	eng := &Engine{
		State:      st,
		Queue:      queue.New(1.0),
		DB:         database,
		MaxQueue:   100,
		Notifier:   NewNotifier(),
		notifyChan: make(chan struct{}, 64),
	}

	// Calibrate from one report so the burst path is header-driven, then let one
	// CanDispatch establish the sustained window as it would in production. No
	// report is handled for this seed, so no ticket is left outstanding.
	now := time.Now()
	st.NoteHeaders(state.Headers{Limit: 50, Policy: "50;w=30", Remaining: 50, ResetSec: 30}, now)
	st.ConsumeWindowOpened()
	st.CanDispatch()
	st.ConsumeWindowOpened()
	return eng, st
}

// requireDB skips a persistence assertion when go-sqlite3 could not be loaded.
func requireDB(t *testing.T, eng *Engine) {
	t.Helper()
	if eng.DB == nil {
		t.Skip("no analytics db (go-sqlite3 needs cgo)")
	}
}

// reportFor puts a matching active ticket on the state so a report resolves.
func reportFor(t *testing.T, eng *Engine, applianceID string, status int) {
	t.Helper()
	eng.State.Lock()
	defer eng.State.Unlock()
	eng.State.ActiveTickets[applianceID] = &state.ActiveTicket{
		Token:       "tok-" + applianceID,
		ApplianceID: applianceID,
		IssuedAt:    time.Now(),
	}
	eng.State.InFlight++
	_ = status
}

// report builds a Report that matches the ticket reportFor installed.
func report(applianceID string, status, remaining, reset, limit int, policy string, retryAfter int) Report {
	now := time.Now()
	return Report{
		Token:         "tok-" + applianceID,
		ApplianceID:   applianceID,
		PriorityClass: state.PriorityMedium,
		APISentAt:     now,
		APIRecvAt:     now.Add(300 * time.Millisecond),
		NSRemaining:   remaining,
		NSReset:       reset,
		NSRateLimit:   limit,
		NSPolicy:      policy,
		StatusCode:    status,
		RetryAfter:    retryAfter,
	}
}

func TestTryAcquireDeductsSustained(t *testing.T) {
	eng, st := testEngine(t)

	st.Lock()
	before := st.SustainedUsed
	st.Unlock()

	token, _, code := eng.TryAcquire(AcquireRequest{ApplianceID: "a1", PriorityClass: state.PriorityHigh})
	if code != 0 || token == "" {
		t.Fatalf("expected an immediate grant, got token=%q code=%d", token, code)
	}

	st.Lock()
	defer st.Unlock()
	if st.SustainedUsed != before+1 {
		t.Errorf("SustainedUsed = %d, want %d — the immediate grant path must deduct",
			st.SustainedUsed, before+1)
	}
}

func TestGrantFromQueueDeductsSustained(t *testing.T) {
	eng, st := testEngine(t)

	item := &queue.Item{
		ApplianceID:   "a2",
		PriorityClass: state.PriorityMedium,
		BasePriority:  50,
		QueuedAt:      time.Now(),
		ResponseChan:  make(chan bool, 1),
	}

	st.Lock()
	before := st.SustainedUsed
	st.Unlock()

	if tok := eng.GrantFromQueue(item); tok == "" {
		t.Fatal("expected a token from GrantFromQueue")
	}

	st.Lock()
	defer st.Unlock()
	if st.SustainedUsed != before+1 {
		t.Errorf("SustainedUsed = %d, want %d — GrantFromQueue must deduct",
			st.SustainedUsed, before+1)
	}
}

func TestReportAndAcquireDeductsSustained(t *testing.T) {
	eng, st := testEngine(t)
	reportFor(t, eng, "a3", 200)

	st.Lock()
	before := st.SustainedUsed
	st.Unlock()

	r := report("a3", 200, 50, 30, 50, "50;w=30", 0)
	r.NextPriorityClass = state.PriorityHigh
	token, _, code := eng.TryReportAndAcquire(r)
	if code != 0 || token == "" {
		t.Fatalf("expected an immediate grant, got token=%q code=%d", token, code)
	}

	st.Lock()
	defer st.Unlock()
	if st.SustainedUsed != before+1 {
		t.Errorf("SustainedUsed = %d, want %d — the combined path must deduct",
			st.SustainedUsed, before+1)
	}
}

func TestEveryGrantPathDeductsExactlyOnce(t *testing.T) {
	// The sustained limit is only a bound if no path can issue a ticket for free.
	eng, st := testEngine(t)

	before := st.SustainedUsed
	eng.TryAcquire(AcquireRequest{ApplianceID: "b1", PriorityClass: state.PriorityHigh})
	eng.GrantFromQueue(&queue.Item{
		ApplianceID:   "b2",
		PriorityClass: state.PriorityMedium,
		QueuedAt:      time.Now(),
		ResponseChan:  make(chan bool, 1),
	})
	reportFor(t, eng, "b3", 200)
	r := report("b3", 200, 50, 30, 50, "50;w=30", 0)
	r.NextPriorityClass = state.PriorityHigh
	eng.TryReportAndAcquire(r)

	st.Lock()
	defer st.Unlock()
	if got := st.SustainedUsed - before; got != 3 {
		t.Errorf("three grants moved the sustained budget by %d, want 3", got)
	}
}

func TestSustainedLimitStopsDispatchAcrossAppliances(t *testing.T) {
	eng, st := testEngine(t)

	st.Lock()
	st.SustainedLimit = 2
	st.SustainedUsed = 2
	st.Unlock()

	token, _, code := eng.TryAcquire(AcquireRequest{ApplianceID: "c1", PriorityClass: state.PriorityHigh})
	if code != 0 || token != "" {
		t.Fatalf("expected the request to be queued rather than granted, got token=%q code=%d", token, code)
	}
	if eng.Queue.Len() != 1 {
		t.Errorf("queue depth = %d, want the request parked at 1", eng.Queue.Len())
	}
}

func TestPenaltyAdoptsReportedWindowAndCutsLimit(t *testing.T) {
	eng, st := testEngine(t)
	reportFor(t, eng, "d1", 429)

	r := report("d1", 429, 0, 30, 50, "50;w=30", 894)
	eng.HandleReport(r)

	st.Lock()
	defer st.Unlock()
	if st.SustainedWindowSec != 894 {
		t.Errorf("SustainedWindowSec = %d, want the 894s Retry-After wired back from NS",
			st.SustainedWindowSec)
	}
	if st.SustainedLimit != 5 {
		t.Errorf("SustainedLimit = %d, want 5 — halved from 10 by the penalty", st.SustainedLimit)
	}
	if !st.LockdownActive {
		t.Error("expected the penalty to hold a lockdown")
	}
	if st.Penalties != 1 {
		t.Errorf("Penalties = %d, want 1", st.Penalties)
	}
}

func TestPenaltyStillReadsHeaders(t *testing.T) {
	// A 429 carries the rate-limit headers too, and CAS should keep using them
	// rather than freezing on stale numbers.
	eng, st := testEngine(t)
	reportFor(t, eng, "d2", 429)

	r := report("d2", 429, 0, 30, 40, "40;w=45", 894)
	eng.HandleReport(r)

	st.Lock()
	defer st.Unlock()
	if st.BucketLimit != 40 {
		t.Errorf("BucketLimit = %d, want 40 taken from the 429 response", st.BucketLimit)
	}
	if st.PolicyWindow() != 45 {
		t.Errorf("PolicyWindow() = %d, want 45 from the 429's policy header", st.PolicyWindow())
	}
}

func TestPenaltyWithoutRetryAfterKeepsLearnedWindow(t *testing.T) {
	eng, st := testEngine(t)

	reportFor(t, eng, "d3", 429)
	eng.HandleReport(report("d3", 429, 0, 30, 50, "50;w=30", 894))

	reportFor(t, eng, "d4", 429)
	eng.HandleReport(report("d4", 429, 0, 30, 50, "50;w=30", 0))

	st.Lock()
	defer st.Unlock()
	if st.SustainedWindowSec != 894 {
		t.Errorf("SustainedWindowSec = %d, want the learned 894 retained", st.SustainedWindowSec)
	}
	if !st.LockdownActive {
		t.Error("expected the penalty to still be held")
	}
}

func TestReportAndAcquireReturns429DuringPenalty(t *testing.T) {
	eng, _ := testEngine(t)
	reportFor(t, eng, "d5", 429)

	r := report("d5", 429, 0, 30, 50, "50;w=30", 600)
	_, _, code := eng.TryReportAndAcquire(r)
	if code != 429 {
		t.Errorf("code = %d, want 429 while a penalty is being served", code)
	}
}

func TestUnmatchedTokenDoesNotMoveQuota(t *testing.T) {
	eng, st := testEngine(t)

	st.Lock()
	before := st.SustainedUsed
	remaining := st.LocalRemaining
	st.Unlock()

	eng.HandleReport(report("ghost", 200, 3, 30, 50, "50;w=30", 0))

	st.Lock()
	defer st.Unlock()
	if st.SustainedUsed != before {
		t.Errorf("SustainedUsed moved to %d on an unmatched token", st.SustainedUsed)
	}
	if st.LocalRemaining != remaining {
		t.Errorf("LocalRemaining moved to %d on an unmatched token", st.LocalRemaining)
	}
}

func TestNSExhaustedIsRecordedAsAnomaly(t *testing.T) {
	eng, st := testEngine(t)
	requireDB(t, eng)
	reportFor(t, eng, "e1", 200)

	// CAS believes it has budget; NS says the bucket is empty.
	eng.HandleReport(report("e1", 200, 0, 7, 50, "50;w=30", 0))

	st.Lock()
	defer st.Unlock()
	if st.LocalRemaining != 0 {
		t.Errorf("LocalRemaining = %d, want NS's 0 adopted", st.LocalRemaining)
	}

	// The anomaly is written asynchronously; give it a moment to land.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := eng.DB.QueryRecentAnomalies(10)
		if err == nil {
			for _, r := range rows {
				if r.EventType == "ns_exhausted" && r.ApplianceID == "e1" {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("expected an ns_exhausted anomaly to be recorded")
}

func TestReportWindowIsTaggedAsHeaderSourced(t *testing.T) {
	eng, _ := testEngine(t)
	requireDB(t, eng)

	// Age the view out so the next report opens a genuinely new window.
	eng.State.Lock()
	eng.State.WindowEndTime = time.Now().Add(-time.Second)
	eng.State.Unlock()

	reportFor(t, eng, "f1", 200)
	eng.HandleReport(report("f1", 200, 30, 7, 50, "50;w=30", 0))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := eng.DB.QueryRecentWindows(10)
		if err == nil {
			for _, r := range rows {
				if r.DetectedVia == "ns_headers" {
					return
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("expected a window row tagged ns_headers")
}
