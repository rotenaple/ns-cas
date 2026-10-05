package state

import (
	"testing"
	"time"
)

func testConfig() Config {
	return Config{
		BucketLimit:        50,
		PolicyWindowSec:    30,
		SustainedLimit:     10,
		SustainedWindowSec: 900,
		SustainedMinLimit:  2,
		SustainedMaxLimit:  20,
	}
}

// calibrated returns a State that has already seen one report, so tests exercise
// the header-driven path rather than the single bootstrap request.
func calibrated(t *testing.T) *State {
	t.Helper()
	s := New(testConfig())
	now := time.Now()
	s.NoteHeaders(Headers{Limit: 50, Policy: "50;w=30", Remaining: 50, ResetSec: 30}, now)
	// The first report opens a window and is expected to be recorded as such;
	// consume it so tests only observe windows they cause themselves. Rolling the
	// sustained window here establishes it, since it has no header to create it.
	s.ConsumeWindowOpened()
	s.rollSustainedWindow(now)
	return s
}

// headers mirrors what NationStates returns for a normal call.
func headers(remaining int) Headers {
	return Headers{Limit: 50, Policy: "50;w=30", Remaining: remaining, ResetSec: 7}
}

func TestParsePolicyWindow(t *testing.T) {
	cases := []struct {
		policy string
		want   int
	}{
		{"50;w=30", 30},
		{"50; w=30", 30},
		{"100;w=60", 60},
		{"50;w=1", 1},
		{"", 0},
		{"50", 0},
		{"50;w=abc", 0},
		{"50;w=0", 0},
		{"w=45;limit=50", 45},
	}
	for _, c := range cases {
		if got := ParsePolicyWindow(c.policy); got != c.want {
			t.Errorf("ParsePolicyWindow(%q) = %d, want %d", c.policy, got, c.want)
		}
	}
}

func TestBootstrapAllowsSingleRequestUntilCalibrated(t *testing.T) {
	s := New(testConfig())
	if !s.CanDispatch() {
		t.Fatal("expected the bootstrap request to be dispatchable")
	}
	if s.BucketLimit != 50 {
		t.Errorf("BucketLimit = %d, want the cold-start default 50", s.BucketLimit)
	}
	if s.LocalRemaining != 1 {
		t.Errorf("LocalRemaining = %d, want 1 before calibration", s.LocalRemaining)
	}
}

func TestNoteHeadersAdoptsLimitAndPolicy(t *testing.T) {
	s := New(testConfig())
	now := time.Now()
	s.NoteHeaders(Headers{Limit: 40, Policy: "40;w=45", Remaining: 40, ResetSec: 45}, now)

	if s.BucketLimit != 40 {
		t.Errorf("BucketLimit = %d, want 40 from ratelimit-limit", s.BucketLimit)
	}
	if s.PolicyWindow() != 45 {
		t.Errorf("PolicyWindow() = %d, want 45 from ratelimit-policy", s.PolicyWindow())
	}
	if !s.Calibrated {
		t.Error("expected Calibrated after the first report")
	}
}

func TestNoteHeadersKeepsConfiguredWindowWhenPolicyAbsent(t *testing.T) {
	s := New(testConfig())
	s.NoteHeaders(Headers{Limit: 50, Remaining: 50}, time.Now())
	if s.PolicyWindow() != 30 {
		t.Errorf("PolicyWindow() = %d, want the configured fallback 30", s.PolicyWindow())
	}
}

func TestRemainingIsTakenFromHeadersNotAModel(t *testing.T) {
	s := calibrated(t)
	s.NoteHeaders(headers(7), time.Now())
	if s.LocalRemaining != 7 {
		t.Errorf("LocalRemaining = %d, want 7 from ratelimit-remaining", s.LocalRemaining)
	}
}

func TestRemainingClampedToLimit(t *testing.T) {
	s := calibrated(t)
	// A remaining above the limit would let CAS dispatch more than NS allows.
	s.NoteHeaders(Headers{Limit: 50, Policy: "50;w=30", Remaining: 90}, time.Now())
	if s.LocalRemaining != 50 {
		t.Errorf("LocalRemaining = %d, want it clamped to BucketLimit 50", s.LocalRemaining)
	}
}

func TestStaleRemainingIsDiscardedRatherThanWedging(t *testing.T) {
	s := calibrated(t)
	s.NoteHeaders(headers(0), time.Now())
	if s.CanDispatch() {
		t.Fatal("a reported 0 should stop dispatch immediately")
	}

	// No further reports arrive, because a 0 remaining is what stopped dispatch.
	// Age the view past the policy window and CAS must recover on its own.
	s.WindowEndTime = time.Now().Add(-time.Second)
	if !s.CanDispatch() {
		t.Fatal("expected recovery once the reported remaining aged out")
	}
	if s.LocalRemaining != s.BucketLimit {
		t.Errorf("LocalRemaining = %d, want a full BucketLimit %d after refresh", s.LocalRemaining, s.BucketLimit)
	}
}

func TestFreshRemainingIsNotRefreshedEarly(t *testing.T) {
	s := calibrated(t)
	s.NoteHeaders(headers(1), time.Now())
	// One unit left, still fresh: dispatching it must exhaust the budget.
	if !s.CanDispatch() {
		t.Fatal("expected one dispatch with remaining=1")
	}
	s.InFlight++
	if s.CanDispatch() {
		t.Error("dispatched beyond the remaining NS reported")
	}
}

func TestSustainedBudgetBlocksDispatchWhenExhausted(t *testing.T) {
	s := calibrated(t)
	s.SustainedLimit = 3
	s.SustainedUsed = 3
	if s.CanDispatch() {
		t.Error("expected dispatch to stop once the sustained allowance is spent")
	}
}

func TestSustainedWindowRollsAndResets(t *testing.T) {
	s := calibrated(t)
	s.SustainedLimit = 10
	s.SustainedUsed = 10
	s.SustainedWindowEnd = time.Now().Add(-time.Second)

	if !s.CanDispatch() {
		t.Fatal("expected a rolled sustained window to release dispatch")
	}
	if s.SustainedUsed != 0 {
		t.Errorf("SustainedUsed = %d, want 0 after the sustained window rolled", s.SustainedUsed)
	}
}

func TestSustainedDeductionIsCounted(t *testing.T) {
	s := calibrated(t)
	s.SustainedLimit = 2
	s.DeductSustained()
	s.DeductSustained()
	if s.SustainedUsed != 2 {
		t.Errorf("SustainedUsed = %d, want 2", s.SustainedUsed)
	}
	if s.CanDispatch() {
		t.Error("expected dispatch to stop at the sustained limit")
	}
}

func TestNotePenaltyAdoptsReportedWindow(t *testing.T) {
	s := calibrated(t)
	s.SustainedWindowSec = 900

	p := s.NotePenalty(894)

	if p.NewWindowSec != 894 {
		t.Errorf("sustained window = %d, want the 894s NS actually returned", p.NewWindowSec)
	}
	if s.SustainedWindowSec != 894 {
		t.Errorf("SustainedWindowSec = %d, want 894 wired back from the response", s.SustainedWindowSec)
	}
	if !s.LockdownActive {
		t.Error("expected the penalty to hold a lockdown")
	}
	if s.SustainedUsed != s.SustainedLimit {
		t.Errorf("SustainedUsed = %d, want the sustained budget exhausted for the penalty", s.SustainedUsed)
	}
	if s.CanDispatch() {
		t.Error("expected no dispatch while a penalty is being served")
	}
}

func TestNotePenaltyWithNoRetryAfterKeepsLearnedWindow(t *testing.T) {
	s := calibrated(t)
	// Learn the real penalty duration from a prior 429...
	s.NotePenalty(894)

	// ...then get a 429 that reports nothing. Collapsing the window to zero would
	// release dispatch immediately and walk straight back into another 429.
	p := s.NotePenalty(0)

	if p.NewWindowSec != 894 {
		t.Errorf("sustained window = %d, want the previously learned 894 to be kept", p.NewWindowSec)
	}
	if !s.LockdownActive {
		t.Error("expected the penalty to still be held")
	}
}

func TestNotePenaltyHalvesLimitAndClampsAtMin(t *testing.T) {
	s := calibrated(t)
	s.SustainedLimit = 20
	s.cfg.SustainedMinLimit = 2

	if p := s.NotePenalty(894); p.NewLimit != 10 {
		t.Errorf("limit after first penalty = %d, want 10 (halved)", p.NewLimit)
	}
	if p := s.NotePenalty(894); p.NewLimit != 5 {
		t.Errorf("limit after second penalty = %d, want 5 (halved again)", p.NewLimit)
	}
	for i := 0; i < 10; i++ {
		s.NotePenalty(894)
	}
	if s.SustainedLimit != 2 {
		t.Errorf("limit = %d, want it clamped to the configured minimum 2", s.SustainedLimit)
	}
}

func TestCleanSustainedWindowCreditsOneAndStopsAtMax(t *testing.T) {
	s := calibrated(t)
	s.SustainedLimit = 10
	s.cfg.SustainedMaxLimit = 12

	s.SustainedWindowEnd = time.Now().Add(-time.Second)
	s.CanDispatch()
	if s.SustainedLimit != 11 {
		t.Errorf("limit after one clean window = %d, want 11", s.SustainedLimit)
	}

	s.SustainedWindowEnd = time.Now().Add(-time.Second)
	s.CanDispatch()
	if s.SustainedLimit != 12 {
		t.Errorf("limit = %d, want it clamped to the configured maximum 12", s.SustainedLimit)
	}

	s.SustainedWindowEnd = time.Now().Add(-time.Second)
	s.CanDispatch()
	if s.SustainedLimit != 12 {
		t.Errorf("limit = %d, want it to stay at the maximum", s.SustainedLimit)
	}
}

func TestPenalisedWindowEarnsNothingBack(t *testing.T) {
	s := calibrated(t)
	s.SustainedLimit = 10
	s.cfg.SustainedMaxLimit = 20
	s.NotePenalty(894)

	// Roll past the penalty window. The sustained limit must not creep back up,
	// or a single unlucky report would restore the rate that caused it.
	s.SustainedWindowEnd = time.Now().Add(-time.Second)
	s.CanDispatch()

	if s.SustainedLimit != 5 {
		t.Errorf("limit = %d, want 5 — a penalised window credits nothing", s.SustainedLimit)
	}
}

func TestLockdownExpiryReleasesDispatch(t *testing.T) {
	s := calibrated(t)
	s.NoteHeaders(headers(0), time.Now())
	s.NotePenalty(60)

	if s.CanDispatch() {
		t.Fatal("expected the penalty to block dispatch")
	}

	// Serve out the penalty.
	s.LockdownUntil = time.Now().Add(-time.Second)
	s.SustainedWindowEnd = time.Now().Add(-time.Second)

	if !s.CanDispatch() {
		t.Fatal("expected dispatch to resume once the penalty was served")
	}
	if s.LocalRemaining <= 0 {
		t.Errorf("LocalRemaining = %d, want a positive allowance after the penalty", s.LocalRemaining)
	}
}

func TestNoDeadlockWithNothingInFlight(t *testing.T) {
	// The state that used to wedge permanently: remaining exhausted, nothing in
	// flight, no penalty running, and no report coming to clear it.
	s := calibrated(t)
	s.LocalRemaining = 0
	s.WindowActive = false
	s.InFlight = 0
	s.LockdownActive = false

	if !s.CanDispatch() {
		t.Fatal("expected recovery with no in-flight tickets and no penalty")
	}
}

func TestInFlightConsumesReportedRemaining(t *testing.T) {
	s := calibrated(t)
	s.NoteHeaders(headers(2), time.Now())
	s.InFlight = 2
	if s.CanDispatch() {
		t.Error("dispatched while every reported unit was already in flight")
	}
	s.InFlight = 1
	if !s.CanDispatch() {
		t.Error("expected one unit of headroom with a single ticket in flight")
	}
}

func TestConsumeWindowOpenedReportsOnce(t *testing.T) {
	s := New(testConfig())
	if s.ConsumeWindowOpened() {
		t.Error("no window should be reported before any headers arrive")
	}
	s.NoteHeaders(headers(50), time.Now())
	if !s.ConsumeWindowOpened() {
		t.Error("expected the first report to open a window")
	}
	if s.ConsumeWindowOpened() {
		t.Error("expected the window flag to be consumed exactly once")
	}
}

func TestReportWithinSameWindowDoesNotReopenIt(t *testing.T) {
	s := calibrated(t)
	first := s.WindowEndTime

	// Several appliances reporting in quick succession must not each mint a new
	// window: that is what refilled the allowance several times faster than NS
	// resets its own.
	s.NoteHeaders(headers(44), time.Now().Add(time.Second))
	s.NoteHeaders(headers(40), time.Now().Add(2*time.Second))

	if s.ConsumeWindowOpened() {
		t.Error("a mid-window report must not reopen the window")
	}
	if !s.WindowEndTime.Equal(first) {
		t.Errorf("WindowEndTime moved to %v, want it anchored at %v", s.WindowEndTime, first)
	}
	if s.LocalRemaining != 40 {
		t.Errorf("LocalRemaining = %d, want the newest reported 40", s.LocalRemaining)
	}
}

func TestReportedRefillOpensAFreshWindow(t *testing.T) {
	s := calibrated(t)
	s.NoteHeaders(headers(0), time.Now())
	s.WindowEndTime = time.Now().Add(-time.Second)

	// NS reporting a higher remaining means its bucket refilled, which is a new
	// allocation period and needs the class split recomputed.
	s.NoteHeaders(headers(30), time.Now())

	if !s.ConsumeWindowOpened() {
		t.Error("expected a refilled bucket to open a fresh window")
	}
	if s.LocalRemaining != 30 {
		t.Errorf("LocalRemaining = %d, want 30", s.LocalRemaining)
	}
}

func TestNewClampsInconsistentConfig(t *testing.T) {
	s := New(Config{
		BucketLimit:        0,
		PolicyWindowSec:    0,
		SustainedLimit:     1,
		SustainedWindowSec: 0,
		SustainedMinLimit:  10,
		SustainedMaxLimit:  5,
	})
	if s.BucketLimit != 50 {
		t.Errorf("BucketLimit = %d, want the 50 default for a zero value", s.BucketLimit)
	}
	if s.PolicyWindow() != 30 {
		t.Errorf("PolicyWindow() = %d, want the 30 default for a zero value", s.PolicyWindow())
	}
	if s.SustainedLimit < 10 {
		t.Errorf("SustainedLimit = %d, want it raised to the minimum 10", s.SustainedLimit)
	}
	if s.Config().SustainedMaxLimit < 10 {
		t.Errorf("SustainedMaxLimit = %d, want it raised to at least the minimum", s.Config().SustainedMaxLimit)
	}
}
