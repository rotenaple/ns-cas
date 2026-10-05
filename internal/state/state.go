// Package state defines the core in-memory data structures for the CAS.
// All fields are protected by the embedded sync.Mutex. Callers must hold the
// lock for any read or write of mutable fields.
package state

import (
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TicketExpiryThreshold is how long a ticket lives before the reaper reclaims it.
const TicketExpiryThreshold = 10 * time.Second

// PriorityClass labels used by clients.
const (
	PriorityHigh   = "P1_HIGH"
	PriorityMedium = "P2_MEDIUM"
	PriorityLow    = "P3_LOW"
)

// BasePriorities maps each class to a numeric base used by the queue scorer.
var BasePriorities = map[string]float64{
	PriorityHigh:   100,
	PriorityMedium: 50,
	PriorityLow:    1,
}

// ClassConfig defines the proportional weight and hard max-fraction ceiling for a
// priority class when allocating dispatch quota at each window boundary.
type ClassConfig struct {
	Weight      float64 // relative share of BucketLimit when this class has demand
	MaxFraction float64 // hard ceiling: class may never receive more than this × BucketLimit
}

// ClassConfigs is the compile-time class configuration.
// Weights are applied proportionally across active (demand > 0) classes.
var ClassConfigs = map[string]ClassConfig{
	PriorityHigh:   {Weight: 4, MaxFraction: 0.5},
	PriorityMedium: {Weight: 2, MaxFraction: 0.7},
	PriorityLow:    {Weight: 1, MaxFraction: 1.0},
}

// Config holds the operator-tunable limits.
//
// PolicyWindowSec is only a fallback for when NationStates does not send a
// ratelimit-policy header. Everything else about the burst budget is taken from
// the headers themselves.
//
// SustainedWindowSec is likewise only the starting value: NationStates reports
// how long a penalty actually lasts, and NotePenalty adopts the reported
// duration, so the default applies until the first 429 teaches CAS the real one.
type Config struct {
	BucketLimit        int // fallback burst allowance when ratelimit-limit is absent
	PolicyWindowSec    int // fallback for the w= component of ratelimit-policy
	SustainedLimit     int // initial dispatches allowed per sustained window
	SustainedWindowSec int // initial sustained window length, in seconds
	SustainedMinLimit  int // floor the sustained allowance is cut down to by penalties
	SustainedMaxLimit  int // ceiling the sustained allowance climbs back to
}

// DefaultConfig returns the built-in limits, used when the environment does not
// override them.
func DefaultConfig() Config {
	return Config{
		BucketLimit:        50,
		PolicyWindowSec:    30,
		SustainedLimit:     300,
		SustainedWindowSec: 900,
		SustainedMinLimit:  25,
		SustainedMaxLimit:  1200,
	}
}

// Headers is the rate-limit state NationStates returned for one API call.
//
// CAS runs on these numbers rather than on a model of its own. An earlier
// version kept a local window and used ratelimit-reset as its length, but that
// header is the countdown to the next refill, not the length of the allocation -
// so CAS granted a fresh allowance several times faster than NationStates resets
// its own, and the 429s that followed looked like NS misbehaving.
type Headers struct {
	Limit     int    // ratelimit-limit
	Policy    string // ratelimit-policy, e.g. "50;w=30"
	Remaining int    // ratelimit-remaining
	ResetSec  int    // ratelimit-reset: seconds until the bucket next refills
}

// ParsePolicyWindow extracts the w=<seconds> component of a ratelimit-policy
// header, returning 0 when the header is absent or does not carry one.
func ParsePolicyWindow(policy string) int {
	for _, part := range strings.Split(policy, ";") {
		part = strings.TrimSpace(part)
		if !strings.HasPrefix(part, "w=") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimSpace(part[2:]))
		if err == nil && n > 0 {
			return n
		}
	}
	return 0
}

// ActiveTicket represents a dispatched-but-unreported request slot.
type ActiveTicket struct {
	Token             string
	ApplianceID       string
	IssuedAt          time.Time
	ExpectedRemaining int // LocalRemaining - InFlight at moment of dispatch (post-increment)
}

// State is the single shared mutable object for the CAS.
// It is embedded with sync.Mutex so callers do state.Lock() / state.Unlock().
type State struct {
	sync.Mutex

	cfg Config

	// Quota tracking, reconciled from the headers NationStates returned.
	BucketLimit    int // ratelimit-limit
	LocalRemaining int // ratelimit-remaining
	InFlight       int // Tickets granted but not yet reported

	// PolicyWindowSec is the w= component of ratelimit-policy: how long a
	// reported ratelimit-remaining stays trustworthy. It is not a budget period.
	// Once it ages out the bucket is assumed refilled to BucketLimit, because a
	// stale "0" would otherwise wedge CAS with no reports coming in to clear it.
	PolicyWindowSec int

	// Per-appliance ticket map; enforces one-in-flight per appliance
	ActiveTickets map[string]*ActiveTicket

	// Calibration
	Calibrated bool // False until first /report is processed

	// Provenance of the current view of LocalRemaining. WindowEndTime is when
	// that view goes stale, not when an allocation period ends.
	WindowActive    bool
	WindowStartTime time.Time
	WindowEndTime   time.Time

	// Sustained budget. NationStates also enforces a longer-run limit whose
	// penalty duration it reports per response, and CAS had no model of it at
	// all - so it could only react to a 429 by freezing every appliance instead
	// of avoiding one. The window length is learned from the reported penalty.
	SustainedWindowSec int
	SustainedLimit     int
	SustainedWindowEnd time.Time
	SustainedUsed      int

	// penaltyInWindow records whether the current sustained window saw a 429,
	// so only genuinely clean windows earn the allowance back.
	penaltyInWindow bool
	Penalties       int // lifetime count of upstream 429s, for telemetry

	// Hard lockdown while a reported penalty is still being served. Kept
	// alongside the sustained window because it is what /report-and-acquire
	// reports back and what the dashboard renders.
	LockdownActive bool
	LockdownUntil  time.Time

	// windowOpened is set when NoteHeaders or refreshRemaining establishes a
	// fresh view of the remaining budget, and consumed by the engine so it can
	// record the window.
	windowOpened bool

	// Class-based quota allocation (reset at each window boundary via ResetClassBudgets).
	// DemandSum accumulates 1+backlog per acquire call and is zeroed on reset.
	// ClassRemaining holds each class's dispatch quota for the current window.
	// SharedRemaining holds unallocated quota that any class may use.
	DemandSum       map[string]int
	ClassRemaining  map[string]int
	SharedRemaining int
}

// New returns a freshly bootstrapped State. Only one bootstrap request is
// permitted until the first telemetry calibrates real quota state.
func New(cfg Config) *State {
	if cfg.BucketLimit <= 0 {
		cfg.BucketLimit = 50
	}
	if cfg.PolicyWindowSec <= 0 {
		cfg.PolicyWindowSec = 30
	}
	if cfg.SustainedWindowSec <= 0 {
		cfg.SustainedWindowSec = 900
	}
	if cfg.SustainedLimit <= 0 {
		cfg.SustainedLimit = 1
	}
	if cfg.SustainedMinLimit <= 0 {
		cfg.SustainedMinLimit = 1
	}
	if cfg.SustainedMaxLimit < cfg.SustainedMinLimit {
		cfg.SustainedMaxLimit = cfg.SustainedMinLimit
	}
	if cfg.SustainedLimit < cfg.SustainedMinLimit {
		cfg.SustainedLimit = cfg.SustainedMinLimit
	}
	if cfg.SustainedLimit > cfg.SustainedMaxLimit {
		cfg.SustainedLimit = cfg.SustainedMaxLimit
	}

	return &State{
		cfg: cfg,

		BucketLimit:    cfg.BucketLimit,
		LocalRemaining: 1, // Conservative: allow exactly one cold-start request
		Calibrated:     false,
		ActiveTickets:  make(map[string]*ActiveTicket),
		DemandSum:      make(map[string]int),
		ClassRemaining: make(map[string]int),
		SharedRemaining: cfg.BucketLimit,

		PolicyWindowSec:   cfg.PolicyWindowSec,
		SustainedWindowSec: cfg.SustainedWindowSec,
		SustainedLimit:     cfg.SustainedLimit,
	}
}

// Config returns the operator-tunable limits this State was built with. The
// sustained limit is mutated at runtime by penalties, so this is the baseline it
// recovers towards rather than the current value.
func (s *State) Config() Config { return s.cfg }

// PolicyWindow returns the window length to trust from the policy header,
// falling back to the configured value when NS has not sent one.
func (s *State) PolicyWindow() int {
	if s.PolicyWindowSec > 0 {
		return s.PolicyWindowSec
	}
	return s.cfg.PolicyWindowSec
}

// NoteHeaders folds the headers from one NationStates response into the state
// and reports whether it established a fresh view of the remaining budget.
// The caller must hold the lock.
func (s *State) NoteHeaders(h Headers, at time.Time) (openedWindow bool) {
	if h.Limit > 0 {
		s.BucketLimit = h.Limit
	}
	if w := ParsePolicyWindow(h.Policy); w > 0 {
		s.PolicyWindowSec = w
	}

	wasStale := s.WindowActive && !at.Before(s.WindowEndTime)
	if !s.Calibrated || wasStale {
		// A fresh allocation period is starting, so the class split has to be
		// recomputed against the allowance that is actually available.
		s.ResetClassBudgets()
	}

	opened := false
	if h.Remaining >= 0 {
		remaining := h.Remaining
		if remaining > s.BucketLimit {
			// NS should never report more than its own limit; if it does, trust
			// the limit rather than the remaining figure.
			remaining = s.BucketLimit
		}
		opened = !s.WindowActive || wasStale || remaining > s.LocalRemaining
		s.LocalRemaining = remaining

		if opened {
			// Only re-anchor on a genuinely new period. Re-anchoring on every
			// report would push the staleness horizon forward each time, so a
			// burst of appliances reporting in quick succession would keep a stale
			// remaining alive indefinitely - the same over-dispatch as minting a
			// fresh allowance per report, just harder to see.
			s.WindowActive = true
			s.WindowStartTime = at
			s.WindowEndTime = at.Add(time.Duration(s.PolicyWindow()) * time.Second)
			s.windowOpened = true
		}
	}

	if !s.Calibrated {
		s.Calibrated = true
	}
	return opened
}

// refreshRemaining discards a reported remaining that has aged past the policy
// window and assumes the bucket refilled. Without this a reported 0 would wedge
// CAS permanently: the value only clears when a report arrives, and if the
// budget is what stopped dispatch then no report is coming.
//
// A window that was never opened counts as stale for the same reason - there is
// no trustworthy figure to hold on to.
// The caller must hold the lock.
func (s *State) refreshRemaining(now time.Time) bool {
	if s.WindowActive && now.Before(s.WindowEndTime) {
		return false
	}
	s.LocalRemaining = s.BucketLimit
	s.WindowActive = true
	s.WindowStartTime = now
	s.WindowEndTime = now.Add(time.Duration(s.PolicyWindow()) * time.Second)
	s.ResetClassBudgets()
	s.windowOpened = true
	return true
}

// rollSustainedWindow advances the sustained window to cover now.
// The caller must hold the lock.
func (s *State) rollSustainedWindow(now time.Time) {
	if !s.SustainedWindowEnd.IsZero() && now.Before(s.SustainedWindowEnd) {
		return
	}
	if !s.SustainedWindowEnd.IsZero() && !s.penaltyInWindow {
		s.creditCleanWindow()
	}
	s.penaltyInWindow = false
	s.SustainedUsed = 0
	s.SustainedWindowEnd = now.Add(time.Duration(s.SustainedWindowSec) * time.Second)
}

// creditCleanWindow nudges the sustained allowance back towards its configured
// baseline after a window that completed without a penalty. The step is one
// request per window so a single unlucky report cannot talk CAS back into the
// rate that provoked the penalty in the first place.
// The caller must hold the lock.
func (s *State) creditCleanWindow() {
	if s.SustainedLimit < s.cfg.SustainedMaxLimit {
		s.SustainedLimit++
	}
}

// ConsumeWindowOpened reports whether a fresh view of the remaining budget was
// established since the last call, clearing the flag. The engine uses it to
// record the window.
// The caller must hold the lock.
func (s *State) ConsumeWindowOpened() bool {
	opened := s.windowOpened
	s.windowOpened = false
	return opened
}

// CanDispatch reports whether a new ticket may be issued right now.
// The caller must hold the lock.
func (s *State) CanDispatch() bool {
	now := time.Now()

	if s.LockdownActive {
		if now.Before(s.LockdownUntil) {
			return false
		}
		// The penalty has been served. Force the next decision to take fresh
		// headers rather than the zeroed remaining from the 429 that caused it.
		s.LockdownActive = false
		s.WindowActive = false
	}

	if !s.Calibrated {
		// Allow only the bootstrap request.
		return s.LocalRemaining > 0
	}

	s.refreshRemaining(now)
	s.rollSustainedWindow(now)

	if s.SustainedUsed >= s.SustainedLimit {
		return false
	}

	return (s.LocalRemaining - s.InFlight) > 0
}

// DeductSustained consumes one unit of the sustained budget. Callers must have
// confirmed CanDispatch first. Every path that issues a ticket must call this,
// or the sustained limit stops bounding anything.
// The caller must hold the lock.
func (s *State) DeductSustained() {
	s.SustainedUsed++
}

// Penalty describes what NotePenalty changed, so the caller can log it.
type Penalty struct {
	OldWindowSec int
	NewWindowSec int
	OldLimit     int
	NewLimit     int
	Until        time.Time
}

// NotePenalty records an upstream 429.
//
// The penalty window is taken from the response rather than assumed, because
// NationStates reports it per occurrence and the duration varies. The sustained
// allowance is also cut: the 429 is direct evidence that the rate CAS was
// dispatching exceeds what NationStates will tolerate over the long run, and
// serving the penalty without lowering the rate just reproduces it.
// The caller must hold the lock.
func (s *State) NotePenalty(retryAfterSeconds int) Penalty {
	if retryAfterSeconds > 0 {
		s.SustainedWindowSec = retryAfterSeconds
	}

	p := Penalty{
		OldWindowSec: s.SustainedWindowSec,
		OldLimit:     s.SustainedLimit,
	}

	if s.SustainedLimit > s.cfg.SustainedMinLimit {
		s.SustainedLimit /= 2
		if s.SustainedLimit < s.cfg.SustainedMinLimit {
			s.SustainedLimit = s.cfg.SustainedMinLimit
		}
	}

	p.NewWindowSec = s.SustainedWindowSec
	p.NewLimit = s.SustainedLimit

	now := time.Now()
	p.Until = now.Add(time.Duration(s.SustainedWindowSec) * time.Second)

	// Serve the penalty out of the sustained budget rather than a separate
	// timer, so a dispatch cannot slip through the moment the sustained window
	// rolls while the penalty is still running.
	s.SustainedWindowEnd = p.Until
	s.SustainedUsed = s.SustainedLimit
	s.penaltyInWindow = true
	s.Penalties++

	s.LockdownActive = true
	s.LockdownUntil = p.Until

	return p
}

// ResetClassBudgets reallocates per-class and shared dispatch quota for the new
// window based on demand accumulated since the last reset.
// The caller must hold the lock.
func (s *State) ResetClassBudgets() {
	// Identify classes that actually had demand.
	active := make([]string, 0, len(ClassConfigs))
	for name := range ClassConfigs {
		if s.DemandSum[name] > 0 {
			active = append(active, name)
		}
	}

	s.ClassRemaining = make(map[string]int)

	if len(active) == 0 {
		// No demand data yet — put everything in the shared pool.
		s.SharedRemaining = s.BucketLimit
		s.DemandSum = make(map[string]int)
		return
	}

	totalW := 0.0
	for _, name := range active {
		totalW += ClassConfigs[name].Weight
	}

	allocated := 0
	for _, name := range active {
		cfg := ClassConfigs[name]
		raw := int(math.Round(float64(s.BucketLimit) * cfg.Weight / totalW))
		quota := raw
		if demand := s.DemandSum[name]; demand < quota {
			quota = demand
		}
		maxAllowed := int(math.Floor(float64(s.BucketLimit) * cfg.MaxFraction))
		if quota > maxAllowed {
			quota = maxAllowed
		}
		s.ClassRemaining[name] = quota
		allocated += quota
	}

	s.SharedRemaining = s.BucketLimit - allocated
	if s.SharedRemaining < 0 {
		s.SharedRemaining = 0
	}

	s.DemandSum = make(map[string]int)
}

// HasClassQuota reports whether the named class has dispatch quota remaining.
// The caller must hold the lock.
func (s *State) HasClassQuota(class string) bool {
	return s.ClassRemaining[class] > 0
}

// DeductClassQuota decrements the dispatch quota for the named class by one.
// The caller must hold the lock and must have confirmed HasClassQuota first.
func (s *State) DeductClassQuota(class string) {
	s.ClassRemaining[class]--
}

// HasSharedQuota reports whether the shared (spill) pool has quota remaining.
// The caller must hold the lock.
func (s *State) HasSharedQuota() bool {
	return s.SharedRemaining > 0
}

// DeductShared decrements the shared pool by one.
// The caller must hold the lock and must have confirmed HasSharedQuota first.
func (s *State) DeductShared() {
	s.SharedRemaining--
}
