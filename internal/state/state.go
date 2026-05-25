// Package state defines the core in-memory data structures for the CAS.
// All fields are protected by the embedded sync.Mutex. Callers must hold
// the lock for any read or write of mutable fields.
package state

import (
	"math"
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

	// Quota tracking
	BucketLimit    int // From RateLimit-Limit header; bootstrapped to 50
	LocalRemaining int // Last known remaining, reconciled from telemetry
	InFlight       int // Tickets granted but not yet reported

	// Per-appliance ticket map; enforces one-in-flight per appliance
	ActiveTickets map[string]*ActiveTicket

	// Calibration
	Calibrated bool // False until first /report is processed

	// Window tracking
	WindowActive      bool
	WindowStartTime   time.Time
	WindowEndTime     time.Time
	WindowDetectedVia string // "49_trigger" (high accuracy) or "reset_fallback" (low accuracy)

	// Hard lockdown after a 429
	LockdownActive bool
	LockdownUntil  time.Time

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
func New() *State {
	return &State{
		BucketLimit:     50, // Overwritten on first /report via RateLimit-Limit
		LocalRemaining:  1,  // Conservative: allow exactly one cold-start request
		Calibrated:      false,
		ActiveTickets:   make(map[string]*ActiveTicket),
		DemandSum:       make(map[string]int),
		ClassRemaining:  make(map[string]int),
		SharedRemaining: 50, // matches initial BucketLimit; all to shared until first window reset
	}
}

// CanDispatch reports whether a new ticket may be issued right now.
// The caller must hold the lock.
func (s *State) CanDispatch() bool {
	now := time.Now()

	if s.LockdownActive {
		if now.Before(s.LockdownUntil) {
			return false
		}
		// Lockdown expired: assume half bucket; telemetry will reconcile.
		s.LockdownActive = false
		s.LocalRemaining = s.BucketLimit / 2
		s.WindowActive = false
	}

	if s.WindowActive && now.After(s.WindowEndTime) {
		// Window expired: reset to full bucket and reallocate class budgets.
		s.WindowActive = false
		s.WindowDetectedVia = ""
		s.LocalRemaining = s.BucketLimit
		s.ResetClassBudgets()
	}

	if !s.Calibrated {
		// Allow only the bootstrap request.
		return s.LocalRemaining > 0
	}

	return (s.LocalRemaining - s.InFlight) > 0
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
