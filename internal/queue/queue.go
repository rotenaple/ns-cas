// Package queue implements a priority queue with aging (starvation prevention).
// Items are sorted by effective priority = BasePriority + (AgingWeight × AgeSeconds).
// The queue is protected by the outer State mutex; all methods here are NOT thread-safe
// and must only be called while holding state.Lock().
package queue

import (
	"sort"
	"time"
)

// Item is a waiting long-poll request.
type Item struct {
	ApplianceID   string
	PriorityClass string
	BasePriority  float64
	QueuedAt      time.Time
	Backlog       int  // pending requests behind this one; used for demand tracking
	// ResponseChan receives true when a slot opens, false on shutdown/timeout.
	ResponseChan chan bool
}

// Queue is an ordered list of waiting items.
type Queue struct {
	items       []*Item
	agingWeight float64
}

// New creates a Queue with the given aging weight ω.
func New(agingWeight float64) *Queue {
	return &Queue{agingWeight: agingWeight}
}

// Len returns the number of items waiting.
func (q *Queue) Len() int { return len(q.items) }

// Enqueue adds an item to the queue. O(n) sort — queue is expected to be short.
func (q *Queue) Enqueue(item *Item) {
	q.items = append(q.items, item)
}

// PopBest removes and returns the highest-effective-priority item, or nil if empty.
func (q *Queue) PopBest() *Item {
	if len(q.items) == 0 {
		return nil
	}
	q.sortByEffective()
	best := q.items[0]
	q.items = q.items[1:]
	return best
}

// Remove removes the item for the given appliance (used when a long-poll times out
// before being granted a ticket). Returns true if found and removed.
func (q *Queue) Remove(applianceID string) bool {
	for i, it := range q.items {
		if it.ApplianceID == applianceID {
			q.items = append(q.items[:i], q.items[i+1:]...)
			return true
		}
	}
	return false
}

// HasAppliance reports whether applianceID already has an item in the queue.
func (q *Queue) HasAppliance(applianceID string) bool {
	for _, it := range q.items {
		if it.ApplianceID == applianceID {
			return true
		}
	}
	return false
}

// sortByEffective sorts descending by EffectivePriority = BasePriority + ω×AgeSeconds.
func (q *Queue) sortByEffective() {
	now := time.Now()
	sort.SliceStable(q.items, func(i, j int) bool {
		return q.effectivePriority(q.items[i], now) > q.effectivePriority(q.items[j], now)
	})
}

func (q *Queue) effectivePriority(item *Item, now time.Time) float64 {
	age := now.Sub(item.QueuedAt).Seconds()
	return item.BasePriority + q.agingWeight*age
}

// DrainAll sends false to every waiting item (used on shutdown).
func (q *Queue) DrainAll() {
	for _, it := range q.items {
		select {
		case it.ResponseChan <- false:
		default:
		}
	}
	q.items = nil
}

// Snapshot returns a copy of current queue state for the status endpoint.
func (q *Queue) Snapshot() []SnapshotItem {
	now := time.Now()
	out := make([]SnapshotItem, len(q.items))
	for i, it := range q.items {
		out[i] = SnapshotItem{
			ApplianceID:       it.ApplianceID,
			PriorityClass:     it.PriorityClass,
			WaitSeconds:       now.Sub(it.QueuedAt).Seconds(),
			EffectivePriority: it.BasePriority + q.agingWeight*now.Sub(it.QueuedAt).Seconds(),
			Backlog:           it.Backlog,
		}
	}
	return out
}

// SnapshotItem is a read-only view of a queue entry.
type SnapshotItem struct {
	ApplianceID       string
	PriorityClass     string
	WaitSeconds       float64
	EffectivePriority float64
	Backlog           int
}
