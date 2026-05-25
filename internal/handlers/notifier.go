package handlers

import (
	"fmt"
	"sync"
)

// Notifier manages SSE subscribers and broadcasts state-change signals.
type Notifier struct {
	mu   sync.Mutex
	subs map[string]chan struct{}
	idSeq int
}

// NewNotifier creates a Notifier.
func NewNotifier() *Notifier {
	return &Notifier{subs: make(map[string]chan struct{})}
}

// Subscribe adds a subscriber and returns its ID and receive channel.
// The channel receives a value whenever Notify is called.
func (n *Notifier) Subscribe() (string, <-chan struct{}) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.idSeq++
	id := fmt.Sprintf("%d", n.idSeq)
	ch := make(chan struct{}, 8)
	n.subs[id] = ch
	return id, ch
}

// Unsubscribe removes a subscriber by ID.
func (n *Notifier) Unsubscribe(id string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.subs, id)
}

// Notify broadcasts to all subscribers. Never blocks.
func (n *Notifier) Notify() {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, ch := range n.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
