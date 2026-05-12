package paxos

import (
	"context"
	"fmt"
	"sync"
)

// LocalTransport wires proposers directly to in-process acceptors.
// Used for tests and single-binary deployments; replace with gRPC for
// production multi-node setups.
type LocalTransport struct {
	mu        sync.RWMutex
	acceptors map[string]*Acceptor
	// hooks for fault injection in tests
	dropRate   map[string]float64
	delayHooks map[string]func()
}

func NewLocalTransport() *LocalTransport {
	return &LocalTransport{
		acceptors:  make(map[string]*Acceptor),
		dropRate:   make(map[string]float64),
		delayHooks: make(map[string]func()),
	}
}

func (t *LocalTransport) Register(a *Acceptor) {
	t.mu.Lock()
	t.acceptors[a.ID()] = a
	t.mu.Unlock()
}

// InjectDelay registers a hook called before each message to acceptorID.
func (t *LocalTransport) InjectDelay(acceptorID string, fn func()) {
	t.mu.Lock()
	t.delayHooks[acceptorID] = fn
	t.mu.Unlock()
}

// Partition marks acceptorID as unreachable.
func (t *LocalTransport) Partition(acceptorID string) {
	t.mu.Lock()
	t.dropRate[acceptorID] = 1.0
	t.mu.Unlock()
}

// Heal removes a partition.
func (t *LocalTransport) Heal(acceptorID string) {
	t.mu.Lock()
	delete(t.dropRate, acceptorID)
	t.mu.Unlock()
}

func (t *LocalTransport) Prepare(ctx context.Context, acceptorID string, msg PrepareMsg) (PromiseMsg, error) {
	t.mu.RLock()
	a, ok := t.acceptors[acceptorID]
	dr := t.dropRate[acceptorID]
	hook := t.delayHooks[acceptorID]
	t.mu.RUnlock()

	if !ok {
		return PromiseMsg{}, fmt.Errorf("unknown acceptor %s", acceptorID)
	}
	if dr >= 1.0 {
		return PromiseMsg{}, fmt.Errorf("acceptor %s is partitioned", acceptorID)
	}
	if hook != nil {
		hook()
	}
	return a.Prepare(ctx, msg)
}

func (t *LocalTransport) Accept(ctx context.Context, acceptorID string, msg AcceptMsg) (AcceptedMsg, error) {
	t.mu.RLock()
	a, ok := t.acceptors[acceptorID]
	dr := t.dropRate[acceptorID]
	hook := t.delayHooks[acceptorID]
	t.mu.RUnlock()

	if !ok {
		return AcceptedMsg{}, fmt.Errorf("unknown acceptor %s", acceptorID)
	}
	if dr >= 1.0 {
		return AcceptedMsg{}, fmt.Errorf("acceptor %s is partitioned", acceptorID)
	}
	if hook != nil {
		hook()
	}
	return a.Accept(ctx, msg)
}
