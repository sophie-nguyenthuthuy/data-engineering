// Package transport provides an in-memory message bus that simulates network
// communication between consensus nodes. It supports Byzantine injection:
// selective message drops, delays, and corruption for testing fault paths.
package transport

import (
	"math/rand"
	"sync"
	"time"
)

// Msg is an envelope passed between nodes.
type Msg struct {
	From    int
	To      int    // -1 = broadcast
	Type    string
	Payload []byte
}

// Handler is the callback a node registers to receive messages.
type Handler func(msg Msg)

// Config controls fault injection on a per-node basis.
type Config struct {
	// DropRate is the probability [0,1) that an outbound message is silently dropped.
	DropRate float64
	// LatencyMin/Max add artificial per-message latency (default 0).
	LatencyMin time.Duration
	LatencyMax time.Duration
	// Byzantine: if true, payloads are randomly corrupted before delivery.
	Byzantine bool
}

// Bus is a synchronous in-memory network.
type Bus struct {
	mu       sync.RWMutex
	handlers map[int]Handler
	configs  map[int]Config
	rng      *rand.Rand
}

func NewBus() *Bus {
	return &Bus{
		handlers: make(map[int]Handler),
		configs:  make(map[int]Config),
		rng:      rand.New(rand.NewSource(42)),
	}
}

func (b *Bus) Register(id int, h Handler) {
	b.mu.Lock()
	b.handlers[id] = h
	b.mu.Unlock()
}

func (b *Bus) SetConfig(id int, cfg Config) {
	b.mu.Lock()
	b.configs[id] = cfg
	b.mu.Unlock()
}

// Send delivers a message from one node. If msg.To == -1, it broadcasts to all
// registered nodes except the sender.
func (b *Bus) Send(msg Msg) {
	b.mu.RLock()
	cfg := b.configs[msg.From]
	b.mu.RUnlock()

	// Drop check
	if cfg.DropRate > 0 {
		b.mu.Lock()
		drop := b.rng.Float64() < cfg.DropRate
		b.mu.Unlock()
		if drop {
			return
		}
	}

	// Latency
	if cfg.LatencyMax > 0 {
		b.mu.Lock()
		jitter := time.Duration(b.rng.Int63n(int64(cfg.LatencyMax-cfg.LatencyMin+1))) + cfg.LatencyMin
		b.mu.Unlock()
		time.Sleep(jitter)
	}

	deliver := func(to int, payload []byte) {
		b.mu.RLock()
		h, ok := b.handlers[to]
		b.mu.RUnlock()
		if !ok {
			return
		}
		p := make([]byte, len(payload))
		copy(p, payload)
		if cfg.Byzantine && len(p) > 0 {
			b.mu.Lock()
			p[b.rng.Intn(len(p))] ^= 0xFF
			b.mu.Unlock()
		}
		// Deliver asynchronously to avoid deadlocks when senders hold their own
		// mutex and receivers try to acquire it during handler callbacks.
		go h(Msg{From: msg.From, To: to, Type: msg.Type, Payload: p})
	}

	if msg.To >= 0 {
		deliver(msg.To, msg.Payload)
		return
	}

	// Broadcast
	b.mu.RLock()
	ids := make([]int, 0, len(b.handlers))
	for id := range b.handlers {
		ids = append(ids, id)
	}
	b.mu.RUnlock()
	for _, id := range ids {
		if id != msg.From {
			deliver(id, msg.Payload)
		}
	}
}
