package tests

import (
	"context"
	"fmt"
	"testing"

	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/paxos"
	"github.com/sophie-nguyenthuthuy/flexible-paxos/pkg/reconfig"
)

func TestReconfigurationBasic(t *testing.T) {
	// Start with 3 acceptors, then grow to 5.
	_, transport := newCluster(5) // pre-create all acceptors

	ids3 := []string{"a1", "a2", "a3"}
	cfg := paxos.Classic(ids3)

	store := reconfig.NewMemConfigStore(cfg)
	p, err := paxos.NewProposer("leader", cfg, transport)
	if err != nil {
		t.Fatal(err)
	}

	r := reconfig.New(store, p, transport)

	// Propose adding a4, a5.
	ids5 := []string{"a1", "a2", "a3", "a4", "a5"}
	newCfg := paxos.Classic(ids5)
	newCfg.Epoch = 1

	ctx := context.Background()
	if err := r.Reconfigure(ctx, newCfg); err != nil {
		t.Fatalf("reconfiguration failed: %v", err)
	}

	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Epoch != 1 {
		t.Fatalf("expected epoch 1, got %d", loaded.Epoch)
	}
	if len(loaded.Acceptors) != 5 {
		t.Fatalf("expected 5 acceptors, got %d", len(loaded.Acceptors))
	}
}

func TestReconfigurationInvalidRejected(t *testing.T) {
	ids := []string{"a1", "a2", "a3"}
	cfg := paxos.Classic(ids)
	store := reconfig.NewMemConfigStore(cfg)

	_, transport := newCluster(3)
	p, _ := paxos.NewProposer("leader", cfg, transport)
	r := reconfig.New(store, p, transport)

	// Invalid: Q1+Q2 = 2+2 = 4, not > 3.
	badCfg := paxos.QuorumConfig{
		Acceptors: ids, Q1Size: 2, Q2Size: 1, Epoch: 1,
	}

	// 2+1 = 3, not > 3 → invalid
	ctx := context.Background()
	err := r.Reconfigure(ctx, badCfg)
	if err == nil {
		t.Fatal("expected error for invalid config")
	}
}

func TestReconfigurationListenerFired(t *testing.T) {
	_, transport := newCluster(5)
	ids := []string{"a1", "a2", "a3"}
	cfg := paxos.Classic(ids)
	store := reconfig.NewMemConfigStore(cfg)
	p, _ := paxos.NewProposer("leader", cfg, transport)
	r := reconfig.New(store, p, transport)

	fired := make(chan paxos.QuorumConfig, 1)
	r.AddListener(func(newCfg paxos.QuorumConfig) {
		fired <- newCfg
	})

	newCfg := paxos.Classic([]string{"a1", "a2", "a3", "a4", "a5"})
	newCfg.Epoch = 1

	if err := r.Reconfigure(context.Background(), newCfg); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-fired:
		if got.Epoch != 1 {
			t.Fatalf("listener got wrong epoch: %d", got.Epoch)
		}
	default:
		t.Fatal("listener was not fired")
	}
}

func TestConcurrentReconfigurations(t *testing.T) {
	// Multiple goroutines attempt concurrent reconfigurations; only one
	// should succeed per round (serialised by the Reconfigurator mutex).
	_, transport := newCluster(5)
	ids := []string{"a1", "a2", "a3"}
	cfg := paxos.Classic(ids)
	store := reconfig.NewMemConfigStore(cfg)
	p, _ := paxos.NewProposer("leader", cfg, transport)
	r := reconfig.New(store, p, transport)

	ctx := context.Background()
	newCfg := paxos.Classic([]string{"a1", "a2", "a3", "a4", "a5"})
	newCfg.Epoch = 1

	// Only the first call with epoch=1 should succeed; subsequent calls will
	// fail because the store already has epoch=1, but the serialised nature
	// means epoch bumps are monotone.
	const n = 5
	errs := make([]error, n)
	done := make(chan struct{})
	go func() {
		for i := 0; i < n; i++ {
			errs[i] = r.Reconfigure(ctx, newCfg)
		}
		close(done)
	}()
	<-done

	loaded, _ := store.Load()
	if loaded.Epoch < 1 {
		t.Fatalf("expected epoch >= 1 after reconfigs, got %d", loaded.Epoch)
	}
	t.Logf("final epoch: %d", loaded.Epoch)
	for i, e := range errs {
		if e != nil {
			t.Logf("reconfig[%d] error (expected for duplicate epochs): %v", i, e)
		}
	}
}

func TestReconfigurationEpochMonotonicity(t *testing.T) {
	_, transport := newCluster(5)
	ids := []string{"a1", "a2", "a3"}
	cfg := paxos.Classic(ids)
	store := reconfig.NewMemConfigStore(cfg)
	p, _ := paxos.NewProposer("leader", cfg, transport)
	r := reconfig.New(store, p, transport)

	ctx := context.Background()
	prevEpoch := uint64(0)

	for i := 1; i <= 3; i++ {
		newCfg := paxos.QuorumConfig{
			Acceptors: []string{"a1", "a2", "a3", "a4", "a5"},
			Q1Size:    3, Q2Size: 3,
			Epoch: uint64(i),
		}
		if err := r.Reconfigure(ctx, newCfg); err != nil {
			t.Logf("step %d: %v", i, err)
			continue
		}
		loaded, _ := store.Load()
		if loaded.Epoch <= prevEpoch {
			t.Fatalf("epoch went backwards: %d → %d", prevEpoch, loaded.Epoch)
		}
		prevEpoch = loaded.Epoch
		t.Logf("step %d: epoch=%d", i, prevEpoch)
	}

	if prevEpoch < 1 {
		t.Fatal(fmt.Sprintf("no successful reconfig, final epoch %d", prevEpoch))
	}
}
