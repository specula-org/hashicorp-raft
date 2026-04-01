// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingLogStore wraps a LogStore and blocks GetLog calls on demand,
// simulating disk IO stalls that freeze replicate()/replicateTo() goroutines.
type blockingLogStore struct {
	LogStore
	mu       sync.Mutex
	blocked  atomic.Bool
	unblockC chan struct{}
}

func newBlockingLogStore(inner LogStore) *blockingLogStore {
	return &blockingLogStore{
		LogStore: inner,
		unblockC: make(chan struct{}),
	}
}

func (b *blockingLogStore) block() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blocked.Store(true)
	b.unblockC = make(chan struct{})
}

func (b *blockingLogStore) unblock() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.blocked.Load() {
		b.blocked.Store(false)
		close(b.unblockC)
	}
}

func (b *blockingLogStore) GetLog(index uint64, log *Log) error {
	if b.blocked.Load() {
		b.mu.Lock()
		ch := b.unblockC
		b.mu.Unlock()
		<-ch
	}
	return b.LogStore.GetLog(index, log)
}

// TestRaft_HeartbeatTermCheck verifies that heartbeat() should check resp.Term
// and step down when a follower responds with a higher term.
//
// The bug: In replication.go, heartbeat() calls setLastContact() unconditionally
// when the transport call succeeds, without checking resp.Term. In contrast,
// replicateTo() correctly checks resp.Term > req.Term (line 250) and calls
// handleStaleTerm() to step down.
//
// When replicate() is blocked on disk IO, only heartbeat() sends RPCs. If a
// follower has a higher term and rejects the heartbeat, heartbeat() still calls
// setLastContact(), creating "phantom contacts" that keep the stale leader's
// lease alive indefinitely via checkLeaderLease().
//
// Test phases:
//  1. Freeze replicate() by blocking GetLog on the leader
//  2. Restart F1 with a higher term (T+5), NOT connected to leader yet
//  3. Verify leader is stable (F2 provides quorum via heartbeat)
//  4. Connect F1 to leader, disconnect F2 — leader's only heartbeat target is F1
//  5. Assert: leader should step down (heartbeat gets higher term from F1)
//
// Key design choice: F1 is NOT connected to the leader until Phase 4, after F2
// is about to be disconnected. This prevents the leader from stepping down too
// early (with the fix) and avoids confounding lease expiry during F1 restart
// (which caused the previous version of this test to trivially pass without any
// code changes — LeaderLeaseTimeout=50ms was shorter than the restart window).
func TestRaft_HeartbeatTermCheck(t *testing.T) {
	conf := inmemConfig(t)
	// Use generous timeouts so the leader does NOT step down during the F1
	// restart window (~50ms). With the defaults (50ms), the leader's lease
	// expires while F1 is restarting, making the test pass for the wrong
	// reason (lease expiry instead of heartbeat term check).
	// Config validation requires: LeaderLeaseTimeout <= HeartbeatTimeout
	// and ElectionTimeout >= HeartbeatTimeout.
	conf.HeartbeatTimeout = 500 * time.Millisecond
	conf.ElectionTimeout = 500 * time.Millisecond
	conf.LeaderLeaseTimeout = 500 * time.Millisecond

	const numNodes = 3
	stores := make([]*InmemStore, numNodes)
	blockStores := make([]*blockingLogStore, numNodes)
	fsms := make([]FSM, numNodes)
	snapStores := make([]*FileSnapshotStore, numNodes)
	snapDirs := make([]string, numNodes)
	transports := make([]*InmemTransport, numNodes)
	addrs := make([]ServerAddress, numNodes)

	var configuration Configuration
	for i := 0; i < numNodes; i++ {
		stores[i] = NewInmemStore()
		blockStores[i] = newBlockingLogStore(stores[i])
		fsms[i] = &MockFSM{}
		dir, snap := FileSnapTest(t)
		snapDirs[i] = dir
		snapStores[i] = snap
		addr, tr := NewInmemTransport("")
		addrs[i] = addr
		transports[i] = tr

		localID := ServerID(fmt.Sprintf("server-%s", addr))
		configuration.Servers = append(configuration.Servers, Server{
			Suffrage: Voter,
			ID:       localID,
			Address:  addr,
		})
	}

	for i := 0; i < numNodes; i++ {
		for j := 0; j < numNodes; j++ {
			if i != j {
				transports[i].Connect(addrs[j], transports[j])
			}
		}
	}

	rafts := make([]*Raft, numNodes)
	for i := 0; i < numNodes; i++ {
		peerConf := *conf
		peerConf.LocalID = configuration.Servers[i].ID
		peerConf.Logger = newTestLoggerWithPrefix(t, string(configuration.Servers[i].ID))

		err := BootstrapCluster(&peerConf, blockStores[i], stores[i], snapStores[i], transports[i], configuration)
		if err != nil {
			t.Fatalf("BootstrapCluster failed: %v", err)
		}

		raft, err := NewRaft(&peerConf, fsms[i], blockStores[i], stores[i], snapStores[i], transports[i])
		if err != nil {
			t.Fatalf("NewRaft failed: %v", err)
		}
		rafts[i] = raft
	}

	defer func() {
		for _, bs := range blockStores {
			bs.unblock()
		}
		for _, r := range rafts {
			if r != nil {
				r.Shutdown().Error()
			}
		}
		for _, d := range snapDirs {
			os.RemoveAll(d)
		}
	}()

	// Wait for a stable leader.
	var leaderRaft *Raft
	var leaderI int
	leaderDeadline := time.After(10 * time.Second)
	for leaderRaft == nil {
		select {
		case <-leaderDeadline:
			t.Fatalf("timeout waiting for leader")
		case <-time.After(10 * time.Millisecond):
		}
		for i, r := range rafts {
			if r.State() == Leader {
				leaderRaft = r
				leaderI = i
				break
			}
		}
	}
	time.Sleep(3 * conf.HeartbeatTimeout)
	if leaderRaft.State() != Leader {
		t.Fatalf("leader lost leadership during stabilization")
	}

	f1I := (leaderI + 1) % 3
	f2I := (leaderI + 2) % 3

	// Apply data to create log entries that replicateTo will need to fetch.
	if err := leaderRaft.Apply([]byte("test"), time.Second).Error(); err != nil {
		t.Fatalf("failed to apply: %v", err)
	}
	time.Sleep(3 * conf.HeartbeatTimeout)

	leaderTerm := leaderRaft.getCurrentTerm()

	// ── Phase 1: Freeze replicate() ──────────────────────────────────────
	//
	// Block GetLog on the leader's log store. In this codebase,
	// setPreviousLog() always calls GetLog() for non-trivial cases, so the
	// next replicateTo() call blocks immediately. After this point, only
	// heartbeat() continues sending RPCs (it doesn't touch the log store).
	blockStores[leaderI].block()
	time.Sleep(50 * time.Millisecond) // Wait for in-flight replicateTo to block

	// ── Phase 2: Restart F1 with higher term (NOT connected to leader) ───
	//
	// Disconnect F1 from the leader first. The leader's heartbeat goroutine
	// for F1 will get "failed to connect" errors, which is fine — the leader
	// still has F2 for quorum: {L, F2} = 2 >= quorum(3) = 2.
	transports[leaderI].Disconnect(addrs[f1I])
	transports[f1I].Disconnect(addrs[leaderI])

	f1Shutdown := rafts[f1I].Shutdown()
	if err := f1Shutdown.Error(); err != nil {
		t.Fatalf("F1 shutdown failed: %v", err)
	}

	// Bump F1's persisted term to simulate F1 having participated in a
	// higher-term election during a transient partition.
	bumpedTerm := leaderTerm + 5
	if err := stores[f1I].SetUint64(keyCurrentTerm, bumpedTerm); err != nil {
		t.Fatalf("failed to bump F1 term: %v", err)
	}

	// Create new F1 at the same address but DO NOT connect to the leader yet.
	// High election timeout prevents F1 from starting its own elections.
	_, newF1Trans := NewInmemTransport(addrs[f1I])

	newF1Conf := *conf
	newF1Conf.LocalID = configuration.Servers[f1I].ID
	newF1Conf.Logger = newTestLoggerWithPrefix(t, string(configuration.Servers[f1I].ID)+"-restarted")
	newF1Conf.HeartbeatTimeout = 10 * time.Minute
	newF1Conf.ElectionTimeout = 10 * time.Minute
	newF1Conf.LeaderLeaseTimeout = 10 * time.Minute

	newF1Snap, err := NewFileSnapshotStoreWithLogger(snapDirs[f1I], 3, newTestLogger(t))
	if err != nil {
		t.Fatalf("failed to create F1 snapshot store: %v", err)
	}
	newF1Snap.noSync = true

	newF1Raft, err := NewRaft(&newF1Conf, &MockFSM{}, stores[f1I], stores[f1I], newF1Snap, newF1Trans)
	if err != nil {
		t.Fatalf("NewRaft for restarted F1 failed: %v", err)
	}
	rafts[f1I] = newF1Raft

	if f1Term := newF1Raft.getCurrentTerm(); f1Term != bumpedTerm {
		t.Fatalf("F1 should have term %d, got %d", bumpedTerm, f1Term)
	}

	// ── Phase 3: Verify leader survived F1 restart ───────────────────────
	//
	// F1 is NOT connected to the leader. The leader maintains its lease
	// purely via F2's heartbeat responses. This check catches the scenario
	// where the leader steps down for the wrong reason (e.g., lease expiry
	// during restart — the bug in the previous version of this test).
	time.Sleep(200 * time.Millisecond)
	if leaderRaft.State() != Leader {
		t.Fatalf("leader should still be leader (F2 provides quorum via heartbeat), got %v",
			leaderRaft.State())
	}

	// ── Phase 4: Connect F1, disconnect F2 ───────────────────────────────
	//
	// Now the leader's only heartbeat target is F1 at term T+5. When the
	// leader sends AppendEntries{Term: T}, F1's appendEntries handler sees
	// T < T+5, rejects, and returns resp.Term = T+5, resp.Success = false.
	//
	// Connect F1 first (so the leader picks up phantom contacts from F1
	// before F2's last contact expires), then disconnect F2.
	newF1Trans.Connect(addrs[leaderI], transports[leaderI])
	transports[leaderI].Connect(addrs[f1I], newF1Trans)

	transports[leaderI].Disconnect(addrs[f2I])
	transports[f2I].Disconnect(addrs[leaderI])

	// ── Phase 5: Assert leader steps down ────────────────────────────────
	//
	// Expected behavior WITH the bug (current code):
	//   heartbeat() calls setLastContact() despite resp.Term > req.Term.
	//   checkLeaderLease() counts: contacted = {L, F1(phantom)} = 2 = quorum.
	//   Leader stays alive indefinitely via phantom contacts.
	//   → Test FAILS (leader never steps down within the deadline).
	//
	// Expected behavior WITH the fix:
	//   heartbeat() checks resp.Term > req.Term → handleStaleTerm() → Follower.
	//   Leader steps down within one heartbeat cycle (~5ms).
	//   → Test PASSES.
	steppedDown := false
	deadline := time.Now().Add(3 * conf.LeaderLeaseTimeout)
	for time.Now().Before(deadline) {
		if leaderRaft.State() != Leader {
			steppedDown = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !steppedDown {
		t.Fatalf("HEARTBEAT TERM CHECK BUG: leader at term %d did not step down after "+
			"heartbeat to F1 at term %d. heartbeat() in replication.go calls setLastContact() "+
			"without checking resp.Term, creating phantom contacts that keep the lease alive. "+
			"Fix: add `if resp.Term > req.Term { r.handleStaleTerm(s); return }` before setLastContact().",
			leaderRaft.getCurrentTerm(), newF1Raft.getCurrentTerm())
	}
}
