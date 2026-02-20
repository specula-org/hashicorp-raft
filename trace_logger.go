// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package raft

// TraceLogger is an optional interface for emitting structured trace events
// for TLA+ trace validation. When set on Config, the Raft node will emit
// events at key state transitions to be captured as NDJSON traces.
type TraceLogger interface {
	TraceEvent(event TraceEvent)
}

// TraceEvent represents a single state transition event for trace validation.
type TraceEvent struct {
	Name  string      `json:"name"`
	NID   string      `json:"nid"`
	State *TraceState `json:"state"`
	Msg   *TraceMsg   `json:"msg,omitempty"`
}

// TraceState captures the node's state at the time of the event.
type TraceState struct {
	Term         uint64 `json:"term"`
	VotedFor     string `json:"votedFor"`
	Role         string `json:"role"`
	CommitIndex  uint64 `json:"commitIndex"`
	LastLogIndex uint64 `json:"lastLogIndex"`
	LastLogTerm  uint64 `json:"lastLogTerm"`
}

// TraceMsg captures message information for message-related events.
type TraceMsg struct {
	Type         string `json:"type"`
	Subtype      string `json:"subtype,omitempty"`
	Term         uint64 `json:"term"`
	From         string `json:"from"`
	To           string `json:"to"`
	PrevLogIndex uint64 `json:"prevLogIndex,omitempty"`
	PrevLogTerm  uint64 `json:"prevLogTerm,omitempty"`
	Entries      int    `json:"entries"`
	CommitIndex  uint64 `json:"commitIndex,omitempty"`
	Success      bool   `json:"success,omitempty"`
	MatchIndex   uint64 `json:"matchIndex,omitempty"`
	VoteGranted  bool   `json:"voteGranted,omitempty"`
	LastLogTerm  uint64 `json:"lastLogTerm,omitempty"`
	LastLogIndex uint64 `json:"lastLogIndex,omitempty"`
}

// getTraceState returns a snapshot of the current node state for tracing.
func (r *Raft) getTraceState() *TraceState {
	lastIdx, lastTerm := r.getLastLog()
	role := "Follower"
	switch r.getState() {
	case Candidate:
		role = "Candidate"
	case Leader:
		role = "Leader"
	}
	return &TraceState{
		Term:         r.getCurrentTerm(),
		VotedFor:     string(r.traceVotedFor),
		Role:         role,
		CommitIndex:  r.getCommitIndex(),
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
}

// traceEvent emits a trace event if TraceLogger is configured.
func (r *Raft) traceEvent(name string, msg *TraceMsg) {
	if r.traceLogger == nil {
		return
	}
	r.traceLogger.TraceEvent(TraceEvent{
		Name:  name,
		NID:   string(r.localID),
		State: r.getTraceState(),
		Msg:   msg,
	})
}
