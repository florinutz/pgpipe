package kafkaserver

import (
	"testing"
	"time"
)

func TestGroupStateTransitions(t *testing.T) {
	g := newConsumerGroup("test-group", 30*time.Second)

	if g.state != groupEmpty {
		t.Fatalf("initial state: got %v, want Empty", g.state)
	}

	// First JoinGroup: Empty -> CompletingRebalance.
	errCode, genID, _, leader, member1, _ := g.joinGroup(joinGroupRequest{
		GroupID:      "test-group",
		ProtocolType: "consumer",
		Protocols:    []joinGroupProtocol{{Name: "range", Metadata: []byte{0}}},
	})
	if errCode != errNone {
		t.Fatalf("first join: error code %d", errCode)
	}
	if genID != 1 {
		t.Errorf("first join: generationID = %d, want 1", genID)
	}
	if member1 == "" {
		t.Fatal("first join: empty member ID")
	}
	if leader != member1 {
		t.Errorf("first join: leader = %q, want %q (sole member)", leader, member1)
	}
	if g.state != groupCompletingRebalance {
		t.Errorf("after first join: state = %v, want CompletingRebalance", g.state)
	}

	// SyncGroup from leader: CompletingRebalance -> Stable.
	syncErr, _ := g.syncGroup(syncGroupRequest{
		GroupID:      "test-group",
		GenerationID: genID,
		MemberID:     member1,
		Assignments: []syncGroupAssignment{
			{MemberID: member1, Assignment: []byte{1, 2, 3}},
		},
	})
	if syncErr != errNone {
		t.Fatalf("sync: error code %d", syncErr)
	}
	if g.state != groupStable {
		t.Errorf("after sync: state = %v, want Stable", g.state)
	}

	// Second member joins: Stable -> CompletingRebalance (rebalance).
	_, genID2, _, _, member2, _ := g.joinGroup(joinGroupRequest{
		GroupID:      "test-group",
		ProtocolType: "consumer",
		Protocols:    []joinGroupProtocol{{Name: "range", Metadata: []byte{0}}},
	})
	if genID2 != 2 {
		t.Errorf("second join: generationID = %d, want 2", genID2)
	}
	if member2 == "" || member2 == member1 {
		t.Errorf("second join: member should be new, got %q", member2)
	}
	if g.state != groupCompletingRebalance {
		t.Errorf("after second join: state = %v, want CompletingRebalance", g.state)
	}
}

func TestGroupLeaveGroup(t *testing.T) {
	g := newConsumerGroup("test-group", 30*time.Second)

	// Join two members.
	_, _, _, _, m1, _ := g.joinGroup(joinGroupRequest{
		GroupID:      "test-group",
		ProtocolType: "consumer",
		Protocols:    []joinGroupProtocol{{Name: "range"}},
	})
	_, _, _, _, m2, _ := g.joinGroup(joinGroupRequest{
		GroupID:      "test-group",
		ProtocolType: "consumer",
		Protocols:    []joinGroupProtocol{{Name: "range"}},
	})

	// Sync to stabilize.
	g.syncGroup(syncGroupRequest{
		GroupID:      "test-group",
		GenerationID: g.generationID,
		MemberID:     g.leader,
		Assignments: []syncGroupAssignment{
			{MemberID: m1, Assignment: []byte{1}},
			{MemberID: m2, Assignment: []byte{2}},
		},
	})

	// Leave m2: should trigger rebalance.
	errCode := g.leaveGroup(leaveGroupRequest{
		GroupID:  "test-group",
		MemberID: m2,
	})
	if errCode != errNone {
		t.Fatalf("leaveGroup: error code %d", errCode)
	}

	// State should be CompletingRebalance (rebalance triggered).
	if g.state != groupCompletingRebalance {
		t.Errorf("after leave: state = %v, want CompletingRebalance", g.state)
	}

	// Leave last member.
	errCode = g.leaveGroup(leaveGroupRequest{
		GroupID:  "test-group",
		MemberID: m1,
	})
	if errCode != errNone {
		t.Fatalf("leaveGroup last: error code %d", errCode)
	}
	if g.state != groupEmpty {
		t.Errorf("after all left: state = %v, want Empty", g.state)
	}
}

func TestGroupHeartbeat(t *testing.T) {
	g := newConsumerGroup("test-group", 30*time.Second)

	_, genID, _, _, m1, _ := g.joinGroup(joinGroupRequest{
		GroupID:      "test-group",
		ProtocolType: "consumer",
		Protocols:    []joinGroupProtocol{{Name: "range"}},
	})

	// Sync to stabilize.
	g.syncGroup(syncGroupRequest{
		GroupID:      "test-group",
		GenerationID: genID,
		MemberID:     m1,
		Assignments:  []syncGroupAssignment{{MemberID: m1, Assignment: []byte{1}}},
	})

	// Heartbeat should succeed.
	errCode := g.heartbeat(heartbeatRequest{
		GroupID:      "test-group",
		GenerationID: genID,
		MemberID:     m1,
	})
	if errCode != errNone {
		t.Errorf("heartbeat: error code %d, want 0", errCode)
	}

	// Heartbeat with wrong generation.
	errCode = g.heartbeat(heartbeatRequest{
		GroupID:      "test-group",
		GenerationID: genID - 1,
		MemberID:     m1,
	})
	if errCode != errIllegalGeneration {
		t.Errorf("heartbeat wrong gen: error code %d, want %d", errCode, errIllegalGeneration)
	}

	// Heartbeat with unknown member.
	errCode = g.heartbeat(heartbeatRequest{
		GroupID:      "test-group",
		GenerationID: genID,
		MemberID:     "unknown",
	})
	if errCode != errUnknownMemberID {
		t.Errorf("heartbeat unknown member: error code %d, want %d", errCode, errUnknownMemberID)
	}
}

func TestGroupReapExpired(t *testing.T) {
	g := newConsumerGroup("test-group", 50*time.Millisecond)

	_, _, _, _, _, _ = g.joinGroup(joinGroupRequest{
		GroupID:      "test-group",
		ProtocolType: "consumer",
		Protocols:    []joinGroupProtocol{{Name: "range"}},
	})

	// Members are alive right now.
	if g.reapExpired() {
		t.Error("reapExpired should not have expired any members")
	}

	// Wait for session timeout.
	time.Sleep(100 * time.Millisecond)

	if !g.reapExpired() {
		t.Error("reapExpired should have expired the member")
	}
	if g.state != groupEmpty {
		t.Errorf("after reap: state = %v, want Empty", g.state)
	}
}
