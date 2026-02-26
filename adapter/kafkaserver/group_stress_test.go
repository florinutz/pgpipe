package kafkaserver

import (
	"log/slog"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func TestGroupStress_ConcurrentJoinLeave(t *testing.T) {
	gc := newGroupCoordinator(30*time.Second, slog.Default())
	g := gc.getOrCreate("stress-group")

	var wg sync.WaitGroup
	var mu sync.Mutex
	members := make(map[string]struct{})

	// 50 goroutines joining and leaving randomly.
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// Join.
			_, _, _, _, mid, _ := g.joinGroup(joinGroupRequest{
				GroupID:      "stress-group",
				ProtocolType: "consumer",
				Protocols:    []joinGroupProtocol{{Name: "range", Metadata: []byte{0}}},
			})

			mu.Lock()
			members[mid] = struct{}{}
			mu.Unlock()

			// Random action: heartbeat or leave.
			// Note: we use generationID 1 as a best-effort value since reading
			// g.generationID without the lock would race. The heartbeat may
			// return errIllegalGeneration, which is acceptable for a stress test.
			if rand.Intn(2) == 0 {
				g.heartbeat(heartbeatRequest{
					GroupID:      "stress-group",
					GenerationID: 1,
					MemberID:     mid,
				})
			} else {
				g.leaveGroup(leaveGroupRequest{
					GroupID:  "stress-group",
					MemberID: mid,
				})
			}
		}()
	}
	wg.Wait()

	// Should not panic. Group should be in a valid state.
	state := g.state
	if state != groupEmpty && state != groupCompletingRebalance && state != groupStable && state != groupPreparingRebalance {
		t.Errorf("unexpected state: %v", state)
	}
}

func TestGroupStress_RapidRebalance(t *testing.T) {
	gc := newGroupCoordinator(30*time.Second, slog.Default())
	g := gc.getOrCreate("rapid-group")

	// Join 10 members.
	memberIDs := make([]string, 10)
	for i := 0; i < 10; i++ {
		_, _, _, _, mid, _ := g.joinGroup(joinGroupRequest{
			GroupID:      "rapid-group",
			ProtocolType: "consumer",
			Protocols:    []joinGroupProtocol{{Name: "range", Metadata: []byte{0}}},
		})
		memberIDs[i] = mid
	}

	// Sync to stabilize.
	assignments := make([]syncGroupAssignment, len(memberIDs))
	for i, mid := range memberIDs {
		assignments[i] = syncGroupAssignment{MemberID: mid, Assignment: []byte{byte(i)}}
	}
	g.syncGroup(syncGroupRequest{
		GroupID:      "rapid-group",
		GenerationID: g.generationID,
		MemberID:     g.leader,
		Assignments:  assignments,
	})

	// Leave 5 members concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			g.leaveGroup(leaveGroupRequest{
				GroupID:  "rapid-group",
				MemberID: memberIDs[idx],
			})
		}(i)
	}

	// Join 5 new members concurrently.
	newMembers := make([]string, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			_, _, _, _, mid, _ := g.joinGroup(joinGroupRequest{
				GroupID:      "rapid-group",
				ProtocolType: "consumer",
				Protocols:    []joinGroupProtocol{{Name: "range", Metadata: []byte{0}}},
			})
			newMembers[idx] = mid
		}(i)
	}

	wg.Wait()

	// Verify all new members got unique IDs.
	seen := make(map[string]struct{})
	for _, mid := range newMembers {
		if mid == "" {
			continue
		}
		if _, exists := seen[mid]; exists {
			t.Fatalf("duplicate member ID: %q", mid)
		}
		seen[mid] = struct{}{}
	}
}
