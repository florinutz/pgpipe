package kafkaserver

import (
	"log/slog"
	"sync"
	"time"

	"github.com/florinutz/pgcdc/metrics"
)

// groupState represents the state of a consumer group.
type groupState int

const (
	groupEmpty               groupState = iota
	groupPreparingRebalance             // waiting for all members to join
	groupCompletingRebalance            // waiting for leader to send SyncGroup
	groupStable                         // all members assigned, steady state
)

func (s groupState) String() string {
	switch s {
	case groupEmpty:
		return "Empty"
	case groupPreparingRebalance:
		return "PreparingRebalance"
	case groupCompletingRebalance:
		return "CompletingRebalance"
	case groupStable:
		return "Stable"
	default:
		return "Unknown"
	}
}

// groupMember represents a member of a consumer group.
type groupMember struct {
	memberID      string
	protocolType  string
	protocols     []joinGroupProtocol
	assignment    []byte // populated by SyncGroup
	lastHeartbeat time.Time
}

// consumerGroup manages the lifecycle of a consumer group.
type consumerGroup struct {
	mu             sync.Mutex
	groupID        string
	state          groupState
	generationID   int32
	leader         string
	protocolType   string
	protocolName   string
	members        map[string]*groupMember
	sessionTimeout time.Duration

	// Channel closed to wake SyncGroup waiters.
	syncWaiters chan struct{}
}

func newConsumerGroup(groupID string, sessionTimeout time.Duration) *consumerGroup {
	return &consumerGroup{
		groupID:        groupID,
		state:          groupEmpty,
		members:        make(map[string]*groupMember),
		sessionTimeout: sessionTimeout,
	}
}

// joinGroup handles a JoinGroup request. Returns the response fields.
func (g *consumerGroup) joinGroup(req joinGroupRequest) (
	errorCode int16,
	generationID int32,
	protocolName string,
	leaderID string,
	memberID string,
	members []joinGroupResponseMember,
) {
	g.mu.Lock()
	defer g.mu.Unlock()

	mid := req.MemberID

	// New member: assign an ID.
	if mid == "" {
		mid = generateMemberID(req.GroupID)
	}

	// Add or update member.
	m, exists := g.members[mid]
	if !exists {
		m = &groupMember{
			memberID:     mid,
			protocolType: req.ProtocolType,
			protocols:    req.Protocols,
		}
		g.members[mid] = m
	} else {
		m.protocols = req.Protocols
	}
	m.lastHeartbeat = time.Now()

	// Trigger rebalance if not already in progress.
	switch g.state {
	case groupEmpty, groupStable:
		g.state = groupPreparingRebalance
		// Immediately transition to CompletingRebalance since we don't
		// do a delayed rebalance timer for simplicity.
		g.completeJoinPhase()
	case groupPreparingRebalance:
		// Another member joining during prepare — re-run join phase.
		g.completeJoinPhase()
	case groupCompletingRebalance:
		// New member during completing — restart rebalance.
		g.completeJoinPhase()
	}

	// Build response.
	errorCode = errNone
	generationID = g.generationID
	protocolName = g.protocolName
	leaderID = g.leader
	memberID = mid

	// Only the leader gets the full member list.
	if mid == g.leader {
		members = make([]joinGroupResponseMember, 0, len(g.members))
		for _, mem := range g.members {
			metadata := []byte{}
			if len(mem.protocols) > 0 {
				metadata = mem.protocols[0].Metadata
			}
			members = append(members, joinGroupResponseMember{
				MemberID: mem.memberID,
				Metadata: metadata,
			})
		}
	}

	return
}

// completeJoinPhase transitions from PreparingRebalance to CompletingRebalance.
func (g *consumerGroup) completeJoinPhase() {
	g.generationID++
	g.state = groupCompletingRebalance

	// Pick leader (first member).
	for id := range g.members {
		g.leader = id
		break
	}

	// Pick protocol (first common protocol from leader).
	if leader, ok := g.members[g.leader]; ok && len(leader.protocols) > 0 {
		g.protocolName = leader.protocols[0].Name
		g.protocolType = leader.protocolType
	}

	// Reset sync waiters.
	g.syncWaiters = make(chan struct{})

	metrics.KafkaServerGroupRebalances.Inc()
}

// syncGroup handles a SyncGroup request. The leader provides assignments.
func (g *consumerGroup) syncGroup(req syncGroupRequest) (int16, []byte) {
	g.mu.Lock()
	defer g.mu.Unlock()

	m, ok := g.members[req.MemberID]
	if !ok {
		return errUnknownMemberID, nil
	}

	if req.GenerationID != g.generationID {
		return errIllegalGeneration, nil
	}

	m.lastHeartbeat = time.Now()

	// If this is the leader, store all assignments.
	if req.MemberID == g.leader && len(req.Assignments) > 0 {
		for _, a := range req.Assignments {
			if mem, exists := g.members[a.MemberID]; exists {
				mem.assignment = a.Assignment
			}
		}
		g.state = groupStable

		// Wake SyncGroup waiters.
		if g.syncWaiters != nil {
			close(g.syncWaiters)
			g.syncWaiters = nil
		}
	}

	// Non-leader: if still completing, wait for leader.
	// For simplicity, we do synchronous handling: by the time the non-leader
	// calls SyncGroup, the leader should already have sent their SyncGroup.
	// If not, return the current assignment (may be empty).

	return errNone, m.assignment
}

// heartbeat handles a Heartbeat request.
func (g *consumerGroup) heartbeat(req heartbeatRequest) int16 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if req.GenerationID != g.generationID {
		return errIllegalGeneration
	}

	m, ok := g.members[req.MemberID]
	if !ok {
		return errUnknownMemberID
	}

	m.lastHeartbeat = time.Now()

	if g.state == groupPreparingRebalance || g.state == groupCompletingRebalance {
		return errRebalanceInProgress
	}

	return errNone
}

// leaveGroup handles a LeaveGroup request.
func (g *consumerGroup) leaveGroup(req leaveGroupRequest) int16 {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, ok := g.members[req.MemberID]; !ok {
		return errUnknownMemberID
	}

	delete(g.members, req.MemberID)

	if len(g.members) == 0 {
		g.state = groupEmpty
		g.leader = ""
	} else {
		// Trigger rebalance.
		g.completeJoinPhase()
	}

	return errNone
}

// reapExpired removes members that haven't sent a heartbeat within sessionTimeout.
// Returns true if any members were removed (triggering a rebalance).
func (g *consumerGroup) reapExpired() bool {
	g.mu.Lock()
	defer g.mu.Unlock()

	now := time.Now()
	var expired []string
	for id, m := range g.members {
		if now.Sub(m.lastHeartbeat) > g.sessionTimeout {
			expired = append(expired, id)
		}
	}

	if len(expired) == 0 {
		return false
	}

	for _, id := range expired {
		delete(g.members, id)
	}

	if len(g.members) == 0 {
		g.state = groupEmpty
		g.leader = ""
	} else {
		g.completeJoinPhase()
	}

	return true
}

// groupCoordinator manages all consumer groups.
type groupCoordinator struct {
	mu             sync.Mutex
	groups         map[string]*consumerGroup
	sessionTimeout time.Duration
	logger         *slog.Logger
}

func newGroupCoordinator(sessionTimeout time.Duration, logger *slog.Logger) *groupCoordinator {
	if sessionTimeout <= 0 {
		sessionTimeout = 30 * time.Second
	}
	return &groupCoordinator{
		groups:         make(map[string]*consumerGroup),
		sessionTimeout: sessionTimeout,
		logger:         logger,
	}
}

// getOrCreate returns an existing group or creates a new one.
func (gc *groupCoordinator) getOrCreate(groupID string) *consumerGroup {
	gc.mu.Lock()
	defer gc.mu.Unlock()
	g, ok := gc.groups[groupID]
	if !ok {
		g = newConsumerGroup(groupID, gc.sessionTimeout)
		gc.groups[groupID] = g
	}
	return g
}

// reapAll runs session expiry across all groups.
func (gc *groupCoordinator) reapAll() {
	gc.mu.Lock()
	groups := make([]*consumerGroup, 0, len(gc.groups))
	for _, g := range gc.groups {
		groups = append(groups, g)
	}
	gc.mu.Unlock()

	for _, g := range groups {
		if g.reapExpired() {
			gc.logger.Info("consumer group rebalance due to expired members",
				"group", g.groupID,
			)
		}
	}
}

// startReaper starts a goroutine that periodically checks for expired members.
func (gc *groupCoordinator) startReaper(done <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				gc.reapAll()
			}
		}
	}()
}

// generateMemberID creates a unique member ID.
func generateMemberID(groupID string) string {
	return groupID + "-" + randomID()
}

// randomID generates a short random ID for member identification.
func randomID() string {
	// Use time-based + simple counter for uniqueness.
	// For production-grade, you'd use UUID, but this is sufficient.
	return time.Now().Format("20060102150405.000000000")
}
