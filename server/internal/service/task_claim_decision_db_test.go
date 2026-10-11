package service

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/multica-ai/multica/server/internal/events"
	obsmetrics "github.com/multica-ai/multica/server/internal/metrics"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// DB-backed coverage for the claim decision funnel. These tests follow the
// package's sharedTestPool convention: without a reachable Postgres they skip,
// so `go test ./...` stays usable without a database. Every case builds its
// own fixture; testutil.Insert registers reverse-order cleanup so cases never
// share rows.

// claimDecisionFixture is one workspace / runtime / agent / issue plus a
// TaskService wired to real metrics, so each case can assert exactly which
// (source, outcome, reason) series the claim path incremented.
type claimDecisionFixture struct {
	svc       *TaskService
	metrics   *obsmetrics.BusinessMetrics
	fx        *testutil.Fixture
	pool      *pgxpool.Pool
	agentID   pgtype.UUID
	runtimeID pgtype.UUID
	issueID   string
}

func newClaimDecisionFixture(t *testing.T, maxConcurrent int) *claimDecisionFixture {
	t.Helper()

	pool := sharedTestPool(t)
	bootstrap := testutil.New(pool, "", "")
	suffix := time.Now().UnixNano()
	ownerID := bootstrap.User(t,
		fmt.Sprintf("claim-decision-owner-%d", suffix),
		fmt.Sprintf("claim-decision-owner-%d@example.com", suffix),
	)
	workspaceID := bootstrap.Workspace(t,
		fmt.Sprintf("claim-decision-%d", suffix),
		fmt.Sprintf("claim-decision-%d", suffix),
	)
	fx := testutil.New(pool, workspaceID, ownerID)
	fx.Member(t, workspaceID, ownerID, "owner")
	runtimeID := fx.Runtime(t, "claim-decision-runtime", testutil.Cols{
		"visibility": "public",
		"owner_id":   ownerID,
	})
	agentID := fx.Agent(t, "claim-decision-agent", runtimeID, testutil.Cols{
		"owner_id":             ownerID,
		"max_concurrent_tasks": maxConcurrent,
	})
	issueID := fx.Issue(t, "claim decision issue")

	m := obsmetrics.NewBusinessMetrics()
	svc := NewTaskService(db.New(pool), pool, nil, events.New())
	svc.Metrics = m

	return &claimDecisionFixture{
		svc:       svc,
		metrics:   m,
		fx:        fx,
		pool:      pool,
		agentID:   util.MustParseUUID(agentID),
		runtimeID: util.MustParseUUID(runtimeID),
		issueID:   issueID,
	}
}

// insertTask queues a task for the fixture agent on the fixture runtime.
func (f *claimDecisionFixture) insertTask(t *testing.T, status string) string {
	t.Helper()

	cols := testutil.Cols{
		"runtime_id": util.UUIDToString(f.runtimeID),
		"issue_id":   f.issueID,
		"status":     status,
	}
	if status == "dispatched" {
		cols["dispatched_at"] = testutil.Raw("now() - interval '10 minutes'")
		cols["prepare_lease_expires_at"] = testutil.Raw("now() - interval '1 minute'")
	}
	return f.fx.Task(t, util.UUIDToString(f.agentID), cols)
}

// TestClaimDecisionDBClaimedRuntimePoll covers the happy path: a queued task,
// an agent with capacity, and a healthy runtime produce exactly one
// (runtime, claimed, claimed) increment from claimTask's defer.
func TestClaimDecisionDBClaimedRuntimePoll(t *testing.T) {
	f := newClaimDecisionFixture(t, 5)
	f.insertTask(t, "queued")

	claimed, err := f.svc.ClaimTaskForRuntime(context.Background(), f.runtimeID)
	if err != nil {
		t.Fatalf("ClaimTaskForRuntime: %v", err)
	}
	if claimed == nil {
		t.Fatal("ClaimTaskForRuntime returned nil, want the queued task")
	}
	if got := claimDecisionCount(t, f.metrics, claimSourceRuntime, claimOutcomeClaimed, claimReasonClaimed); got != 1 {
		t.Fatalf("claim_decision_total{runtime,claimed,claimed} = %v, want 1", got)
	}
}

// TestClaimDecisionDBNoCapacity covers the concurrency cap: max_concurrent_tasks=0
// leaves the task visible to the candidate list, so the claim attempt runs and
// is rejected by the capacity check rather than by the queue being empty.
func TestClaimDecisionDBNoCapacity(t *testing.T) {
	f := newClaimDecisionFixture(t, 0)
	f.insertTask(t, "queued")

	claimed, err := f.svc.ClaimTaskForRuntime(context.Background(), f.runtimeID)
	if err != nil {
		t.Fatalf("ClaimTaskForRuntime: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimTaskForRuntime claimed %s, want nil", util.UUIDToString(claimed.ID))
	}
	if got := claimDecisionCount(t, f.metrics, claimSourceRuntime, claimOutcomeNoCapacity, claimReasonAgentConcurrencyCap); got != 1 {
		t.Fatalf("claim_decision_total{runtime,no_capacity,agent_concurrency_cap} = %v, want 1", got)
	}
}

// TestClaimDecisionDBCandidatesBlocked is the core #8551 scenario: the queued
// task stays visible to the candidate list, yet the claim still returns
// nothing, so the wait belongs to a fence rather than to an empty queue.
//
// The fence exercised here is runtime liveness: the candidate query's
// authorization fence checks binding and visibility but not liveness, while
// ClaimAgentTask additionally requires r.status='online' with a fresh
// heartbeat. Marking the runtime offline after enqueue therefore leaves the
// candidate visible while the claim is rejected — outcome=no_tasks must be
// attributed to candidates_blocked. The same collapse hides the other SQL
// fences (wakeup gate, per-(issue, agent) serialization, rebind), which is
// exactly the attribution gap this counter exists to close.
func TestClaimDecisionDBCandidatesBlocked(t *testing.T) {
	f := newClaimDecisionFixture(t, 5)
	f.insertTask(t, "queued")

	if _, err := f.pool.Exec(context.Background(),
		`UPDATE agent_runtime SET status = 'offline' WHERE id = $1`, f.runtimeID,
	); err != nil {
		t.Fatalf("take runtime offline: %v", err)
	}

	claimed, err := f.svc.ClaimTaskForRuntime(context.Background(), f.runtimeID)
	if err != nil {
		t.Fatalf("ClaimTaskForRuntime: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimTaskForRuntime claimed %s, want nil", util.UUIDToString(claimed.ID))
	}
	if got := claimDecisionCount(t, f.metrics, claimSourceRuntime, claimOutcomeNoTasks, claimReasonCandidatesBlocked); got != 1 {
		t.Fatalf("claim_decision_total{runtime,no_tasks,candidates_blocked} = %v, want 1", got)
	}
	if got := claimDecisionCount(t, f.metrics, claimSourceRuntime, claimOutcomeNoTasks, claimReasonNoCandidates); got != 0 {
		t.Fatalf("claim_decision_total{runtime,no_tasks,no_candidates} = %v, want 0", got)
	}
}

// TestClaimDecisionDBEmptyQueue covers the idle poll: no tasks at all produce
// outcome=empty_db (reason empty_queue) from the poll-level exit that never
// enters claimTask.
func TestClaimDecisionDBEmptyQueue(t *testing.T) {
	f := newClaimDecisionFixture(t, 5)

	claimed, err := f.svc.ClaimTaskForRuntime(context.Background(), f.runtimeID)
	if err != nil {
		t.Fatalf("ClaimTaskForRuntime: %v", err)
	}
	if claimed != nil {
		t.Fatalf("ClaimTaskForRuntime claimed %s, want nil", util.UUIDToString(claimed.ID))
	}
	if got := claimDecisionCount(t, f.metrics, claimSourceRuntime, claimOutcomeEmptyDB, claimReasonEmptyQueue); got != 1 {
		t.Fatalf("claim_decision_total{runtime,empty_db,empty_queue} = %v, want 1", got)
	}
}
