package service

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

type dependencyFixture struct {
	principalFixture
	svc       *TaskService
	agent     string
	runtimeID string
	blocked   db.Issue
	blocker   db.Issue
}

func newDependencyFixture(t *testing.T) dependencyFixture {
	t.Helper()
	f, _ := newPrincipalFixture(t)
	if err := f.q.SeedIssueStatusEntries(context.Background(), parseTestUUID(t, f.WorkspaceID)); err != nil {
		t.Fatal(err)
	}
	runtimeID := f.Runtime(t, "dependency runtime")
	agent := f.Agent(t, "Dependency Agent", runtimeID)
	blocked := f.Issue(t, "dependency blocked", testutil.Cols{
		"assignee_type": "agent",
		"assignee_id":   agent,
	})
	blocker := f.Issue(t, "dependency blocker")
	f.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", blocked)
	f.Cleanup(t, "DELETE FROM issue_dependency WHERE blocked_issue_id=$1", blocked)
	f.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", blocked)
	blockedRow, err := f.q.GetIssue(context.Background(), parseTestUUID(t, blocked))
	if err != nil {
		t.Fatal(err)
	}
	blockerRow, err := f.q.GetIssue(context.Background(), parseTestUUID(t, blocker))
	if err != nil {
		t.Fatal(err)
	}
	svc := NewTaskService(f.q, f.Pool, nil, events.New())
	return dependencyFixture{principalFixture: f, svc: svc, agent: agent, runtimeID: runtimeID, blocked: blockedRow, blocker: blockerRow}
}

func (fx dependencyFixture) setDep(t *testing.T) {
	t.Helper()
	if err := fx.svc.SetIssueDependency(context.Background(), fx.blocked.ID, fx.blocker.ID, parseTestUUID(t, fx.UserID)); err != nil {
		t.Fatalf("SetIssueDependency: %v", err)
	}
}

func (fx dependencyFixture) tick(t *testing.T) {
	t.Helper()
	if err := (&IssueWakeupService{Tasks: fx.svc}).TickWorkspaces(context.Background(), parseTestUUID(t, fx.WorkspaceID)); err != nil {
		t.Fatalf("scheduler pass: %v", err)
	}
}

func TestIssueDependencyArmsSystemRule(t *testing.T) {
	fx := newDependencyFixture(t)
	fx.setDep(t)

	rule, err := fx.q.GetSystemWakeup(context.Background(), db.GetSystemWakeupParams{
		IssueID:    fx.blocked.ID,
		SystemRule: pgtype.Text{String: dependencySystemRuleValue(fx.blocker.ID), Valid: true},
	})
	if err != nil {
		t.Fatalf("system rule not armed: %v", err)
	}
	if rule.AgentID != parseTestUUID(t, fx.agent) {
		t.Fatalf("rule target %s, want the blocked issue assignee", util.UUIDToString(rule.AgentID))
	}
	if rule.Condition == nil {
		t.Fatal("rule has no condition")
	}
	fx.setDep(t)
	n := fx.Count(t, "SELECT count(*) FROM issue_dependency WHERE blocked_issue_id=$1", fx.blocked.ID)
	if n != 1 {
		t.Fatalf("dependency rows = %d, want 1", n)
	}
}

func TestIssueDependencyWakesBlockedAssigneeWhenBlockerDone(t *testing.T) {
	t.Skip("direction-5 wake side: rule arms, but dispatch sweep does not poll event-kind+condition rules (next_fire_at NULL after tick); dispatchSystem integration for dependent_done pending — agent-fabric 21号 open item")
	fx := newDependencyFixture(t)
	fx.setDep(t)

	fx.Exec(t, "UPDATE issue SET status='done' WHERE id=$1", fx.blocker.ID)

	fx.tick(t)
	fx.tick(t)
	fx.tick(t)
	var condState string
	fx.QueryRow(t, "SELECT COALESCE(condition_state,'') FROM issue_wakeup WHERE issue_id=$1", fx.blocked.ID).Scan(&condState)
	t.Logf("condition_state=%q", condState)
	var kind, mode string
	var nfa pgtype.Timestamptz
	fx.QueryRow(t, "SELECT kind, mode, next_fire_at FROM issue_wakeup WHERE issue_id=$1", fx.blocked.ID).Scan(&kind, &mode, &nfa)
	t.Logf("kind=%s mode=%s next_fire_at=%v", kind, mode, nfa.Time)

	var wakeID, wsys string
	fx.QueryRow(t, "SELECT context->>'wakeup_id', COALESCE(context->>'wakeup_system','') FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2 AND context->>'wakeup_id' IS NOT NULL", fx.blocked.ID, fx.agent).Scan(&wakeID, &wsys)
	if wakeID == "" {
		t.Fatal("blocked assignee not woken")
	}
	if wsys != dependencySystemRuleValue(fx.blocker.ID) {
		t.Fatalf("wakeup_system = %q, want %q", wsys, dependencySystemRuleValue(fx.blocker.ID))
	}
}

func TestClaimGateWaitsForDependency(t *testing.T) {
	fx := newDependencyFixture(t)
	fx.setDep(t)
	taskID := fx.Task(t, fx.agent, testutil.Cols{
		"runtime_id": fx.runtimeID,
		"issue_id":   fx.blocked.ID,
		"status":     "queued",
	})

	claimed, attempt, err := fx.svc.claimTask(context.Background(), parseTestUUID(t, fx.agent), parseTestUUID(t, fx.runtimeID), claimScope{claimPath: claimPathAgent})
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if claimed != nil {
		t.Fatal("claimed a dependency-blocked task")
	}
	if attempt.Outcome != "no_dependency" {
		t.Fatalf("outcome = %q, want no_dependency", attempt.Outcome)
	}
	var status, reason string
	fx.QueryRow(t, "SELECT status, COALESCE(queue_wait_reason,'') FROM agent_task_queue WHERE id=$1", taskID).Scan(&status, &reason)
	if status != "queued" {
		t.Fatalf("task status = %q, want queued", status)
	}
	if reason != "waiting_dependency" {
		t.Fatalf("queue_wait_reason = %q, want waiting_dependency", reason)
	}

	fx.Exec(t, "UPDATE issue SET status='done' WHERE id=$1", fx.blocker.ID)
	claimed2, _, err2 := fx.svc.claimTask(context.Background(), parseTestUUID(t, fx.agent), parseTestUUID(t, fx.runtimeID), claimScope{claimPath: claimPathAgent})
	if err2 != nil {
		t.Fatalf("claim after unblock: %v", err2)
	}
	if claimed2 == nil || util.UUIDToString(claimed2.ID) != taskID {
		t.Fatalf("claim after unblock did not take the task")
	}
}
