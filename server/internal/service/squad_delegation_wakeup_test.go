package service

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// The delegation_done platform rule (see issue_wakeup_delegation.go): when an
// agent delegates by @mention on a squad-assigned issue, the member run's
// terminal state must wake the squad's leader again — the briefing's "a
// delegated member finishes and the issue moves forward" promise. These tests
// drive the real enqueue path, the real capture trigger and the real
// scheduler pass; no wakeup internals are stubbed.

// delegationFixture wires one squad-assigned issue whose leader has a finished
// dispatch turn, ready to delegate to the squad's member agent by @mention.
type delegationFixture struct {
	principalFixture
	issue      db.Issue
	squad      string
	leader     string
	member     string
	leaderTask string
}

func newDelegationFixture(t *testing.T) delegationFixture {
	t.Helper()
	f, _ := newPrincipalFixture(t)
	if err := f.q.SeedIssueStatusEntries(context.Background(), parseTestUUID(t, f.WorkspaceID)); err != nil {
		t.Fatal(err)
	}
	leaderRuntime := f.Runtime(t, "delegation leader runtime")
	memberRuntime := f.Runtime(t, "delegation member runtime")
	leader := f.Agent(t, "Delegation Leader", leaderRuntime)
	member := f.Agent(t, "Delegation Member", memberRuntime)
	squad := f.Squad(t, "Delegation squad", leader)
	f.SquadMember(t, squad, "agent", member)
	issue := f.Issue(t, "delegation wakeup", testutil.Cols{
		"assignee_type": "squad",
		"assignee_id":   squad,
	})
	// The leader's dispatch turn: finished, so it is neither a waiting run the
	// firing could join nor a stray queued leader task on the issue.
	leaderTask := f.Task(t, leader, testutil.Cols{
		"runtime_id":          leaderRuntime,
		"issue_id":            issue,
		"status":              "completed",
		"is_leader_task":      true,
		"squad_id":            squad,
		"originator_user_id":  f.UserID,
		"accountable_user_id": f.UserID,
	})
	f.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", issue)
	f.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", issue)
	f.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issue)
	issueRow, err := f.q.GetIssue(context.Background(), parseTestUUID(t, issue))
	if err != nil {
		t.Fatal(err)
	}
	return delegationFixture{principalFixture: f, issue: issueRow, squad: squad, leader: leader, member: member, leaderTask: leaderTask}
}

// delegate posts a delegation comment and runs the real mention enqueue for
// targetAgent, returning the member task it created. An authorTask names the
// run the agent author wrote the comment from.
func (fx delegationFixture) delegate(t *testing.T, authorType, authorID, targetAgent, authorTask string) db.AgentTaskQueue {
	t.Helper()
	cols := testutil.Cols{"author_type": authorType, "author_id": authorID}
	if authorTask != "" {
		cols["source_task_id"] = authorTask
	}
	comment := fx.Comment(t, util.UUIDToString(fx.issue.ID), "[@you](mention://agent/"+targetAgent+") please take this", cols)
	task, err := fx.svc.TaskSvc.EnqueueTaskForMention(context.Background(), fx.issue, parseTestUUID(t, targetAgent), parseTestUUID(t, comment), OriginNamed)
	if err != nil {
		t.Fatalf("EnqueueTaskForMention: %v", err)
	}
	return task
}

func (fx delegationFixture) rule(t *testing.T, memberTaskID pgtype.UUID) db.IssueWakeup {
	t.Helper()
	rule, err := fx.q.GetSystemWakeup(context.Background(), db.GetSystemWakeupParams{
		IssueID:    fx.issue.ID,
		SystemRule: pgtype.Text{String: delegationSystemRuleValue(memberTaskID), Valid: true},
	})
	if err != nil {
		t.Fatalf("load delegation rule: %v", err)
	}
	return rule
}

func (fx delegationFixture) ruleCount(t *testing.T) int {
	t.Helper()
	return fx.Count(t, "SELECT count(*) FROM issue_wakeup WHERE issue_id=$1", fx.issue.ID)
}

func (fx delegationFixture) tick(t *testing.T) {
	t.Helper()
	if err := (&IssueWakeupService{Tasks: fx.svc.TaskSvc}).TickWorkspaces(context.Background(), parseTestUUID(t, fx.WorkspaceID)); err != nil {
		t.Fatalf("scheduler pass: %v", err)
	}
}

func (fx delegationFixture) lastTaskOf(t *testing.T, agentID string) pgtype.UUID {
	t.Helper()
	var id string
	fx.QueryRow(t, "SELECT id FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2 ORDER BY created_at DESC LIMIT 1", fx.issue.ID, agentID).Scan(&id)
	return parseTestUUID(t, id)
}

func TestSquadDelegationWakeupRuleCreatedOnLeaderMention(t *testing.T) {
	fx := newDelegationFixture(t)
	task := fx.delegate(t, "agent", fx.leader, fx.member, fx.leaderTask)

	rule := fx.rule(t, task.ID)
	if rule.SystemRule.String != delegationSystemRuleValue(task.ID) {
		t.Fatalf("system_rule = %q, want %q", rule.SystemRule.String, delegationSystemRuleValue(task.ID))
	}
	if rule.AgentID != parseTestUUID(t, fx.leader) {
		t.Fatalf("rule targets %s, want the squad leader %s", util.UUIDToString(rule.AgentID), fx.leader)
	}
	if rule.CreatedBy != parseTestUUID(t, fx.UserID) {
		t.Fatalf("rule created_by = %s, want the delegation's human %s", util.UUIDToString(rule.CreatedBy), fx.UserID)
	}
	if rule.FilterTaskID != task.ID {
		t.Fatalf("filter_task_id = %s, want the member task %s", util.UUIDToString(rule.FilterTaskID), util.UUIDToString(task.ID))
	}
	if !slices.Equal(rule.EventTypes, delegationDoneEventTypes) {
		t.Fatalf("event_types = %v, want %v", rule.EventTypes, delegationDoneEventTypes)
	}
	if rule.Kind != "event" || rule.Mode != "once" || !rule.Enabled {
		t.Fatalf("rule shape: kind=%s mode=%s enabled=%v, want event/once/enabled", rule.Kind, rule.Mode, rule.Enabled)
	}
	if rule.Instruction != DelegationDoneDefaultInstruction {
		t.Fatalf("instruction = %q, want the default delegation instruction", rule.Instruction)
	}
	if rule.SourceTaskID != parseTestUUID(t, fx.leaderTask) {
		t.Fatalf("source_task_id = %s, want the leader's dispatch run %s", util.UUIDToString(rule.SourceTaskID), fx.leaderTask)
	}
}

// TestSquadDelegationWakeupWakesLeaderWhenMemberRunEnds is the core closed
// loop: the member run reaches a terminal state, the capture trigger records
// the receipt, one scheduler pass enqueues the leader again — as a leader run
// carrying the squad, so the claim path injects the briefing and roster — and
// the once rule is spent without stranding the run it created.
func TestSquadDelegationWakeupWakesLeaderWhenMemberRunEnds(t *testing.T) {
	for _, status := range []string{"completed", "failed", "cancelled"} {
		t.Run(status, func(t *testing.T) {
			fx := newDelegationFixture(t)
			task := fx.delegate(t, "agent", fx.leader, fx.member, fx.leaderTask)
			rule := fx.rule(t, task.ID)

			// The member run ends the way every terminal writer ends it: a
			// status transition, which the capture trigger sees.
			fx.Exec(t, "UPDATE agent_task_queue SET status=$2, started_at=now(), completed_at=now() WHERE id=$1", task.ID, status)
			fx.tick(t)
			fx.tick(t) // a second pass must not queue a second leader run

			var leaderTaskID, wakeupID, squadID, note string
			var isLeader bool
			fx.QueryRow(t, `
				SELECT id, context->>'wakeup_id', squad_id::text, is_leader_task, COALESCE(handoff_note,'')
				FROM agent_task_queue
				WHERE issue_id=$1 AND agent_id=$2 AND context->>'wakeup_id' IS NOT NULL
			`, fx.issue.ID, fx.leader).Scan(&leaderTaskID, &wakeupID, &squadID, &isLeader, &note)
			if wakeupID != util.UUIDToString(rule.ID) {
				t.Fatalf("woken run belongs to wakeup %s, want %s", wakeupID, util.UUIDToString(rule.ID))
			}
			if !isLeader {
				t.Fatal("the woken leader run is not is_leader_task; the claim path would skip the squad briefing")
			}
			if squadID != fx.squad {
				t.Fatalf("woken run squad_id = %s, want %s", squadID, fx.squad)
			}
			if !strings.Contains(note, "delegated to a squad member") {
				t.Fatalf("handoff note lacks the delegation instruction: %q", note)
			}
			if n := fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2", fx.issue.ID, fx.leader); n != 2 {
				t.Fatalf("leader has %d tasks on the issue, want 2 (the finished dispatch turn and the wakeup)", n)
			}

			// The once rule is spent, but the run it created stays claimable:
			// disabled_at stays NULL and the claim check accepts the run.
			spent, err := fx.q.GetSystemWakeup(context.Background(), db.GetSystemWakeupParams{IssueID: fx.issue.ID, SystemRule: rule.SystemRule})
			if err != nil {
				t.Fatal(err)
			}
			if spent.Enabled || !spent.LastTaskID.Valid || util.UUIDToString(spent.LastTaskID) != leaderTaskID || spent.DisabledAt.Valid {
				t.Fatalf("once rule after firing: enabled=%v last_task_id=%s disabled_at=%v", spent.Enabled, util.UUIDToString(spent.LastTaskID), spent.DisabledAt.Valid)
			}
			wakeups := &IssueWakeupService{Tasks: fx.svc.TaskSvc}
			claimed, err := fx.q.GetAgentTask(context.Background(), parseTestUUID(t, leaderTaskID))
			if err != nil {
				t.Fatal(err)
			}
			if err := wakeups.CheckClaim(context.Background(), claimed); err != nil {
				t.Fatalf("the woken run is not claimable: %v", err)
			}
		})
	}
}

func TestSquadDelegationWakeupEnsureIsIdempotent(t *testing.T) {
	fx := newDelegationFixture(t)
	ctx := context.Background()
	task := fx.delegate(t, "agent", fx.leader, fx.member, fx.leaderTask)
	original := fx.rule(t, task.ID)

	squad, err := fx.q.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: parseTestUUID(t, fx.squad), WorkspaceID: fx.issue.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	again, err := fx.svc.TaskSvc.EnsureDelegationWakeupRule(ctx, fx.issue, squad, task.ID, parseTestUUID(t, fx.UserID), parseTestUUID(t, fx.leaderTask))
	if err != nil {
		t.Fatalf("re-ensure: %v", err)
	}
	if again.ID != original.ID {
		t.Fatalf("re-ensure created %s, want the existing %s", util.UUIDToString(again.ID), util.UUIDToString(original.ID))
	}

	// A rule a person registered by hand on the same run keeps its own
	// instruction and stays untouched by the platform's ensure.
	wakeups := &IssueWakeupService{Tasks: fx.svc.TaskSvc}
	manual, err := wakeups.Create(ctx, fx.issue.ID, parseTestUUID(t, fx.UserID), pgtype.UUID{}, WakeupInput{
		AgentID: fx.member, Kind: "event", Mode: "once",
		EventTypes: []string{"task.completed"}, FilterTaskID: util.UUIDToString(task.ID),
		Instruction: "mine",
	})
	if err != nil {
		t.Fatalf("manual rule: %v", err)
	}
	if _, err := fx.svc.TaskSvc.EnsureDelegationWakeupRule(ctx, fx.issue, squad, task.ID, parseTestUUID(t, fx.UserID), parseTestUUID(t, fx.leaderTask)); err != nil {
		t.Fatalf("ensure beside a manual rule: %v", err)
	}
	kept, err := fx.q.GetIssueWakeup(ctx, db.GetIssueWakeupParams{ID: manual.ID, WorkspaceID: fx.issue.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	if kept.Instruction != "mine" || !kept.Enabled || kept.Revision != manual.Revision {
		t.Fatalf("manual rule was changed: instruction=%q enabled=%v revision=%d", kept.Instruction, kept.Enabled, kept.Revision)
	}
	if got := fx.ruleCount(t); got != 2 {
		t.Fatalf("issue has %d wakeup rules, want 2 (one platform, one manual)", got)
	}
}

// TestSquadDelegationWakeupNotArmedOutsideDelegation pins the gates: only an
// agent's delegation of a squad-assigned issue to another agent arms the rule.
func TestSquadDelegationWakeupNotArmedOutsideDelegation(t *testing.T) {
	t.Run("human author", func(t *testing.T) {
		fx := newDelegationFixture(t)
		fx.delegate(t, "member", fx.UserID, fx.member, "")
		if got := fx.ruleCount(t); got != 0 {
			t.Fatalf("human's @mention armed %d rules, want 0", got)
		}
	})
	t.Run("issue not squad-assigned", func(t *testing.T) {
		fx := newDelegationFixture(t)
		fx.Exec(t, "UPDATE issue SET assignee_type='agent', assignee_id=$2 WHERE id=$1", fx.issue.ID, fx.member)
		// The enqueue path sees the issue the way its callers do: loaded after
		// the assignment change.
		updated, err := fx.q.GetIssue(context.Background(), fx.issue.ID)
		if err != nil {
			t.Fatal(err)
		}
		fx.issue = updated
		fx.delegate(t, "agent", fx.leader, fx.member, fx.leaderTask)
		if got := fx.ruleCount(t); got != 0 {
			t.Fatalf("@mention on a non-squad issue armed %d rules, want 0", got)
		}
	})
	t.Run("self mention", func(t *testing.T) {
		fx := newDelegationFixture(t)
		fx.delegate(t, "agent", fx.leader, fx.leader, fx.leaderTask)
		if got := fx.ruleCount(t); got != 0 {
			t.Fatalf("an agent's self-mention armed %d rules, want 0", got)
		}
	})
	t.Run("leader target", func(t *testing.T) {
		fx := newDelegationFixture(t)
		comment := fx.Comment(t, util.UUIDToString(fx.issue.ID), "[@squad](mention://squad/"+fx.squad+")", testutil.Cols{
			"author_type": "agent", "author_id": fx.leader, "source_task_id": fx.leaderTask,
		})
		if _, err := fx.svc.TaskSvc.EnqueueTaskForSquadLeader(context.Background(), fx.issue, parseTestUUID(t, fx.leader), parseTestUUID(t, fx.squad), parseTestUUID(t, comment), OriginNamed); err != nil {
			t.Fatalf("EnqueueTaskForSquadLeader: %v", err)
		}
		if got := fx.ruleCount(t); got != 0 {
			t.Fatalf("a leader-run enqueue armed %d rules, want 0", got)
		}
	})
	t.Run("worker author still wakes the leader", func(t *testing.T) {
		fx := newDelegationFixture(t)
		workerRuntime := fx.Runtime(t, "delegation worker runtime")
		worker := fx.Agent(t, "Delegation Worker", workerRuntime)
		workerTask := fx.Task(t, worker, testutil.Cols{
			"runtime_id": workerRuntime, "issue_id": util.UUIDToString(fx.issue.ID), "status": "completed",
		})
		task := fx.delegate(t, "agent", worker, fx.member, workerTask)
		if got := fx.ruleCount(t); got != 1 {
			t.Fatalf("a worker's hand-off armed %d rules, want 1 (the leader coordinates)", got)
		}
		if rule := fx.rule(t, task.ID); rule.AgentID != parseTestUUID(t, fx.leader) {
			t.Fatalf("worker's hand-off targets %s, want the leader", util.UUIDToString(rule.AgentID))
		}
	})
}

// TestSquadDelegationWakeupSurvivesTerminalRace pins the registration snapshot:
// a member run that somehow ended before its rule existed still wakes the
// leader, because the ensure records the run's own outcome as the first input.
func TestSquadDelegationWakeupSurvivesTerminalRace(t *testing.T) {
	fx := newDelegationFixture(t)
	ctx := context.Background()
	// A member task with no rule yet (the enqueue hook failed or has not run).
	runtime := fx.Runtime(t, "delegation raced runtime")
	raced := fx.Task(t, fx.member, testutil.Cols{
		"runtime_id": runtime, "issue_id": util.UUIDToString(fx.issue.ID), "status": "completed",
		"completed_at": testutil.Raw("now()"),
	})
	squad, err := fx.q.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: parseTestUUID(t, fx.squad), WorkspaceID: fx.issue.WorkspaceID})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fx.svc.TaskSvc.EnsureDelegationWakeupRule(ctx, fx.issue, squad, parseTestUUID(t, raced), parseTestUUID(t, fx.UserID), parseTestUUID(t, fx.leaderTask)); err != nil {
		t.Fatalf("ensure on an ended run: %v", err)
	}
	rule := fx.rule(t, parseTestUUID(t, raced))
	if rule.Enabled {
		t.Fatal("the once rule armed on an ended run should already be spent")
	}
	fx.tick(t)
	if n := fx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id=$1 AND agent_id=$2 AND context->>'wakeup_id' IS NOT NULL", fx.issue.ID, fx.leader); n != 1 {
		t.Fatalf("leader woken %d times for an already-ended run, want 1", n)
	}
}
