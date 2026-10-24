package handler

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
)

// The delegation_done platform rule must not surface as a subscription row:
// GET /api/issues/{id}/wakeups lists what people and agents registered, and a
// platform rule can be neither edited nor disabled through that surface. A
// child_done rule never appeared only because its agent_id is NULL and the
// INNER JOIN dropped it; a delegation rule names the leader and would render
// as an ordinary subscription the user cannot act on.
func TestListIssueWakeupsHidesDelegationRules(t *testing.T) {
	ctx := context.Background()
	leaderRuntime := dbfx.Runtime(t, "delegation list leader runtime")
	memberRuntime := dbfx.Runtime(t, "delegation list member runtime")
	leader := dbfx.Agent(t, "Delegation list leader", leaderRuntime)
	member := dbfx.Agent(t, "Delegation list member", memberRuntime)
	squad := dbfx.Squad(t, "Delegation list squad", leader)
	dbfx.SquadMember(t, squad, "agent", member)
	issue := dbfx.Issue(t, "delegation wakeup list", testutil.Cols{
		"assignee_type": "squad",
		"assignee_id":   squad,
	})
	dbfx.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", issue)
	dbfx.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", issue)
	dbfx.Cleanup(t, "DELETE FROM agent_task_queue WHERE issue_id=$1", issue)
	leaderTask := dbfx.Task(t, leader, testutil.Cols{
		"runtime_id": leaderRuntime, "issue_id": issue, "status": "completed",
		"is_leader_task": true, "squad_id": squad,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	comment := dbfx.Comment(t, issue, "[@you](mention://agent/"+member+") please take this", testutil.Cols{
		"author_type": "agent", "author_id": leader, "source_task_id": leaderTask,
	})
	issueRow, err := testHandler.Queries.GetIssue(ctx, parseUUID(issue))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := testHandler.TaskService.EnqueueTaskForMention(ctx, issueRow, parseUUID(member), parseUUID(comment), service.OriginNamed); err != nil {
		t.Fatalf("EnqueueTaskForMention: %v", err)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM issue_wakeup WHERE issue_id=$1", issue); n != 1 {
		t.Fatalf("the delegation rule was not armed (%d rows)", n)
	}

	list := func() []map[string]any {
		req := withURLParam(newRequest("GET", "/api/issues/"+issue+"/wakeups", nil), "id", issue)
		var rows []map[string]any
		testutil.Call(t, testHandler.ListIssueWakeups, req).Want(200).JSON(&rows)
		return rows
	}
	if rows := list(); len(rows) != 0 {
		t.Fatalf("platform rule leaked into the subscription list: %v", rows)
	}

	// A rule a person registered stays visible, so the empty list above is
	// the platform filter and not a broken query.
	svc := service.IssueWakeupService{Tasks: testHandler.TaskService}
	if _, err := svc.Create(ctx, parseUUID(issue), parseUUID(testUserID), pgtype.UUID{}, service.WakeupInput{
		AgentID: leader, Kind: "event", Mode: "once",
		EventTypes: []string{"comment.created"}, Instruction: "watch the thread",
	}); err != nil {
		t.Fatal(err)
	}
	rows := list()
	if len(rows) != 1 || rows[0]["instruction"] != "watch the thread" {
		t.Fatalf("manual subscription missing from the list: %v", rows)
	}
}
