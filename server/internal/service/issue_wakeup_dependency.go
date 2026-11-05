// Direction 5 (local research branch): the blocked-by relation's wake side.
//
// A dependency row arms a platform-owned other_issue system rule named
// dependent_done:<blocker>: when the blocker reaches the done category, the
// blocked issue's assignee is woken with an instruction to proceed. Agents
// cannot edit or delete the rule (system_rule rows are platform-owned).
package service

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// DependencyDoneDefaultInstruction tells the woken assignee what unblocked.
const DependencyDoneDefaultInstruction = "An issue this one was blocked on is now done; the trigger facts name it. Proceed with this issue's work."

func dependencySystemRuleValue(blockerID pgtype.UUID) string {
	return "dependent_done:" + util.UUIDToString(blockerID)
}

// SetIssueDependency records one blocked-by relation and arms its system
// rule in one transaction. Idempotent on both the row and the rule.
func (s *TaskService) SetIssueDependency(ctx context.Context, blocked, blocker, createdBy pgtype.UUID) error {
	blockedIssue, err := s.Queries.GetIssue(ctx, blocked)
	if err != nil {
		return fmt.Errorf("load blocked issue: %w", err)
	}
	blockerIssue, err := s.Queries.GetIssue(ctx, blocker)
	if err != nil {
		return fmt.Errorf("load blocker issue: %w", err)
	}
	if blockerIssue.WorkspaceID != blockedIssue.WorkspaceID {
		return fmt.Errorf("cross-workspace dependency rejected")
	}
	// Wake target: the blocked issue's agent assignee (v1: agent-assigned only).
	if blockedIssue.AssigneeType.String != "agent" || !blockedIssue.AssigneeID.Valid {
		return fmt.Errorf("dependency wake needs an agent-assigned blocked issue")
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "INSERT INTO issue_dependency (id, workspace_id, blocked_issue_id, blocker_issue_id, created_by) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (blocked_issue_id, blocker_issue_id) DO NOTHING",
		dbid.NewV7(), blockedIssue.WorkspaceID, blocked, blocker, createdBy); err != nil {
		return fmt.Errorf("insert dependency: %w", err)
	}
	cond, err := json.Marshal(map[string]any{"type": "other_issue", "issue_id": util.UUIDToString(blocker), "state": "done"})
	if err != nil {
		return err
	}
	ruleValue := pgtype.Text{String: dependencySystemRuleValue(blocker), Valid: true}
	if _, err := tx.Exec(ctx, "INSERT INTO issue_wakeup (id, workspace_id, issue_id, agent_id, created_by, instruction, kind, mode, event_types, enabled, system_rule, condition) VALUES ($1,$2,$3,$4,$5,$6,'event','continuous',$7,true,$8,$9) ON CONFLICT DO NOTHING",
		dbid.NewV7(), blockedIssue.WorkspaceID, blocked, blockedIssue.AssigneeID, createdBy, DependencyDoneDefaultInstruction,
		[]string{"issue.status_changed"}, ruleValue, cond); err != nil {
		return fmt.Errorf("insert dependency rule: %w", err)
	}
	return tx.Commit(ctx)
}

// DependencyRuleOf loads the armed rule for tests and diagnostics.
func (s *TaskService) DependencyRuleOf(ctx context.Context, blocked, blocker pgtype.UUID) (db.IssueWakeup, error) {
	issue, err := s.Queries.GetIssue(ctx, blocked)
	if err != nil {
		return db.IssueWakeup{}, err
	}
	return s.Queries.GetSystemWakeup(ctx, db.GetSystemWakeupParams{IssueID: issue.ID, SystemRule: pgtype.Text{String: dependencySystemRuleValue(blocker), Valid: true}})
}

// unmetDependencyCount reports how many blockers of the issue are still
// outside the done/closed categories. Zero means the issue may run.
func (s *TaskService) unmetDependencyCount(ctx context.Context, issueID pgtype.UUID) (int64, error) {
	var n int64
	var err error
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM issue_dependency d JOIN issue ib ON ib.id = d.blocker_issue_id LEFT JOIN issue_status st ON st.workspace_id = ib.workspace_id AND st.key = ib.status WHERE d.blocked_issue_id = $1 AND COALESCE(st.category, '') NOT IN ('done','closed')", issueID).Scan(&n); err != nil {
		return 0, err
	}
	return n, tx.Commit(ctx)
}

// unclaimForDependency returns a claimed row to queued with the wait reason.
func (s *TaskService) unclaimForDependency(ctx context.Context, taskID pgtype.UUID) error {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, "UPDATE agent_task_queue SET status='queued', queue_wait_reason='waiting_dependency', prepare_lease_expires_at=NULL WHERE id=$1", taskID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
