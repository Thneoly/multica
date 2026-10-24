package service

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/eventcontract"
)

// pgerrLockNotAvailable is SQLSTATE 55P03: a row lock hit NOWAIT or the
// statement's lock_timeout.
const pgerrLockNotAvailable = "55P03"

// SystemRuleDelegationDone names the platform rule that closes the
// leader→member→leader loop of @mention delegation on a squad-assigned issue:
// when the run a delegating comment created for a member agent reaches a
// terminal state, the squad's leader is woken once to route the next step. The
// squad briefing has always promised this ("a delegated member finishes and
// the issue moves forward"); before this rule nothing backed it, so a member
// that finished without commenting left the chain stalled.
//
// One rule exists per delegated run. The member task is part of the stored
// system_rule value ('delegation_done:<task uuid>'), so the (issue_id,
// system_rule) unique index doubles as the rule's fingerprint and concurrent
// ensures keep the first row. Unlike child_done, which targets whatever the
// issue's assignee is when it fires, this rule names its target when created:
// agent_id is the squad's leader and created_by the human the delegated run
// runs as, so the waking run continues that delegation's authority. Being a
// system rule, neither agents nor members can edit or delete it.
const SystemRuleDelegationDone = "delegation_done"

// DelegationDoneDefaultInstruction is what the woken leader is asked to do.
// The trigger facts say which delegated run ended and how.
const DelegationDoneDefaultInstruction = "A run you delegated to a squad member on this issue has reached a terminal state; the trigger facts name the run and its outcome. " +
	"Read the member's result on the issue, then decide the next step: delegate what follows, ask for a correction when the run failed, or bring the results together and move the issue forward. " +
	"When nothing remains, record your evaluation with `multica squad activity` as usual. " +
	"A member may finish without commenting; the run's outcome and the issue's state are enough to route from."

// delegationDoneEventTypes are the terminal states of a delegated member run.
var delegationDoneEventTypes = []string{
	eventcontract.EventTaskCompleted, eventcontract.EventTaskFailed, eventcontract.EventTaskCancelled,
}

func delegationSystemRuleValue(memberTaskID pgtype.UUID) string {
	return SystemRuleDelegationDone + ":" + util.UUIDToString(memberTaskID)
}

// isDelegationSystemRule reports whether a system_rule value names a
// delegation_done rule, as stored and as the dispatch paths read it back.
func isDelegationSystemRule(systemRule string) bool {
	return strings.HasPrefix(systemRule, SystemRuleDelegationDone+":")
}

// EnsureDelegationWakeupRule creates the delegation_done rule for one
// delegated member run, or returns the existing one. The rule rides the
// issue's existing event capture: the member run's terminal status write
// records the receipt, and the ordinary dispatcher wakes the leader with
// leader-run semantics. If the run reached its terminal state before the rule
// existed, that outcome is recorded as the rule's first input so the leader is
// still woken. A squad that cannot coordinate — archived, or led by an agent
// that is gone, archived or runtime-less — gets no rule: there is nothing to
// wake, and the zero wakeup is returned whole.
func (s *TaskService) EnsureDelegationWakeupRule(ctx context.Context, issue db.Issue, squad db.Squad, memberTaskID, createdBy, leaderTaskID pgtype.UUID) (db.IssueWakeup, error) {
	if squad.ArchivedAt.Valid {
		return db.IssueWakeup{}, nil
	}
	leader, err := s.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: squad.LeaderID, WorkspaceID: issue.WorkspaceID})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.IssueWakeup{}, nil
	}
	if err != nil {
		return db.IssueWakeup{}, err
	}
	if leader.ArchivedAt.Valid || !leader.RuntimeID.Valid {
		return db.IssueWakeup{}, nil
	}
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return db.IssueWakeup{}, err
	}
	defer tx.Rollback(ctx)
	q := s.Queries.WithTx(tx)
	ruleValue := pgtype.Text{String: delegationSystemRuleValue(memberTaskID), Valid: true}
	rule, err := q.CreateDelegationWakeup(ctx, db.CreateDelegationWakeupParams{
		ID: dbid.NewV7(), WorkspaceID: issue.WorkspaceID, IssueID: issue.ID,
		AgentID: leader.ID, CreatedBy: createdBy, SourceTaskID: leaderTaskID,
		Instruction: DelegationDoneDefaultInstruction,
		EventTypes:  append([]string{}, delegationDoneEventTypes...),
		FilterTaskID: memberTaskID, SystemRule: ruleValue,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		// A concurrent ensure won; its rule already covers this run.
		rule, err = q.GetSystemWakeup(ctx, db.GetSystemWakeupParams{IssueID: issue.ID, SystemRule: ruleValue})
		if err != nil {
			return db.IssueWakeup{}, err
		}
		return rule, tx.Commit(ctx)
	}
	if err != nil {
		return db.IssueWakeup{}, err
	}
	// Subscribe and inspect the member run under the same transaction, so a
	// run that ended between the enqueue and this rule still wakes the leader.
	// The read takes the run's row lock (bounded), so a terminal write that is
	// committing now is visible once the lock is granted — the same race the
	// save path closes with LockWakeupSourceTask. Best effort: a lock that
	// times out skips the snapshot and leaves the rule armed.
	if _, err = tx.Exec(ctx, "SET LOCAL lock_timeout = '250ms'"); err != nil {
		return db.IssueWakeup{}, err
	}
	member, err := q.LockWakeupSourceTaskWait(ctx, db.LockWakeupSourceTaskWaitParams{ID: memberTaskID, IssueID: issue.ID})
	if err != nil {
		var lockBusy *pgconn.PgError
		if !errors.Is(err, pgx.ErrNoRows) && !(errors.As(err, &lockBusy) && lockBusy.Code == pgerrLockNotAvailable) {
			return db.IssueWakeup{}, err
		}
		member = db.AgentTaskQueue{}
	}
	if member.ID.Valid &&
		slices.Contains([]string{"completed", "failed", "cancelled"}, member.Status) {
		var now time.Time
		if err := tx.QueryRow(ctx, "SELECT now()").Scan(&now); err != nil {
			return db.IssueWakeup{}, err
		}
		occurred := member.CompletedAt
		if !occurred.Valid {
			occurred = pgtype.Timestamptz{Time: now, Valid: true}
		}
		payload, _ := json.Marshal(map[string]any{
			"event_id": util.UUIDToString(member.ID) + ":" + member.Status, "event_type": eventcontract.TaskEvent(member.Status), "version": 1,
			"occurred_at": occurred, "observed_at": now, "registration_snapshot": true,
			"workspace_id": util.UUIDToString(issue.WorkspaceID), "issue_id": util.UUIDToString(issue.ID),
			"task_id": util.UUIDToString(member.ID), "source_task_id": util.UUIDToString(member.ID), "status": member.Status,
			"agent_id": member.AgentID, "actor_type": "agent", "actor_id": member.AgentID,
			"retry_of_task_id": member.RetryOfTaskID, "rerun_of_task_id": member.RerunOfTaskID,
		})
		if _, err = q.RecordWakeupReceipt(ctx, db.RecordWakeupReceiptParams{
			ID: dbid.NewV7(), WakeupID: rule.ID, Revision: rule.Revision,
			EventKey: util.UUIDToString(member.ID) + ":" + member.Status,
			EventType: eventcontract.TaskEvent(member.Status), Payload: payload,
		}); err != nil {
			return db.IssueWakeup{}, err
		}
		if err = q.AdvanceIssueWakeup(ctx, db.AdvanceIssueWakeupParams{ID: rule.ID, Enabled: false}); err != nil {
			return db.IssueWakeup{}, err
		}
		rule.Enabled = false
	}
	activity, err := recordWakeupActivity(ctx, q, rule, wakeupActivityCreated, "system", pgtype.UUID{}, map[string]any{
		"rule": SystemRuleDelegationDone, "task_id": util.UUIDToString(memberTaskID), "agent_id": util.UUIDToString(leader.ID),
	})
	if err != nil {
		return db.IssueWakeup{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return db.IssueWakeup{}, err
	}
	(&IssueWakeupService{Tasks: s}).publishWakeupActivities(activity)
	return rule, nil
}

// ensureDelegationWakeupRule arms the delegation_done rule after an enqueue
// created a member run. It is called only from the explicit @mention path
// (EnqueueTaskForMention) and the retry of a delegated member run: the routed
// fallbacks — a threaded reply, a conversation continuation, a squad-leader
// hop — create a run for work the target already saw, not a delegation whose
// end the leader must be woken about. The @mention delegation shape: the
// issue is assigned to a squad, the delegating comment came from an agent,
// and the run just created targets another agent — not the delegating author
// (a self-mention), and not the squad's leader itself, whose runs are leader
// turns rather than delegated work. Which agent delegated does not matter,
// and the target need not be a squad member — a deliberate choice: any
// agent-to-agent hand-off on a squad-coordinated issue routes through the
// leader, who is the coordinator the rule wakes either way. Failures are
// logged and swallowed — the rule is a coordination aid and must never block
// the enqueue it serves.
func (s *TaskService) ensureDelegationWakeupRule(ctx context.Context, issue db.Issue, memberAgentID, memberTaskID, originator, triggerCommentID pgtype.UUID) {
	if !triggerCommentID.Valid {
		return
	}
	if issue.AssigneeType.String != "squad" || !issue.AssigneeID.Valid {
		return
	}
	comment, err := s.Queries.GetCommentInWorkspace(ctx, db.GetCommentInWorkspaceParams{ID: triggerCommentID, WorkspaceID: issue.WorkspaceID})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("delegation wakeup rule not armed: delegating comment unreadable", "issue_id", util.UUIDToString(issue.ID), "comment_id", util.UUIDToString(triggerCommentID), "error", err)
		}
		return
	}
	if comment.AuthorType != "agent" || !comment.AuthorID.Valid || comment.AuthorID == memberAgentID {
		return
	}
	squad, err := s.Queries.GetSquadInWorkspace(ctx, db.GetSquadInWorkspaceParams{ID: issue.AssigneeID, WorkspaceID: issue.WorkspaceID})
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			slog.Warn("delegation wakeup rule not armed: squad unreadable", "issue_id", util.UUIDToString(issue.ID), "error", err)
		}
		return
	}
	if squad.ArchivedAt.Valid || squad.LeaderID == memberAgentID {
		return
	}
	if _, err := s.EnsureDelegationWakeupRule(ctx, issue, squad, memberTaskID, originator, comment.SourceTaskID); err != nil {
		slog.Warn("delegation wakeup rule not armed", "issue_id", util.UUIDToString(issue.ID), "task_id", util.UUIDToString(memberTaskID), "error", err)
	}
}
