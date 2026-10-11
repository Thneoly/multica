package service

import (
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
)

// Claim decision observability (#8551, queue-wait attribution).
//
// Every daemon poll decides whether a task is dispatched, skipped, or left
// waiting, and until now most of those decisions were invisible: claimTask's
// internal outcome string only surfaced through the >=300ms slow log, two
// outcomes had scattered debug lines, and no_runtime / runtime_mismatch had
// nothing at all. The worst blind spot is outcome=no_tasks — it collapses the
// ClaimAgentTask SQL fences (wakeup gate, runtime liveness, per-(issue, agent)
// serialization, rebind fence) into one value even though the caller's
// candidate list knew whether work was visible at all.
//
// recordClaimDecision below is the single funnel for this: one call per claim
// attempt (from claimTask's defer) and one per claim-poll exit that never
// reaches claimTask. It emits a bounded-cardinality counter
// (source x outcome x reason) and a structured debug log. Nothing here changes
// any decision — the outcomes mirror strings the claim paths already produce,
// and the mapping is pure.

// Outcomes produced inside claimTask, shared by every caller.
const (
	claimOutcomeClaimed                = "claimed"
	claimOutcomeNoTasks                = "no_tasks"
	claimOutcomeNoCapacity             = "no_capacity"
	claimOutcomeNoRuntime              = "no_runtime"
	claimOutcomeRuntimeMismatch        = "runtime_mismatch"
	claimOutcomeErrorGetAgent          = "error_get_agent"
	claimOutcomeErrorCountRunning      = "error_count_running"
	claimOutcomeErrorClaim             = "error_claim"
	claimOutcomeErrorReanchorChatInput = "error_reanchor_chat_input"
	claimOutcomeErrorTransaction       = "error_transaction"
)

// Outcomes of the single-runtime claim poll (ClaimTaskForRuntime) that return
// before claimTask is ever entered. claimOutcomeNoTask is that function's
// initial slow-log outcome; no funnel call records it.
const (
	claimOutcomeNoTask                 = "no_task"
	claimOutcomeEmptyCacheHit          = "empty_cache_hit"
	claimOutcomeEmptyDB                = "empty_db"
	claimOutcomeReclaimedDispatched    = "reclaimed_dispatched"
	claimOutcomeErrorPromoteDeferred   = "error_promote_deferred"
	claimOutcomeErrorReclaimDispatched = "error_reclaim_dispatched"
	claimOutcomeErrorList              = "error_list"
)

// Reasons narrow an outcome into the cause a dashboard or debug-log reader
// cares about. The candidates_blocked vs no_candidates split is the #8551
// signal: the runtime saw queued work, yet the SQL claim still returned
// nothing — so the wait belongs to a fence, not to an empty queue.
const (
	claimReasonClaimed                = "claimed"
	claimReasonNoCandidates           = "no_candidates"
	claimReasonCandidatesBlocked      = "candidates_blocked"
	claimReasonUnknownCandidates      = "unknown_candidates"
	claimReasonAgentConcurrencyCap    = "agent_concurrency_cap"
	claimReasonAgentUnbound           = "agent_unbound"
	claimReasonAgentRebound           = "agent_rebound"
	claimReasonEmptyCache             = "empty_cache"
	claimReasonEmptyQueue             = "empty_queue"
	claimReasonStaleDispatchRecovered = "stale_dispatch_recovered"
	claimReasonError                  = "error"
)

// Claim poll paths, as seen by the counter's source label.
const (
	claimSourceAgent   = "agent"   // ClaimTask (agent-triggered claim)
	claimSourceRuntime = "runtime" // ClaimTaskForRuntime (single-runtime poll)
	claimSourceBatch   = "batch"   // ClaimTasksForRuntimes (machine-level batch)
)

// claimScope is informational context the caller already holds. candidates is
// how many rows in the caller's candidate list belong to the agent being
// claimed; the agent path has no list, so it reports zero and its no_tasks
// outcome maps to unknown_candidates rather than a definitive no_candidates.
type claimScope struct {
	source     string
	candidates int
}

// claimDecisionDetail carries the capacity numbers behind a no_capacity
// decision. Left zero for every other outcome.
type claimDecisionDetail struct {
	running       int64
	maxConcurrent int64
}

// claimReasonFor maps an outcome onto the closed reason enum above. Unknown
// inputs degrade to "error" rather than minting a new series: a value nobody
// classified is not evidence of a healthy path.
func claimReasonFor(outcome string, scope claimScope) string {
	switch outcome {
	case claimOutcomeNoTasks:
		if scope.candidates > 0 {
			return claimReasonCandidatesBlocked
		}
		if scope.source == claimSourceAgent {
			return claimReasonUnknownCandidates
		}
		return claimReasonNoCandidates
	case claimOutcomeNoCapacity:
		return claimReasonAgentConcurrencyCap
	case claimOutcomeRuntimeMismatch:
		return claimReasonAgentRebound
	case claimOutcomeNoRuntime:
		return claimReasonAgentUnbound
	case claimOutcomeClaimed:
		return claimReasonClaimed
	case claimOutcomeEmptyCacheHit:
		return claimReasonEmptyCache
	case claimOutcomeEmptyDB:
		return claimReasonEmptyQueue
	case claimOutcomeReclaimedDispatched:
		return claimReasonStaleDispatchRecovered
	default:
		// Every error_* outcome, and anything unrecognized.
		return claimReasonError
	}
}

// normalizeClaimSource keeps the counter's source label a closed enum: values
// outside the three known poll paths land on "other".
func normalizeClaimSource(source string) string {
	switch source {
	case claimSourceAgent, claimSourceRuntime, claimSourceBatch:
		return source
	default:
		return "other"
	}
}

// recordClaimDecision is the single observability funnel for the claim path:
// metric first, then log, so a logging problem can never lose the counter.
// runtimeID may be the zero UUID (the agent path passes no runtime).
//
// The empty-poll outcomes (empty_cache_hit, empty_db) fire on every idle
// runtime poll — the hottest path in the scheduler — so they are counted but
// deliberately not logged; a debug line per idle poll would flood any
// deployment running with debug logging enabled.
func (s *TaskService) recordClaimDecision(agentID, runtimeID pgtype.UUID, outcome string, scope claimScope, detail claimDecisionDetail) {
	scope.source = normalizeClaimSource(scope.source)
	reason := claimReasonFor(outcome, scope)
	if s.Metrics != nil {
		s.Metrics.RecordClaimDecision(scope.source, outcome, reason)
	}
	if outcome == claimOutcomeEmptyCacheHit || outcome == claimOutcomeEmptyDB {
		return
	}
	attrs := []any{
		"outcome", outcome,
		"reason", reason,
		"source", scope.source,
		"agent_id", util.UUIDToString(agentID),
		"runtime_id", util.UUIDToString(runtimeID),
		"candidates", scope.candidates,
	}
	if outcome == claimOutcomeNoCapacity {
		attrs = append(attrs, "running", detail.running, "max", detail.maxConcurrent)
	}
	slog.Debug("task claim decision", attrs...)
}
