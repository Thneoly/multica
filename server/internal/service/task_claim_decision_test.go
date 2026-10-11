package service

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	obsmetrics "github.com/multica-ai/multica/server/internal/metrics"
)

// This runs without a database: claimReasonFor, normalizeClaimSource, and the
// metric funnel are pure functions over their arguments.

// TestClaimDecisionReasonFor pins the outcome -> reason mapping, especially
// the #8551 split inside no_tasks: candidates > 0 means work was visible and a
// SQL fence rejected it, zero candidates on a list-bearing path means the
// queue really was empty, and the list-less agent path can only report
// "unknown".
func TestClaimDecisionReasonFor(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		outcome string
		scope   claimScope
		want    string
	}{
		{name: "claimed stays claimed", outcome: claimOutcomeClaimed, scope: claimScope{source: claimSourceAgent}, want: claimReasonClaimed},
		{name: "claimed via runtime poll", outcome: claimOutcomeClaimed, scope: claimScope{source: claimSourceRuntime, candidates: 3}, want: claimReasonClaimed},
		{name: "no capacity maps to concurrency cap", outcome: claimOutcomeNoCapacity, scope: claimScope{source: claimSourceRuntime}, want: claimReasonAgentConcurrencyCap},
		{name: "no runtime maps to unbound agent", outcome: claimOutcomeNoRuntime, scope: claimScope{source: claimSourceAgent}, want: claimReasonAgentUnbound},
		{name: "runtime mismatch maps to rebound agent", outcome: claimOutcomeRuntimeMismatch, scope: claimScope{source: claimSourceRuntime}, want: claimReasonAgentRebound},

		{name: "no tasks with candidates is blocked", outcome: claimOutcomeNoTasks, scope: claimScope{source: claimSourceRuntime, candidates: 1}, want: claimReasonCandidatesBlocked},
		{name: "no tasks blocked on batch path", outcome: claimOutcomeNoTasks, scope: claimScope{source: claimSourceBatch, candidates: 2}, want: claimReasonCandidatesBlocked},
		{name: "no tasks without candidates is empty queue", outcome: claimOutcomeNoTasks, scope: claimScope{source: claimSourceRuntime}, want: claimReasonNoCandidates},
		{name: "no tasks without candidates on batch", outcome: claimOutcomeNoTasks, scope: claimScope{source: claimSourceBatch}, want: claimReasonNoCandidates},
		{name: "no tasks on agent path is unknown", outcome: claimOutcomeNoTasks, scope: claimScope{source: claimSourceAgent}, want: claimReasonUnknownCandidates},
		{name: "agent path with candidates is still blocked", outcome: claimOutcomeNoTasks, scope: claimScope{source: claimSourceAgent, candidates: 1}, want: claimReasonCandidatesBlocked},

		{name: "empty cache hit", outcome: claimOutcomeEmptyCacheHit, scope: claimScope{source: claimSourceRuntime}, want: claimReasonEmptyCache},
		{name: "empty db", outcome: claimOutcomeEmptyDB, scope: claimScope{source: claimSourceRuntime}, want: claimReasonEmptyQueue},
		{name: "reclaimed dispatch", outcome: claimOutcomeReclaimedDispatched, scope: claimScope{source: claimSourceRuntime}, want: claimReasonStaleDispatchRecovered},

		{name: "error get agent", outcome: claimOutcomeErrorGetAgent, scope: claimScope{source: claimSourceAgent}, want: claimReasonError},
		{name: "error count running", outcome: claimOutcomeErrorCountRunning, scope: claimScope{source: claimSourceRuntime}, want: claimReasonError},
		{name: "error claim", outcome: claimOutcomeErrorClaim, scope: claimScope{source: claimSourceRuntime}, want: claimReasonError},
		{name: "error reanchor chat input", outcome: claimOutcomeErrorReanchorChatInput, scope: claimScope{source: claimSourceRuntime}, want: claimReasonError},
		{name: "error transaction", outcome: claimOutcomeErrorTransaction, scope: claimScope{source: claimSourceBatch}, want: claimReasonError},
		{name: "error promote deferred", outcome: claimOutcomeErrorPromoteDeferred, scope: claimScope{source: claimSourceRuntime}, want: claimReasonError},
		{name: "error reclaim dispatched", outcome: claimOutcomeErrorReclaimDispatched, scope: claimScope{source: claimSourceRuntime}, want: claimReasonError},
		{name: "error list", outcome: claimOutcomeErrorList, scope: claimScope{source: claimSourceRuntime}, want: claimReasonError},

		// Unknown inputs degrade to "error" instead of minting a series.
		{name: "unrecognized outcome", outcome: "no_task", scope: claimScope{source: claimSourceRuntime}, want: claimReasonError},
		{name: "bogus outcome", outcome: "surprise", scope: claimScope{source: claimSourceAgent}, want: claimReasonError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := claimReasonFor(tt.outcome, tt.scope)
			if got != tt.want {
				t.Fatalf("claimReasonFor(%q, %+v) = %q, want %q", tt.outcome, tt.scope, got, tt.want)
			}
		})
	}
}

// TestClaimDecisionNormalizeSource keeps the source label a closed enum.
func TestClaimDecisionNormalizeSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		source string
		want   string
	}{
		{source: claimSourceAgent, want: claimSourceAgent},
		{source: claimSourceRuntime, want: claimSourceRuntime},
		{source: claimSourceBatch, want: claimSourceBatch},
		{source: "", want: "other"},
		{source: "daemon", want: "other"},
		{source: "AGENT", want: "other"},
	}

	for _, tt := range tests {
		t.Run(tt.source, func(t *testing.T) {
			t.Parallel()
			if got := normalizeClaimSource(tt.source); got != tt.want {
				t.Fatalf("normalizeClaimSource(%q) = %q, want %q", tt.source, got, tt.want)
			}
		})
	}
}

// TestClaimDecisionNilMetricsIsSafe covers self-hosted deployments and tests
// that run without the metrics listener: the funnel must not panic and must
// still skip the (suppressed) hot-path log branches.
func TestClaimDecisionNilMetricsIsSafe(t *testing.T) {
	t.Parallel()

	svc := &TaskService{}
	for _, outcome := range []string{
		claimOutcomeClaimed,
		claimOutcomeNoTasks,
		claimOutcomeNoCapacity,
		claimOutcomeNoRuntime,
		claimOutcomeRuntimeMismatch,
		claimOutcomeEmptyCacheHit,
		claimOutcomeEmptyDB,
		claimOutcomeErrorClaim,
	} {
		svc.recordClaimDecision(pgtype.UUID{}, pgtype.UUID{}, outcome, claimScope{source: claimSourceRuntime, candidates: 1}, claimDecisionDetail{running: 2, maxConcurrent: 2})
	}
}

// TestClaimDecisionIncrementsLabeledSeries verifies the funnel lands each
// decision on exactly its (source, outcome, reason) series, that an
// unclassified source is normalized to "other", and that a never-fired
// prewarmed series still reads as zero rather than being absent.
func TestClaimDecisionIncrementsLabeledSeries(t *testing.T) {
	t.Parallel()

	m := obsmetrics.NewBusinessMetrics()
	svc := &TaskService{Metrics: m}

	svc.recordClaimDecision(pgtype.UUID{}, pgtype.UUID{}, claimOutcomeNoTasks, claimScope{source: claimSourceRuntime, candidates: 2}, claimDecisionDetail{})
	svc.recordClaimDecision(pgtype.UUID{}, pgtype.UUID{}, claimOutcomeNoTasks, claimScope{source: claimSourceAgent}, claimDecisionDetail{})
	svc.recordClaimDecision(pgtype.UUID{}, pgtype.UUID{}, claimOutcomeNoCapacity, claimScope{source: claimSourceAgent}, claimDecisionDetail{running: 3, maxConcurrent: 3})
	svc.recordClaimDecision(pgtype.UUID{}, pgtype.UUID{}, claimOutcomeEmptyDB, claimScope{source: claimSourceRuntime}, claimDecisionDetail{})
	svc.recordClaimDecision(pgtype.UUID{}, pgtype.UUID{}, claimOutcomeErrorList, claimScope{source: "daemon"}, claimDecisionDetail{})

	tests := []struct {
		source, outcome, reason string
		want                    float64
	}{
		{claimSourceRuntime, claimOutcomeNoTasks, claimReasonCandidatesBlocked, 1},
		{claimSourceRuntime, claimOutcomeNoTasks, claimReasonNoCandidates, 0},
		{claimSourceAgent, claimOutcomeNoTasks, claimReasonUnknownCandidates, 1},
		{claimSourceAgent, claimOutcomeNoCapacity, claimReasonAgentConcurrencyCap, 1},
		{claimSourceRuntime, claimOutcomeEmptyDB, claimReasonEmptyQueue, 1},
		// An unknown source is normalized to "other" rather than minting a
		// per-call-site series.
		{"other", claimOutcomeErrorList, claimReasonError, 1},
		{claimSourceRuntime, claimOutcomeErrorList, claimReasonError, 0},
		// A prewarmed series that has not fired reads as zero.
		{claimSourceBatch, claimOutcomeNoTasks, claimReasonCandidatesBlocked, 0},
		{claimSourceRuntime, claimOutcomeReclaimedDispatched, claimReasonStaleDispatchRecovered, 0},
	}
	for _, tt := range tests {
		t.Run(tt.source+"/"+tt.outcome+"/"+tt.reason, func(t *testing.T) {
			t.Parallel()
			if got := claimDecisionCount(t, m, tt.source, tt.outcome, tt.reason); got != tt.want {
				t.Fatalf("claim_decision_total{%s,%s,%s} = %v, want %v", tt.source, tt.outcome, tt.reason, got, tt.want)
			}
		})
	}
}

// ---- helpers ---------------------------------------------------------------

// claimDecisionCount reads one (source, outcome, reason) series of the claim
// decision counter out of a freshly gathered registry.
func claimDecisionCount(t *testing.T, m *obsmetrics.BusinessMetrics, source, outcome, reason string) float64 {
	t.Helper()

	fam := obsmetrics.GatherForTest(t, m)["multica_agent_task_claim_decision_total"]
	if fam == nil {
		t.Fatalf("multica_agent_task_claim_decision_total not registered")
	}
	for _, mtr := range fam.GetMetric() {
		if labelValue(mtr, "source") == source && labelValue(mtr, "outcome") == outcome && labelValue(mtr, "reason") == reason {
			return mtr.GetCounter().GetValue()
		}
	}
	t.Fatalf("no sample for %s/%s/%s", source, outcome, reason)
	return 0
}
