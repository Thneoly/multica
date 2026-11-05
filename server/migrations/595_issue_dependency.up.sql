-- 595: blocked-by dependency relation (direction 5, local research branch).
-- Replaces the dead issue_dependency table from 001_init (different columns,
-- zero application code consumed it — see the squad-break root-cause memo).
-- Wake side: SetIssueDependency arms a system other_issue rule
-- (dependent_done:<blocker>). Gate side: claimTask returns the claimed row
-- to queued while any blocker is unmet (queue_wait_reason waiting_dependency).
-- No foreign keys, per repo convention; the pair is unique.

DROP TABLE IF EXISTS issue_dependency;
CREATE TABLE issue_dependency (
    id uuid PRIMARY KEY,
    workspace_id uuid NOT NULL,
    blocked_issue_id uuid NOT NULL,
    blocker_issue_id uuid NOT NULL,
    created_by uuid,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (blocked_issue_id, blocker_issue_id),
    CONSTRAINT issue_dependency_no_self CHECK (blocked_issue_id <> blocker_issue_id)
);
CREATE INDEX issue_dependency_blocker_idx ON issue_dependency (blocker_issue_id);
