-- The delegation_done platform rule wakes a squad's leader when a run the
-- leader delegated by @mention reaches a terminal state (migration-family
-- comment in server/internal/service/issue_wakeup_delegation.go). One rule
-- exists per delegated run: the member task is part of the stored value
-- ('delegation_done:<task uuid>'), so the (issue_id, system_rule) unique index
-- is also the rule's idempotency fingerprint. Unlike child_done, the rule
-- names a fixed target (the squad's leader) and the human its run continues,
-- so agent_id and created_by are set.
ALTER TABLE issue_wakeup DROP CONSTRAINT IF EXISTS issue_wakeup_system_rule_check;
ALTER TABLE issue_wakeup ADD CONSTRAINT issue_wakeup_system_rule_check
  CHECK (system_rule IN ('child_done')
         OR system_rule ~ '^delegation_done:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$');
