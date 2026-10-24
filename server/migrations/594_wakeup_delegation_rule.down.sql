DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE system_rule LIKE 'delegation_done:%');
DELETE FROM agent_task_queue WHERE context->>'wakeup_id' IN (SELECT id::text FROM issue_wakeup WHERE system_rule LIKE 'delegation_done:%');
DELETE FROM issue_wakeup WHERE system_rule LIKE 'delegation_done:%';
ALTER TABLE issue_wakeup DROP CONSTRAINT IF EXISTS issue_wakeup_system_rule_check;
ALTER TABLE issue_wakeup ADD CONSTRAINT issue_wakeup_system_rule_check CHECK (system_rule IN ('child_done'));
