-- 596: allow dependent_done:<blocker-uuid> as a system rule (direction 5).
ALTER TABLE issue_wakeup DROP CONSTRAINT IF EXISTS issue_wakeup_system_rule_check;
ALTER TABLE issue_wakeup ADD CONSTRAINT issue_wakeup_system_rule_check CHECK (
  system_rule = 'child_done'
  OR system_rule ~ '^delegation_done:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  OR system_rule ~ '^dependent_done:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
);
