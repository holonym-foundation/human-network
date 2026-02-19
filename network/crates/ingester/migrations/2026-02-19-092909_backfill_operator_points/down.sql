-- This file should undo anything in `up.sql`
-- Remove performer backfilled rows
DELETE FROM operator_points_ledger opl
USING task_performers tp, tasks t
WHERE
    opl.task_id = tp.task_id
    AND opl.operator = tp.performer
    AND opl.role = 'performer'
    AND t.id = tp.task_id;


-- Remove attestor backfilled rows
DELETE FROM operator_points_ledger opl
USING task_attestors ta, tasks t
WHERE
    opl.task_id = ta.task_id
    AND opl.operator = ta.attestor
    AND opl.role = 'attestor'
    AND t.id = ta.task_id;
