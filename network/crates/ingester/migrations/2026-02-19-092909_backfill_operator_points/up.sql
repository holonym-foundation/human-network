-- Your SQL goes here
-- ===========================
-- Backfill performer points
-- ===========================

INSERT INTO operator_points_ledger
(operator, task_id, role, points, created_at)
SELECT
    tp.performer,
    t.id,
    'performer',
    t.task_size,
    t.timestamp
FROM task_performers tp
JOIN tasks t ON t.id = tp.task_id
ON CONFLICT (task_id, operator, role) DO NOTHING;


-- ===========================
-- Backfill attestor points
-- ===========================

INSERT INTO operator_points_ledger
(operator, task_id, role, points, created_at)
SELECT
    ta.attestor,
    t.id,
    'attestor',
    t.task_size * 0.1,
    t.timestamp
FROM task_attestors ta
JOIN tasks t ON t.id = ta.task_id
ON CONFLICT (task_id, operator, role) DO NOTHING;
