-- Your SQL goes here
INSERT INTO operator_points_daily (operator, snapshot_time, cumulative_points)
WITH daily_points AS (
    SELECT
        operator,
        date_trunc('day', created_at) AS snapshot_time,
        SUM(points) AS earned
    FROM operator_points_ledger
    GROUP BY operator, date_trunc('day', created_at)
),
cumulative AS (
    SELECT
        operator,
        snapshot_time,
        SUM(earned)
            OVER (
                PARTITION BY operator
                ORDER BY snapshot_time
            ) AS cumulative_points
    FROM daily_points
)
SELECT operator, snapshot_time, cumulative_points
FROM cumulative
ON CONFLICT (operator, snapshot_time)
DO UPDATE SET cumulative_points = EXCLUDED.cumulative_points;
