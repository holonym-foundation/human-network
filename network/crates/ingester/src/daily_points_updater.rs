use crate::PgPool;
use diesel::prelude::*;
use diesel::sql_query;
use std::time::Duration;
use tracing::{info, error};

pub async fn run_daily_points_updater(pool: PgPool) {
    loop {
        match update_operator_points_daily(pool.clone()).await {
            Ok(_) => info!("operator_points_daily updated"),
            Err(e) => error!("daily updater failed: {:?}", e),
        }

        // run every day
        tokio::time::sleep(Duration::from_secs(24 * 60 * 60)).await;
    }
}

async fn update_operator_points_daily(pool: PgPool)
    -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let mut conn = pool.get()?;

    sql_query(r#"
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
    "#)
    .execute(&mut conn)?;

    Ok(())
}
