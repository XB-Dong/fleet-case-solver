-- Daily completion rate and comparable daily deltas. SQLite Lite dialect.
-- This metric is delivery completion, not the live per-minute dispatch acceptance metric.
WITH daily AS (
  SELECT date(created_at,'unixepoch','+8 hours') AS day,
         COUNT(*) AS tasks,
         SUM(status='completed') AS completed
  FROM tasks WHERE task_id NOT LIKE 'SIM-%'
  GROUP BY day
)
SELECT cur.day, cur.tasks, cur.completed,
       ROUND(100.0*cur.completed/NULLIF(cur.tasks,0),2) AS completion_pct,
       ROUND(100.0*(cur.tasks-prev.tasks)/NULLIF(prev.tasks,0),2) AS day_over_day_pct,
       ROUND(100.0*(cur.tasks-week.tasks)/NULLIF(week.tasks,0),2) AS same_weekday_pct
FROM daily cur
LEFT JOIN daily prev ON prev.day=date(cur.day,'-1 day')
LEFT JOIN daily week ON week.day=date(cur.day,'-7 days')
ORDER BY cur.day DESC;

-- Check the difference between event time and ingest time.
SELECT station_id, MIN(event_time) AS first_event, MAX(event_time) AS last_event,
       COUNT(*) AS rows, ROUND(AVG(ingest_time-event_time),2) AS avg_lag_s
FROM telemetry
GROUP BY station_id;
