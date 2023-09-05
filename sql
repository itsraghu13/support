SELECT meta_copyactivity.activity_name,
       meta_copyactivity.parent_name,
       meta_pipelines.pipeline_name,
       COUNT(*) AS total_runs,
       AVG(meta_copyactivity.duration) AS avg_duration,
       SUM(meta_copyactivity.rows_processed) / COUNT(*) AS avg_rows,
       AVG(meta_copyactivity.duration) OVER (PARTITION BY meta_copyactivity.activity_name) AS avg_runtime,
       AVG(meta_copyactivity.duration) OVER (PARTITION BY meta_copyactivity.activity_name ORDER BY meta_copyactivity.request_date ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_runtime_15,
       AVG(meta_copyactivity.duration) OVER (PARTITION BY meta_copyactivity.activity_name ORDER BY meta_copyactivity.request_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_runtime_30
FROM meta_copyactivity
INNER JOIN meta_pipelines ON meta_copyactivity.parent_runid = meta_pipelines.run_id
GROUP BY meta_copyactivity.activity_name, meta_copyactivity.parent_name, meta_pipelines.pipeline_name;
