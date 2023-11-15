from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
import time

def append_data_with_retry(table_path, data_to_append, max_retry_attempts=5, initial_backoff_seconds=1):
    retry_attempt = 0
    backoff_seconds = initial_backoff_seconds

    while retry_attempt < max_retry_attempts:
        try:
            delta_table = DeltaTable.forPath(spark, table_path)
            delta_table.alias("existing_data").merge(
                data_to_append.alias("new_data"),
                "existing_data.id = new_data.id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            break  # Success, exit the loop
        except AnalysisException as e:
            if "ConcurrentAppendException" in str(e):
                # Handle the ConcurrentAppendException
                print(f"Concurrent append conflict. Retrying (Attempt {retry_attempt + 1}).")
                retry_attempt += 1
                if retry_attempt < max_retry_attempts:
                    # Exponential backoff with some jitter
                    backoff_seconds *= 2
                    time.sleep(backoff_seconds + random.uniform(-1, 1))
            else:
                raise  # Propagate the exception if it's not a ConcurrentAppendException

# Usage
table_path = "/mnt/delta/table_name"
data_to_append = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])

# Attempt to append data with robust retries
append_data_with_retry(table_path, data_to_append)











WITH RECURSIVE WaitForInProgress AS (
    SELECT
        config.config_id,
        CONCAT('SELECT * FROM ', schema_name, ' ', REPLACE(config.table_name, 'dbo_', '')) AS query,
        DATE_FORMAT(delta_end_time, 'yyyy/MM/dd HH:mm:ss') AS begin_time,
        DATE_FORMAT(currenc_camescamp, 'yyyy/MM/dd HH:mm:ss') AS end_time,
        config.table_name,
        config.load_type,
        config.schema_name,
        config.do_name,
        config.source_name,
        config.db_type,
        config.frequency,
        config.secret_name,
        config.watermark_columns,
        config.custom_refresh,
        process_id AS from_structured_t_database_config
    FROM
        structured.t_database_config config
    LEFT JOIN (
        SELECT
            t_dataload.*
        FROM
            structured.t_dataload t_dataload
        JOIN (
            SELECT
                config_id,
                MAX(CAST(REPLACE(process_id, 'ID', '') AS INT)) AS process_id
            FROM
                structured.t_dataload
            GROUP BY
                config_id
        ) max_processid ON t_dataload.config_id = max_processid.config_id
        AND CONCAT('ID', max_processid.config_id, 'R', max_processid.process_id) = t_dataload.process_id
    ) dataload ON config.config_id = dataload.config_id
    WHERE
        do_name = 'int1'
        AND author = 'Raghav'
        AND db_type = 'sql server'
        AND status = 'success'
        AND dataload.process_status = 'inprogress'
    -- Recursive member with a limit
    UNION ALL
    SELECT
        config.config_id,
        CONCAT('SELECT * FROM ', schema_name, ' ', REPLACE(config.table_name, 'dbo_', '')) AS query,
        DATE_FORMAT(delta_end_time, 'yyyy/MM/dd HH:mm:ss') AS begin_time,
        DATE_FORMAT(currenc_camescamp, 'yyyy/MM/dd HH:mm:ss') AS end_time,
        config.table_name,
        config.load_type,
        config.schema_name,
        config.do_name,
        config.source_name,
        config.db_type,
        config.frequency,
        config.secret_name,
        config.watermark_columns,
        config.custom_refresh,
        process_id AS from_structured_t_database_config
    FROM
        WaitForInProgress
    WHERE
        WaitForInProgress.process_status = 'inprogress'
        AND WaitForInProgress.__iteration < 10  -- Limit the number of recursive iterations
)
SELECT *
FROM WaitForInProgress;

