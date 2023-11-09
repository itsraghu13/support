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
