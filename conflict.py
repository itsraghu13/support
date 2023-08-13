import concurrent.futures
import delta

def resolve_conflicts_optimistically(table_path):
    """
    Resolves conflicts in the given Delta Lake table optimistically.

    Args:
        table_path: The path to the Delta Lake table.
    """

    # Record the starting table version.
    starting_version = delta.get_current_version(table_path)

    # Record reads/writes.
    reads = []
    writes = []

    # Attempt a commit.
    commit = delta.commit(table_path, reads, writes)

    # If someone else wins, check whether anything you read has changed.
    if commit.state != delta.DeltaCommitState.COMMITTED:
        new_version = commit.version
        delta_log = delta.read_delta_log(table_path)
        for entry in delta_log.entries:
            if entry.version > new_version:
                # Something has changed, so we need to retry the commit.
                return resolve_conflicts_optimistically(table_path)

        # Nothing has changed, so we can proceed with the commit.
        return commit


def process_run(item):
    # Your existing code for processing each run
    queryActivity_URL = "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/"
    headers = {'Authorization': 'Bearer ' + access_token}
    # ... (rest of the code for processing a single run)


if len(run_list) > 0:
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # You can adjust max_workers to control the number of parallel threads
        futures = [executor.submit(process_run, item) for item in run_list]

        # Wait for all threads to finish
        concurrent.futures.wait(futures)

else:
    raise Exception('get_pipeline_list returned empty')



























import concurrent.futures
import delta

def process_run(item):
    queryActivity_URL = "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/"
    headers = {'Authorization': 'Bearer ' + access_token}

    # Get pipeline run details for the given pipelineName and run id
    pipeline_runs = fetch_pipeline_runs(queryActivity_URL, headers, generate_request_body(last_updated_after, last_updated_before, False))
    pipeline_runs = pipeline_runs['value']

    if pipeline_runs is not None and len(pipeline_runs) > 0:
        result = get_activity_output_data(pipeline_runs, item['runid'])
        parent_id, parent_name = get_parent_id_recursive(item['runid'])[0:2]

        df_parameters_data = get_parameters_data(item['runid'])

        table_exists = check_table_exists("structured.t_meta_copyactivities")

        if result:
            df = spark.createDataFrame(result, schema)

            if parent_id:
                df = df.withColumn("parent_runid", lit(parent_id)).withColumn("parent_name", lit(parent_name)).withColumn("requested_date", lit(current_date()))
            else:
                df = df.withColumn("parent_runid", lit('NA')).withColumn("parent_name", lit('NA')).withColumn("requested_date", lit(current_date()))

            if table_exists:
                while True:
                    try:
                        spark.sql("BEGIN TRANSACTION")
                        df.createOrReplaceTempView("temp_copyactivities")
                        sql_query_upsert = """
                            MERGE INTO structured.t_meta_copyactivities_123
                            AS target USING temp_copyactivities AS source
                            ON target.activityrunid = source.activityrunid
                            """
                        spark.sql(sql_query_upsert)
                        spark.sql("COMMIT")
                        break  # Exit the loop on successful commit
                    except DeltaConcurrencyControlException as e:
                        # Conflict detected, refresh data and try again
                        df = refresh_df(df)
        else:
            pass
    else:
        raise Exception('get_pipeline_runs did not return anything')

if len(run_list) > 0:
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # You can adjust max_workers to control the number of parallel threads
        futures = [executor.submit(process_run, item) for item in run_list]

        # Wait for all threads to finish
        concurrent.futures.wait(futures)

else:
    raise Exception('get_pipeline_list returned empty')


