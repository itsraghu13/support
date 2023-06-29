from multiprocessing import Pool

def process_run(item):
    queryActivity_URI = 'https://management.azure.com/subscriptions/(l/resourceGroups/(l/providers/Microsoft.DataFactory/factories/(/pipelineru'
    headers = {'Authorization': 'Bearer ' + access_token}

    # Get pipeline run details for given pipelineName and run id
    pipeline_runs = get_pipeline_runs(queryActivity_URL, headers)

    if pipeline_runs is not None:
        # Get Pipeline Copy Activity details
        result = get_activity_output_data(pipeline_runs, item['runId'])

        # Get parent id for given run id
        P_id = get_parent_id_recursive(item['runId'])

        if result:
            df = spark.createDataFrame(result, schema)

            if P_id:
                df = df.withColumn("Parent RunId", lit(P_id)).withColumn("requested date", lit(current_date()))
            else:
                df = df.withColumn("Parent_ RunId", lit('NA')).withColumn("requested date", lit(current_date()))
        else:
            return None

        df_parmetes_data = get_parameters_data(item['runId'])

        if spark.catalog.tableExists("structured._meta_copyactivities"):
            table_exists = True
        else:
            table_exists = False

        if table_exists:
            df.createOrReplaceTempView('temp_copyactivities')
            sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
            spark.sql(sql_query_upsert)
        else:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')
    else:
        raise Exception('get_pipeline_runs did not return anything')

if len(run_list) > 0:
    # Define the number of processes to run in parallel
    num_processes = 4

    # Create a pool of processes
    pool = Pool(processes=num_processes)

    # Execute the process_run function for each item in run_list in parallel
    pool.map(process_run, run_list)

    # Close the pool of processes
    pool.close()
    pool.join()
else:
    raise Exception('get_pipeline_list returned empty')
