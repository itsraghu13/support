def run_pipeline_run(item, resource_group_name, subscription_id):
    queryActivity_URI = 'https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.DataFactory/factories/{}/pipelineru'.format(
        subscription_id, resource_group_name, item['factoryName'])
    headers = {'Authorization': 'Bearer ' + access_token}

    print('Processing item:', item)

    # Get pipeline run details for given pipelineName and run id
    pipeline_runs = get_pipeline_runs(queryActivity_URL, headers)

    if pipeline_runs is not None:
        print('Pipeline runs found')

        # Get Pipeline Copy Activity details
        result = get_activity_output_data(pipeline_runs, item['runId'])

        # Get parent id for given run id
        P_id = get_parent_id_recursive(item['runId'])

        if result:
            print('Copy activity data found')

            df = spark.createDataFrame(result, schema)

            if P_id:
                print('Parent run id found')
                df = df.withColumn("Parent RunId", lit(P_id)).withColumn("requested date", lit(current_date()))
            else:
                print('Parent run id not found')
                df = df.withColumn("Parent_ RunId", lit('NA')).withColumn("requested date", lit(current_date()))
        else:
            print('Copy activity data not found')
            return

        df_parmetes_data = get_parameters_data(item['runId'])

        if spark.catalog.tableExists("structured._meta_copyactivities"):
            print('Table exists')
            df.createOrReplaceTempView('temp_copyactivities')
            sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
            spark.sql(sql_query_upsert)
        else:
            print('Table does not exist')
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')

threads = []

for item in run_list:
    thread = threading.Thread(target=run_pipeline_run, args=(item, resource_group_name, subscription_id))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()


def run_pipeline_runs(run_list):
    batches = []
    for i in range(0, len(run_list), 100):
        batch = run_list[i:i + 100]
        batches.append(batch)

    for batch in batches:
        run_pipeline_run(batch)











import threading
import time

def run_pipeline_run(batch):
    queryActivity_URI = 'https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.DataFactory/factories/{}/pipelineru'.format(
        batch[0]['subscription_id'], batch[0]['resource_group_name'], batch[0]['factoryName'])
    headers = {'Authorization': 'Bearer ' + access_token}

    print('Processing batch:', batch)

    # Get pipeline run details for given pipelineName and run id
    pipeline_runs = get_pipeline_runs(queryActivity_URL, headers)

    if pipeline_runs is not None:
        print('Pipeline runs found')

        # Get Pipeline Copy Activity details
        result = get_activity_output_data(pipeline_runs, batch[0]['runId'])

        # Get parent id for given run id
        P_id = get_parent_id_recursive(batch[0]['runId'])

        if result:
            print('Copy activity data found')

            df = spark.createDataFrame(result, schema)

            if P_id:
                print('Parent run id found')
                df = df.withColumn("Parent RunId", lit(P_id)).withColumn("requested date", lit(current_date()))
            else:
                print('Parent run id not found')
                df = df.withColumn("Parent_ RunId", lit('NA')).withColumn("requested date", lit(current_date()))
        else:
            print('Copy activity data not found')
            return

        df_parmetes_data = get_parameters_data(batch[0]['runId'])

        if spark.catalog.tableExists("structured._meta_copyactivities"):
            print('Table exists')
            df.createOrReplaceTempView('temp_copyactivities')
            sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
            spark.sql(sql_query_upsert)
        else:
            print('Table does not exist')
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')

def run_pipeline_runs(run_list):
    batches = []
    for i in range(0, len(run_list), 100):
        batch = run_list[i:i + 100]
        batches.append(batch)

    for batch in batches:
        thread = threading.Thread(target=run_pipeline_run, args=(batch,))
        thread.start()

for thread in threads:
    thread.join()


