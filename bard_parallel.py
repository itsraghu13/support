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

































import threading

def wait_for_update_or_insert(table_name):
    while True:
        if spark.catalog.tableExists(table_name):
            return
        time.sleep(5)

def run_pipeline_run(item, resource_group_name, subscription_id):
    semaphore = api_semaphore.Semaphore(10)

    with semaphore:
        queryActivity_URI = 'https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.DataFactory/factories/{}/pipelineru'.format(
            subscription_id, resource_group_name, item['factoryName'])
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
                return

            df_parmetes_data = get_parameters_data(item['runId'])

            if spark.catalog.tableExists("structured._meta_copyactivities"):
                table_exists = True
            else:
                table_exists = False

            if table_exists:
                df.createOrReplaceTempView('temp_copyactivities')
                sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                while not wait_for_update_or_insert("structured._meta_copyactivities"):
                    pass
                spark.sql(sql_query_upsert)
            else:
                df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')

threads = []

for item in run_list:
    semaphore.acquire()
    thread = threading.Thread(target=run_pipeline_run, args=(item, resource_group_name, subscription_id))
    thread.start()
    threads.append(thread)

for thread in threads:
    thread.join()
    semaphore.release()
























import threading
import api_semaphore  # Make sure you have this library imported

# Create a semaphore with a limited number of permits
max_concurrent_threads = 1
semaphore = api_semaphore.Semaphore(max_concurrent_threads)

# Create a barrier to wait until all operations are completed
barrier = threading.Barrier(max_concurrent_threads + 1)  # +1 for the main thread

def run_pipeline_run(item, resource_group_name, subscription_id):
    queryActivity_URI = 'https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.DataFactory/factories/{}/pipelineruns/{}'.format(
        subscription_id, resource_group_name, item['factoryName'], item['runId'])
    headers = {'Authorization': 'Bearer ' + access_token}

    # Get pipeline run details for the given pipelineName and run id
    pipeline_runs = get_pipeline_runs(queryActivity_URI, headers)

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
            return

        df_parmetes_data = get_parameters_data(item['runId'])

        with semaphore:
            if spark.catalog.tableExists("structured._meta_copyactivities"):
                df.createOrReplaceTempView('temp_copyactivities')
                sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                spark.sql(sql_query_upsert)
            else:
                df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')

    # Wait for all operations to complete before releasing the semaphore
    barrier.wait()
    semaphore.release()

# Split run_list into batches of 5
batch_size = 5
batched_run_list = [run_list[i:i + batch_size] for i in range(0, len(run_list), batch_size)]

for batch in batched_run_list:
    threads = []
    for item in batch:
        thread = threading.Thread(target=run_pipeline_run, args=(item, resource_group_name, subscription_id))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete before moving to the next batch
    for thread in threads:
        thread.join()















import threading
import api_semaphore  # Make sure you have this library imported

# Create a semaphore with a limited number of permits
max_concurrent_threads = 1
semaphore = api_semaphore.Semaphore(max_concurrent_threads)

# Create a barrier to wait until all operations are completed
barrier = threading.Barrier(max_concurrent_threads + 1)  # +1 for the main thread

def run_pipeline_run(item, resource_group_name, subscription_id):
    queryActivity_URI = 'https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.DataFactory/factories/{}/pipelineruns/{}'.format(
        subscription_id, resource_group_name, item['factoryName'], item['runId'])
    headers = {'Authorization': 'Bearer ' + access_token}

    # Get pipeline run details for the given pipelineName and run id
    pipeline_runs = get_pipeline_runs(queryActivity_URI, headers)

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
            return

        df_parmetes_data = get_parameters_data(item['runId'])

        with semaphore:
            if spark.catalog.tableExists("structured._meta_copyactivities"):
                df.createOrReplaceTempView('temp_copyactivities')
                sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                spark.sql(sql_query_upsert)
            else:
                df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')

    # Release the semaphore before proceeding to the next operation
    semaphore.release()

# Split run_list into batches of 5
batch_size = 5
batched_run_list = [run_list[i:i + batch_size] for i in range(0, len(run_list), batch_size)]

for batch in batched_run_list:
    threads = []
    for item in batch:
        thread = threading.Thread(target=run_pipeline_run, args=(item, resource_group_name, subscription_id))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete before moving to the next batch
    for thread in threads:
        thread.join()

# Ensure that all threads have finished before moving on
barrier.wait()

























import threading
import api_semaphore  # Make sure you have this library imported

# Create a semaphore with a limited number of permits
max_concurrent_threads = 1
semaphore = api_semaphore.Semaphore(max_concurrent_threads)

# Create a barrier to wait until all operations are completed
barrier = threading.Barrier(max_concurrent_threads + 1)  # +1 for the main thread

def run_pipeline_run(item, resource_group_name, subscription_id):
    queryActivity_URI = 'https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/Microsoft.DataFactory/factories/{}/pipelineruns/{}'.format(
        subscription_id, resource_group_name, item['factoryName'], item['runId'])
    headers = {'Authorization': 'Bearer ' + access_token}

    # Get pipeline run details for the given pipelineName and run id
    pipeline_runs = get_pipeline_runs(queryActivity_URI, headers)

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
            return

        df_parmetes_data = get_parameters_data(item['runId'])

        with semaphore:
            if spark.catalog.tableExists("structured._meta_copyactivities"):
                # Acquire a permit from the semaphore before proceeding
                semaphore.acquire()

                # Update the table
                df.createOrReplaceTempView('temp_copyactivities')
                sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
                spark.sql(sql_query_upsert)

                # Release the permit after the operation is complete
                semaphore.release()
            else:
                df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')

    # Release the semaphore before proceeding to the next operation
    semaphore.release()

# Split run_list into batches of 5
batch_size = 5
batched_run_list = [run_list[i:i + batch_size] for i in range(0, len(run_list), batch_size)]

for batch in batched_run_list:
    threads = []
    for item in batch:
        thread = threading.Thread(target=run_pipeline_run, args=(item, resource_group_name, subscription_id))
        thread.start()
        threads.append(thread)

    # Wait for all threads to complete before moving to the next batch
    for thread in threads:
        thread.join()

# Ensure that all threads have finished before moving on
barrier.wait()




