def get_parameters_data(pipeline_run_id):

    # Get the parameters data from Azure Data Factory
    parameter_adf_url = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelineruns/{pipeline_run_id}?api-versions=2018-06-01"
    headers = {'Authorization': 'Bearer ' + access_token}
    response = requests.get(parameter_adf_url, headers=headers)
    data = json.loads(response.text)

    # Read the parameters data into a Spark DataFrame
    df = spark.read.json(sc.parallelize([json.dumps(data)]))

    # Select the desired columns
    df = df.select('runId', 'pipelineName', 'parameters', 'message', 'runStart', 'runEnd', 'durationInMs', 'status')

    # Create a temporary view
    df.createOrReplaceTempView('tempDF')

    # Get the list of columns to select
    select_cols = ['pipelineName as pipeline_name', 'runId as run_id', 'date_format(runStart, "yyyy-MM-dd HH:mm:ss") as start_time', 'date_format(runEnd, "yyyy-MM-dd HH:mm:ss") as end_time']

    # Get the list of parameter columns
    parameter_cols = [f'{col_name.split("-")[-1]}: parameters." + col_name.split(".")[-1] for col_name in df.select('parameters.*').columns]

    # Generate the SQL query
    sql_query = f"SELECT {','.join(select_cols)}, CONCAT_WS(', ', {','.join(parameter_cols)}) AS parameters, CONCAT(CAST(FLOOR(durationInMs / 60000) AS VARCHAR(10)), ' minutes') AS duration_in_minutes FROM tempDF"

    # Run the SQL query
    df_combined = spark.sql(sql_query)

    # Write the DataFrame to a Delta table
    df_combined.write.format('delta').mode('append').option('overwriteSchema', 'true').saveAsTable('structured.t_meta_pipelines')

    # Get the existing data from the Delta table
    df_existing = spark.read.table('structured.t_meta_pipelines')

    # Merge the two DataFrames
    df_merged = df_combined.merge(df_existing, on='runId', how='left')

    # Write the merged DataFrame back to the Delta table
    df_merged.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').saveAsTable('structured.t_meta_pipelines')
                      
                      
                      
                      
df_merged = sqlContext.sql(f"MERGE INTO structured.t_meta_pipelines AS t USING (SELECT * FROM {df_combined.alias('c')}) AS c ON t.runId = c.runId WHEN MATCHED THEN UPDATE SET parameters = c.parameters WHEN NOT MATCHED THEN INSERT (runId, pipelineName, parameters, message, runStart, runEnd, durationInMs, status) VALUES (c.runId, c.pipelineName, c.parameters, c.message, c.runStart, c.runEnd, c.durationInMs, c.status)")

