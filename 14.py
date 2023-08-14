# Initialize an empty list to store the collected data
collected_data = []

if len(run_list) > 0:
    for item in run_list:
        queryActivity_URL = "https://management.azure.com/subscriptions/{}/resourceGroups/{}/providers/"
        headers = {'Authorization': 'Bearer ' + access_token}
        
        pipeline_runs = fetch_pipeline_runs(queryActivity_URL, headers, generate_request_body(last_updated_after, last_updated_before, False))
        pipeline_runs = pipeline_runs['value']
        
        if pipeline_runs is not None and len(pipeline_runs) > 0:
            result = get_activity_output_data(pipeline_runs, item['runid'])
            parent_id, parent_name = get_parent_id_recursive(item['runid'])[0:2]
            
            df_parameters_data = get_parameters_data(item['runid'])
            
            if result:
                collected_data.append((result, parent_id, parent_name, current_date()))
            else:
                pass
        else:
            raise Exception('get_pipeline_runs did not return anything')
else:
    raise Exception('get_pipeline_list returned empty')

# After collecting data from all runids, create a DataFrame and insert into the table
if collected_data:
    data_rows = []
    for data in collected_data:
        result, parent_id, parent_name, requested_date = data
        for row in result:
            data_rows.append(row + (parent_id, parent_name, requested_date))
    
    collected_df = spark.createDataFrame(data_rows, schema)
    
    table_exists = check_table_exists("structured.t_meta_copyactivities")
    if table_exists:
        collected_df.createOrReplaceTempView("temp_copyactivities")
        sql_query_upsert = """
            MERGE INTO structured.t_meta_copyactivities_123
            AS target USING temp_copyactivities AS source
            ON target.activityrunid = source.activityrunid
            """
        spark.sql(sql_query_upsert)
    else:
        collected_df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("structured.t_meta_copyactivities")



# After collecting data from all runids, create a DataFrame and insert into the table
if collected_data:
    data_rows = []
    for data in collected_data:
        result, parent_id, parent_name, requested_date = data
        for row_dict in result:
            # Add additional values to the dictionary
            row_dict["parent_id"] = parent_id
            row_dict["parent_name"] = parent_name
            row_dict["requested_date"] = requested_date
            data_rows.append(row_dict)
    
    collected_df = spark.createDataFrame(data_rows, schema)
    
    table_exists = check_table_exists("structured.t_meta_copyactivities")
    if table_exists:
        collected_df.createOrReplaceTempView("temp_copyactivities")
        sql_query_upsert = """
            MERGE INTO structured.t_meta_copyactivities_123
            AS target USING temp_copyactivities AS source
            ON target.activityrunid = source.activityrunid
            """
        spark.sql(sql_query_upsert)
    else:
        collected_df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("structured.t_meta_copyactivities")




from pyspark.sql import Row

# After collecting data from all runids, create a DataFrame and insert into the table
if collected_data:
    data_rows = []
    for data in collected_data:
        result, parent_id, parent_name, requested_date = data
        for row_dict in result:
            # Create a Row instance with the desired schema
            row = Row(**row_dict, parent_id=parent_id, parent_name=parent_name, requested_date=requested_date)
            data_rows.append(row)
    
    collected_df = spark.createDataFrame(data_rows, schema)
    
    table_exists = check_table_exists("structured.t_meta_copyactivities")
    if table_exists:
        collected_df.createOrReplaceTempView("temp_copyactivities")
        sql_query_upsert = """
            MERGE INTO structured.t_meta_copyactivities_123
            AS target USING temp_copyactivities AS source
            ON target.activityrunid = source.activityrunid
            """
        spark.sql(sql_query_upsert)
    else:
        collected_df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("structured.t_meta_copyactivities")











if collected_data:
    rows = []
    for data in collected_data:
        result, parent_id, parent_name, requested_date = data
        for row_dict in result:
            row_dict["parent_id"] = parent_id
            row_dict["parent_name"] = parent_name
            row_dict["requested_date"] = requested_date
            rows.append(tuple(row_dict.values()))
    
    # Create a temporary table to hold the rows
    temp_table_name = "temp_copyactivities_table"
    temp_table_df = spark.createDataFrame(rows, schema)
    temp_table_df.createOrReplaceTempView(temp_table_name)
    
    # Perform the merge operation using SQL
    table_exists = check_table_exists("structured.t_meta_copyactivities")
    if table_exists:
        sql_query_upsert = f"""
            MERGE INTO structured.t_meta_copyactivities_123
            AS target USING {temp_table_name} AS source
            ON target.activityrunid = source.activityrunid
            """
        spark.sql(sql_query_upsert)
    else:
        temp_table_df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("structured.t_meta_copyactivities")
    
    # Drop the temporary table
    spark.catalog.dropTempView(temp_table_name)


