import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, floor, current_date

# Create a Spark session
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

# Create an empty list to hold collected data
collected_data = []

def get_parameters_data(pipeline_runId):
    parameter_adf_URI = f'https://management.azure.com/subscriptions/<subscription_id>/resourceGroups/<resource_GroupName>/providers/Microsoft.DataFactory/factories/<factory_name>/pipelineruns/{pipeline_runId}?api-version=2018-06-01'
    headers = {'Authorization': 'Bearer <access_token>'}
    
    response = requests.get(parameter_adf_URI, headers=headers)
    data = json.loads(response.text)
    # spark = SparkSession.builder.getOrCreate()
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(data)]))
    
    df = df.select(
        col('runId'),
        col('pipelineName').alias('pipeline_name'),
        col('parameters'),
        col('message').alias('error'),
        col('runStart').alias('start_time'),
        col('runEnd').alias('end_time'),
        col('durationInMs'),
        col('status')
    )
    
    df.createOrReplaceTempView("tempDF")
    
    select_cols = [
        "pipeline_name",
        "runId",
        "date_format(start_time, 'yyyy-MM-dd HH:mm:ss') as start_time",
        "date_format(end_time, 'yyyy-MM-dd HH:mm:ss') as end_time",
        "error",
        "status"
    ]
    
    parameter_cols = [
        f"concat('{col_name.split('.')[-1]}:', parameters.`{col_name.split('.')[-1]}`)" 
        for col_name in df.select('parameters.*').columns
    ]
    
    sql_query = (
        f"SELECT " + "," .join(select_cols) + ", "
        f"concat_ws('|', " + ",".join(parameter_cols) + ") as parameters, "
        f"CONCAT(CAST(FLOOR(durationInMs / 60000) AS STRING), ' min ', "
        f"CAST(FLOOR((durationInMs % 60000 / 1000)) AS STRING), ' Sec') as duration, "
        f"current_date() as requested_date FROM tempDF"
    )
    
    df_combined = spark.sql(sql_query)
    collected_data.append(df_combined.collect())  # Collect the data

# Call the function with a valid pipeline_runId
get_parameters_data("<pipeline_runId>")

# Process all collected data
if collected_data:
    all_collected_rows = []
    for data in collected_data:
        all_collected_rows.extend(data)
    
    # Combine all rows into a DataFrame
    collected_df = spark.createDataFrame(all_collected_rows)
    
    # Create a temporary table to hold the DataFrame
    temp_table_name = "temp_pipelines_table"
    collected_df.createOrReplaceTempView(temp_table_name)
    
    # Perform the merge operation using SQL
    table_name = "<table_name>"
    table_exists = spark.catalog.tableExists(table_name)
    
    if table_exists:
        sql_query_upsert = f"""
            MERGE INTO {table_name} AS target
            USING {temp_table_name} AS source
            ON target.runId = source.runId
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(sql_query_upsert)
    else:
        collected_df.write.format("delta").mode("append").saveAsTable(table_name)
    
    # Drop the temporary table
    spark.catalog.dropTempView(temp_table_name)
