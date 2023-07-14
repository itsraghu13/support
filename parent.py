def get_parent_id_recursive(input_id):
    # Construct request URL using input parameters
    pipelineRuns_URL = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_GroupName}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelineruns/{input_id}?api-version=2018-06-01"
    
    # Set headers with access token
    headers = {"Authorization": "Bearer " + access_token}
    
    # Send GET request to Azure API and load data into JSON object
    response = requests.get(pipelineRuns_URL, headers=headers)
    parameters_data = json.loads(response.text)
    
    # Initialize variables for parent ID and current ID
    parent_id = None
    current_id = None
    parent_name = None
    
    # Loop through JSON object and check for activity that invoked the current pipeline run
    for key, value in parameters_data.items():
        if key == "invokedBy" and value["invokedByType"] != "Manual":
            current_id = value['pipelineRunId']
            parent_id = current_id
            
            # Recursively call function to find top-level parent ID
            parent_id = get_parent_id_recursive(current_id)
            
            # Get the name of the parent pipeline run
            if parent_id is not None:
                response = requests.get(f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_GroupName}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelineruns/{parent_id}?api-version=2018-06-01")
                parameters_data = json.loads(response.text)
                parent_name = parameters_data['name']
            break
        
    # If no parent activity is found, return the input ID as the parent ID
    else:
        parent_id = input_id
        parent_name = "None"
        
    return parent_id, parent_name




















import requests
import json

def get_parent_id_recursive(input_id):
    # Construct request URL using input parameters
    pipelineRuns_URL = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_GroupName}/providers/Microsoft.DataFactory/factories/{factory_name}/pipelineruns/{input_id}?api-version=2018-06-01"
    
    # Set headers with access token
    headers = {"Authorization": "Bearer " + access_token}
    
    # Send GET request to Azure API and load data into JSON object
    response = requests.get(pipelineRuns_URL, headers=headers)
    parameters_data = json.loads(response.text)
    
    # Initialize variables for parent ID and parent name
    parent_id = None
    parent_name = None
    
    # Check if the pipeline run was invoked by an activity
    if "invokedBy" in parameters_data:
        invoked_by = parameters_data["invokedBy"]
        if isinstance(invoked_by, list):
            # If there are multiple levels of invocation, iterate through each level
            for level in invoked_by:
                if level["invokedByType"] != "Manual":
                    parent_id = level["pipelineRunId"]
                    parent_id, parent_name = get_parent_id_recursive(parent_id)
                    break
        elif isinstance(invoked_by, dict):
            # If there is only one level of invocation
            if invoked_by["invokedByType"] != "Manual":
                parent_id = invoked_by["pipelineRunId"]
                parent_id, parent_name = get_parent_id_recursive(parent_id)
    
    # If no parent activity is found, return the input ID as the parent ID
    if not parent_id:
        parent_id = input_id
    
    # Retrieve the name of the current pipeline run
    if "name" in parameters_data:
        parent_name = parameters_data["name"]
    
    return parent_id, parent_name





























from concurrent.futures import ThreadPoolExecutor

def process_item(item):
    queryActivity_URI = 'https://management.azure.com/subscriptions/(l/resourceGroups/(l/providers/Microsoft.DataFactory/factories/(/pipelineru'
    headers = {'Authorization': 'Bearer ' + access_token}

    pipeline_runs = get_pipeline_runs(queryActivity_URL, headers)

    if pipeline_runs is not None:
        result = get_activity_output_data(pipeline_runs, item['runId'])

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

        table_exists = spark.catalog.tableExists("structured._meta_copyactivities")

        if table_exists:
            df.createOrReplaceTempView('temp_copyactivities')
            sql_query_upsert = f'MERGE INTO structured._meta_copyactivities AS target USING temp_copyactivities AS source ON target.RunID = source.RunID WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *'
            spark.sql(sql_query_upsert)
        else:
            df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable('structured._meta_copyactivities')

    return item['runId']

if len(run_list) > 0:
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_item, item) for item in run_list]
        completed_tasks = [future.result() for future in futures if future.result() is not None]

    if not completed_tasks:
        raise Exception('get_pipeline_list returned empty')
else:
    raise Exception('get_pipeline_list returned empty')


