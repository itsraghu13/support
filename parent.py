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

