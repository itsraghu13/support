import requests
import re

def fetch_pipeline_runs(url, headers, body=None):
    response = requests.post(url, headers=headers, json=body, stream=True)
    response.raise_for_status()
    return response.json()

def get_token():
    # Implement your logic to generate a new token here
    # This may involve making a request to your authentication endpoint
    new_token = 'your_new_access_token'
    return new_token

def filter_pipeline_runs(pipeline_runs, last_updated_after, last_updated_before):
    # Your existing filter function here...

def get_all_pipeline_runs(subscription_id, resource_group_name, factory_name, last_updated_after, last_updated_before, num_threads=1):
    queryPipelineRuns_URL = f"https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers/Microsoft.DataFactory/factories/{factory_name}/queryPipelineRuns?api-version=2018-06-01"
    data = []
    continuation_token = None

    while True:
        # Request a new token for each pagination
        access_token = get_token()
        
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-type': 'application/json'
        }

        request_body = {
            "lastUpdatedAfter": last_updated_after,
            "lastUpdatedBefore": last_updated_before,
            "continuationToken": continuation_token,
            "filters": [
                {
                    "operand": "RunStart",
                    "operator": "GreaterThan",
                    "values": [last_updated_after]
                },
                {
                    "operand": "RunEnd",
                    "operator": "LessThan",
                    "values": [last_updated_before]
                }
            ]
        }

        response = fetch_pipeline_runs(queryPipelineRuns_URL, headers, request_body)
        pipeline_runs = response.get('value', [])
        filtered_runs = filter_pipeline_runs(pipeline_runs, last_updated_after, last_updated_before)
        data.extend(filtered_runs)
        continuation_token = get_token(response.get('continuationToken', None))
        if not continuation_token:
            break

    return data

# Example usage:
pipeline_runs_data = get_all_pipeline_runs(subscription_id, resource_group_name, factory_name, last_updated_after, last_updated_before)
