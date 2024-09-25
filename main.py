from cloudevents.http import CloudEvent
import functions_framework
import json

# google
import google.auth
from googleapiclient.discovery import build 

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event: CloudEvent) -> tuple:
    """This function is triggered by a change in a storage bucket.

    Args:
        cloud_event: The CloudEvent that triggered this function.
    Returns:
        The event ID, event type, bucket, name, metageneration, and timeCreated.
    """
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Event ID: {event_id}")
    print(f"Event type: {event_type}")
    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")
    
    # Trigger DataFlow DataPipeLine
    credentials, project_id = google.auth.default()
    dataflow = build('dataflow', 'v1b3', credentials=credentials)
    job_name = 'pipeline'
    template_path = 'gs://ann-billing/dataflow/data_pipeline'
    region = 'asia-east1'
    # parameters = {
    #     'inputFile': "gs://ann-billing/staging/billing_report.csv",
    #     'output': "gs://ann-billing/dataflow/my_output"
    # }
    job_request = {
        "jobName": job_name,
        # "parameters": parameters,
        "environment": {
            "tempLocation": "gs://ann-billing/temp",
            "zone": "asia-east1-a"
        }
    }
    # call kubernetes service
    result = dataflow.projects().locations().templates().launch(
        projectId=project_id,
        location=region,
        gcsPath=template_path,
        body=job_request
    ).execute()
    print(f"Dataflow job triggered: {json.dumps(result, indent=2)}")
    return event_id, event_type, bucket, name, metageneration, timeCreated, updated
