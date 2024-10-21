from cloudevents.http import CloudEvent
from dotenv import load_dotenv
import functions_framework
import json
import os 

# google
import google.auth
from googleapiclient.discovery import build 

load_dotenv()

TEMPLATE_PATH=os.getenv("TEMPLATE_PATH")
REGION=os.getenv("REGION")
GCS_BUCKET=os.getenv("GCS_BUCKET")

print("TEMPLATE_PATH is ->", TEMPLATE_PATH)

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def trigger_event(cloud_event: CloudEvent) -> tuple:
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
    job_name = 'calculate'
    template_path = TEMPLATE_PATH
    region = REGION
    job_request = {
        "jobName": job_name,
        "environment": {
            "tempLocation": f"{GCS_BUCKET}/temp",
            "zone": 'asia-east1-a',  # Ensure this matches the region of your template
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
