import os

import requests
from dotenv import load_dotenv

load_dotenv()

CRONICLE_URL = os.getenv("CRONICLE_URL")
CRONICLE_API_KEY = os.getenv("CRONICLE_API_KEY")


def create_event(title: str, category: str, plugin: str, target: str, enabled: int = 0, **kwargs):
    params = {
        "api_key": CRONICLE_API_KEY,
        "title": title,
        "enabled": enabled,
        "category": category,
        "plugin": plugin,
        "target": target,
        **kwargs,
    }
    create_event_url = os.path.join(CRONICLE_URL, "api/app/create_event/v1")
    response = requests.post(create_event_url, json=params)
    return response.json()


def run_event(event_id: str, **kwargs):
    params = {"api_key": CRONICLE_API_KEY, "id": event_id, **kwargs}
    run_event_url = os.path.join(CRONICLE_URL, "api/app/run_event/v1")
    response = requests.post(run_event_url, json=params)
    return response.json()


def get_job_status(job_id: str, **kwargs):
    params = {"api_key": CRONICLE_API_KEY, "id": job_id, **kwargs}
    job_status_url = os.path.join(CRONICLE_URL, "api/app/get_job_status/v1")
    response = requests.get(job_status_url, json=params)
    return response.json()


def delete_event(event_id: str):
    params = {"api_key": CRONICLE_API_KEY, "id": event_id}
    delete_event_url = os.path.join(CRONICLE_URL, "api/app/delete_event/v1", json=params)
    response = requests.post(delete_event_url, json=params)
    return response
