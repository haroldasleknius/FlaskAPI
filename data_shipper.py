import requests
from elasticsearch import Elasticsearch
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = os.environ.get("API_URL")
SCHEMA_ENDPOINT = f"{API_URL}/schemas"
GEN_ENDPOINT = f"{API_URL}/generate-documents"

SCHEMA_NAME = "Esports"
COUNT = int(os.environ.get("COUNT"))
INTERVAL = int(os.environ.get("INTERVAL"))

ES_URL = os.environ.get("ES_URL")
ES_USER = "elastic"
ES_PASS = os.environ.get("ES_PASS")
ES_INDEX = "pro_players"
ES_VERIFY_CERTS = False
ES = Elasticsearch(
    ES_URL,
    basic_auth=(ES_USER, ES_PASS),
    verify_certs=ES_VERIFY_CERTS,
    ssl_show_warn=not ES_VERIFY_CERTS,
    request_timeout=30,
)


def create_schema():
    body = {
        "schema_name": SCHEMA_NAME,
        "fields": {
            "nickname": {"type": "name", "format": "gamertag"},
            "name": {"type": "name", "format": "full"},
            "id": {"type": "integer", "min": 1, "max": 100000},
            "dob": {"type": "dob", "min": 18, "max": 32},
            "country_code": {"type": "country", "format": "alpha2"},
            "game": {"type": "game"},
            "role": {"type": "role"},
            "org": {"type": "org"},
            "trophies": {"type": "trophies", "min": 1, "max": 20, "start_year": 2020},
        },
    }
    try:
        r = requests.post(SCHEMA_ENDPOINT, json=body, timeout=20)
        if r.status_code == 201:
            print("schema created")
        elif r.status_code == 400 and "already been taken" in r.text:
            print("schema already exists")
        else:
            print("schema POST returned", r.status_code, r.text[:200])
    except Exception as e:
        print("ensure_schema error:", e)


def fetch_docs_raw():
    headers = {"Accept": "application/x-ndjson"}
    payload = {"schema_name": SCHEMA_NAME, "count": COUNT}
    r = requests.post(GEN_ENDPOINT, json=payload, headers=headers, timeout=20)
    r.raise_for_status()
    return r.text


def build_bulk_body(doc_ndjson):
    out = []
    for line in doc_ndjson.splitlines():
        line = line.strip()
        if not line:
            continue
        out.append(json.dumps({"index": {"_index": ES_INDEX}}))
        out.append(line)
    return "\n".join(out) + "\n"


def bulk_upload():
    doc_ndjson = fetch_docs_raw()
    body = build_bulk_body(doc_ndjson)
    r = ES.options(
        headers={"Content-Type": "application/x-ndjson"}, request_timeout=30
    ).bulk(operations=body, refresh="wait_for")
    print("bulk done; errors =", r.get("errors"))
    if r.get("errors"):
        print("sample:", r.get("items", [])[:2])


def mapping_index():
    if ES.indices.exists(index=ES_INDEX):
        print(f"index exists: {ES_INDEX}")
        return

    settings = {"number_of_shards": 1, "number_of_replicas": 0}

    mappings = {
        "properties": {
            "id": {"type": "integer"},
            "nickname": {"type": "keyword"},
            "name": {"type": "keyword"},
            "dob": {"type": "date"},
            "country_code": {"type": "keyword"},
            "ip": {"type": "ip"},
            "game": {"type": "keyword"},
            "role": {"type": "keyword"},
            "org": {"type": "keyword"},
            "trophies": {
                "type": "nested",
                "properties": {
                    "tournament": {"type": "keyword"},
                    "placement": {"type": "keyword"},
                },
            },
        }
    }
    ES.indices.create(index=ES_INDEX, settings=settings, mappings=mappings)
    print(f"created index {ES_INDEX}")


def run_intervals():
    while True:
        bulk_upload()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    create_schema()
    mapping_index()
    run_intervals()
