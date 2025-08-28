import requests
from elasticsearch import Elasticsearch
import json
import time
import os
from dotenv import load_dotenv
from logsetup import setup_logging, get_logger

load_dotenv()
setup_logging()
log = get_logger(__name__)

API_URL = os.environ.get("API_URL")
SCHEMA_ENDPOINT = f"{API_URL}/schemas"
GEN_ENDPOINT = f"{API_URL}/generate-documents"

SCHEMA_NAME = "Esports"
COUNT = int(os.environ.get("COUNT", "100"))
INTERVAL = int(os.environ.get("INTERVAL", "10"))

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
    time_start = time.monotonic()
    try:
        r = requests.post(SCHEMA_ENDPOINT, json=body, timeout=20)
        time_diff = (time.monotonic() - time_start) * 1000
        if r.status_code == 201:
            log.info(
                "action=schema.create outcome=success status=%s duration_ms=%.1f",
                r.status_code,
                time_diff,
            )
        elif r.status_code == 400 and "already been taken" in r.text:
            log.info(
                "action=schema.create outcome=already_exists status=%s duration_ms=%.1f",
                r.status_code,
                time_diff,
            )
        else:
            log.warning(
                "action=schema.create outcome=unexpected_response status=%s duration_ms=%.1f body_sample=%s",
                r.status_code,
                time_diff,
                r.text[:200].replace("\n", " "),
            )
    except Exception:
        time_diff = (time.monotonic() - time_start) * 1000
        log.exception("action=schema.create outcome=error duration_ms=%.1f", time_diff)


def fetch_docs_raw():
    headers = {"Accept": "application/x-ndjson"}
    payload = {"schema_name": SCHEMA_NAME, "count": COUNT}
    time_start = time.monotonic()
    try:
        log.debug(
            "action=docs.fetch request=POST endpoint=%s count=%s", GEN_ENDPOINT, COUNT
        )
        r = requests.post(GEN_ENDPOINT, json=payload, headers=headers, timeout=20)
        r.raise_for_status()
        time_diff = (time.monotonic() - time_start) * 1000
        log.info(
            "action=docs.fetch outcome=success status=%s duration_ms=%.1f",
            r.status_code,
            time_diff,
        )
        return r.text
    except Exception:
        time_diff = (time.monotonic() - time_start) * 1000
        log.exception("action=docs.fetch outcome=error duration_ms=%.1f", time_diff)
        raise


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
    time_start = time.monotonic()
    try:
        doc_ndjson = fetch_docs_raw()
        body = build_bulk_body(doc_ndjson)

        res = ES.options(
            headers={"Content-Type": "application/x-ndjson"},
            request_timeout=30,
        ).bulk(operations=body, refresh="wait_for")

        time_diff = (time.monotonic() - time_start) * 1000
        has_errors = bool(res.get("errors"))

        log.info(
            "action=bulk.upload outcome=%s errors=%s duration_ms=%.1f",
            "success" if not has_errors else "error",
            has_errors,
            time_diff,
        )
        if has_errors:
            log.error(
                "action=bulk.upload error_sample=%s",
                json.dumps(res.get("items", [])[:2])[:300],
            )
    except Exception:
        time_diff = (time.monotonic() - time_start) * 1000
        log.exception("action=bulk.upload outcome=error duration_ms=%.1f", time_diff)


def mapping_index():
    time_start = time.monotonic()
    try:
        if ES.indices.exists(index=ES_INDEX):
            log.info("action=index.mapping outcome=exists index=%s", ES_INDEX)
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
        time_diff = (time.monotonic() - time_start) * 1000
        log.info(
            "action=index.mapping outcome=created index=%s duration_ms=%.1f",
            ES_INDEX,
            time_diff,
        )
    except Exception:
        time_diff = (time.monotonic() - time_start) * 1000
        log.exception(
            "action=index.mapping outcome=error index=%s duration_ms=%.1f",
            ES_INDEX,
            time_diff,
        )


def run_intervals():
    log.info("action=runner.start interval_s=%s index=%s", INTERVAL, ES_INDEX)
    while True:
        bulk_upload()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    create_schema()
    mapping_index()
    run_intervals()
