import pytest
from playwright.sync_api import Playwright, APIRequestContext
from typing import Generator
import json


@pytest.fixture(scope="session")
def api_request_context(
    playwright: Playwright,
) -> Generator[APIRequestContext, None, None]:
    request_context = playwright.request.new_context(base_url="http://127.0.0.1:5454")
    yield request_context
    request_context.dispose()


def test_create_schema(api_request_context: APIRequestContext):
    schema = {
        "schema_name": "Haroldas's Generator",
        "fields": {
            "name": {"type": "name", "format": "full"},
            "id": {"type": "integer", "min": 1, "max": 9999},
            "dob": {"type": "dob", "min": 12, "max": 100},
            "ip": {"type": "ip", "version": 4, "visibility": "public"},
            "country_code": {
                "type": "country",
                "format": "alpha2",
                "countries": ["US", "GB", "FR"],
            },
        },
    }
    response = api_request_context.post("/schemas", data=schema)
    assert response.ok, f"Schema creation failed: {response.status}"
    body = response.json()
    assert body["schema_name"] == "Haroldas's Generator"
    assert "fields" in body
    assert "name" in body["fields"]
    assert "id" in body["fields"]
    assert "dob" in body["fields"]
    assert "ip" in body["fields"]
    assert "country_code" in body["fields"]
    assert body["fields"]["name"]["format"] == "full"
    assert body["fields"]["country_code"]["format"] == "alpha2"


def test_create_schema_duplicate(api_request_context: APIRequestContext):
    schema = {"schema_name": "Haroldas's Generator", "fields": {}}
    response = api_request_context.post("/schemas", data=schema)
    assert response.status == 400, "Duplicate schema should return HTTP 400"


def test_generate_documents(api_request_context: APIRequestContext):
    document = {"schema_name": "Haroldas's Generator", "count": 5}
    response = api_request_context.post("/generate-documents", data=document)
    assert response.ok, f"Failed to generate documents: {response.status}"

    body = response.json()
    print(json.dumps(body, indent=2))

    assert isinstance(body, list), "Expected list of json documents"
    assert len(body) == 5, f"Expected 5 documents, got {len(body)}"

    for doc in body:
        assert "name" in doc
        assert "id" in doc
        assert "dob" in doc
        assert "ip" in doc
        assert "country_code" in doc


def test_create_schema_multiple(api_request_context: APIRequestContext):
    schema = {
        "schema_name": "Haroldas's 2nd Generator",
        "fields": {
            "name": {"type": "name", "format": "first"},
            "id": {"type": "integer", "min": 1, "max": 500},
            "dob": {"type": "dob", "min": 5, "max": 25},
            "ip": {"type": "ip", "version": 6},
            "country_code": {
                "type": "country",
                "format": "alpha3",
            },
        },
    }
    response = api_request_context.post("/schemas", data=schema)
    assert response.ok, f"Schema creation failed: {response.status}"
    body = response.json()
    assert body["schema_name"] == "Haroldas's 2nd Generator"
    assert "fields" in body
    assert "name" in body["fields"]
    assert "id" in body["fields"]
    assert "dob" in body["fields"]
    assert "ip" in body["fields"]
    assert "country_code" in body["fields"]
    assert body["fields"]["name"]["format"] == "first"
    assert body["fields"]["country_code"]["format"] == "alpha3"

    schema = {
        "schema_name": "Haroldas's 3rd Generator",
        "fields": {
            "name": {"type": "name", "format": "last"},
            "id": {"type": "integer", "min": 100, "max": 250},
            "dob": {"type": "dob", "min": 25, "max": 45},
            "ip": {"type": "ip", "version": 4, "visibility": "private"},
            "country_code": {
                "type": "country",
                "format": "name",
                "countries": ["US", "GB", "FR"],
            },
        },
    }
    response = api_request_context.post("/schemas", data=schema)
    assert response.ok, f"Schema creation failed: {response.status}"
    body = response.json()
    assert body["schema_name"] == "Haroldas's 3rd Generator"
    assert "fields" in body
    assert "name" in body["fields"]
    assert "id" in body["fields"]
    assert "dob" in body["fields"]
    assert "ip" in body["fields"]
    assert "country_code" in body["fields"]
    assert body["fields"]["name"]["format"] == "last"
    assert body["fields"]["country_code"]["format"] == "name"


def test_generate_documents_multiple(api_request_context: APIRequestContext):
    document = {"schema_name": "Haroldas's 2nd Generator", "count": 20}
    response = api_request_context.post("/generate-documents", data=document)
    assert response.ok, f"Failed to generate documents: {response.status}"

    body = response.json()

    assert isinstance(body, list), "Expected list of json documents"
    assert len(body) == 20, f"Expected 20 documents, got {len(body)}"

    for doc in body:
        assert "name" in doc
        assert "id" in doc
        assert "dob" in doc
        assert "ip" in doc
        assert "country_code" in doc

    document = {"schema_name": "Haroldas's 3rd Generator", "count": 50}
    response = api_request_context.post("/generate-documents", data=document)
    assert response.ok, f"Failed to generate documents: {response.status}"

    body = response.json()

    assert isinstance(body, list), "Expected list of json documents"
    assert len(body) == 50, f"Expected 50 documents, got {len(body)}"

    for doc in body:
        assert "name" in doc
        assert "id" in doc
        assert "dob" in doc
        assert "ip" in doc
        assert "country_code" in doc
