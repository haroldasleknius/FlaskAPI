import pytest
from playwright.sync_api import Playwright, APIRequestContext
from typing import Generator
import json


@pytest.fixture(scope="session")
def api_request_context(
    playwright: Playwright,
) -> Generator[APIRequestContext, None, None]:
    request_context = playwright.request.new_context(base_url="http://127.0.0.1:5000")
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
