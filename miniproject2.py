from flask import Flask, request, jsonify, Response
import json
import time
import pymysql

from db import DB
from generators import make_document, process_fields, ALLOWED_TYPES
from logsetup import setup_logging, get_logger

setup_logging()
log = get_logger(__name__)

app = Flask(__name__)

db = DB()
db.init_schema()


def fetch_schema_by_name(schema_name):  # pragma: no cover
    row = db.query_one("SELECT fields FROM schemas WHERE name=%s", (schema_name,))
    if row:
        return json.loads(row["fields"])
    else:
        return None


def insert_schema(schema_name, field_map):  # pragma: no cover
    try:
        db.execute(
            "INSERT INTO schemas (name, fields) VALUES (%s, %s)",
            (schema_name, json.dumps(field_map)),
        )
        return True
    except pymysql.err.IntegrityError:
        return False


def extract_schema_field_and_count(data):  # pragma: no cover
    if not data or "schema_name" not in data:
        return jsonify(Error="schema_name is required"), 400

    if "count" not in data:
        return jsonify(Error="Count is required"), 400

    schema_name = data["schema_name"]
    count = data["count"]

    if not isinstance(schema_name, str) or not schema_name.strip():
        return jsonify(Error="schema_name must be a non_empty string"), 400

    schema_fields = fetch_schema_by_name(schema_name)
    if not schema_fields:
        return jsonify(Error="schema not found", schema_name=schema_name), 404

    try:
        count = int(count)
    except (TypeError, ValueError):
        return jsonify(Error="Count must be an integer that's greater than 0"), 400

    if count < 1:
        return jsonify(Error="Count must be greater than 0"), 400

    return schema_fields, count


def extract_schema_name_and_fields(data):  # pragma: no cover
    if not data or "schema_name" not in data:
        return jsonify(Error="schema_name is required"), 400

    schema_name = data["schema_name"]

    if not isinstance(schema_name, str) or not schema_name.strip():
        return jsonify(Error="schema_name must be a non_empty string"), 400

    if "fields" not in data:
        return jsonify(Error="fields are required"), 400

    fields = data["fields"]
    if not isinstance(fields, dict) or not fields:
        return jsonify(Error="fields must be a non-empty dict"), 400

    if fetch_schema_by_name(schema_name):
        return jsonify(
            Error="Schema name has already been taken", schema_name=schema_name
        ), 400

    field_map, bad_types = process_fields(fields)
    if bad_types:
        return jsonify(
            Allowed_types=list(ALLOWED_TYPES),
            Unknown_types=bad_types,
            Error="unknown data types",
        ), 400

    return schema_name, field_map


@app.post("/schemas")  # pragma: no cover
def create_schema():  # pragma: no cover
    """
    Create a schema.
    """
    time_start = time.monotonic()
    try:
        data = request.get_json()
        result = extract_schema_name_and_fields(data)

        if isinstance(result[0], Response):
            status = result[1] if len(result) > 1 else 400
            time_diff = (time.monotonic() - time_start) * 1000
            log.warning(
                "action=schema.create component=api outcome=validation_error status=%s duration_ms=%.1f",
                status,
                time_diff,
            )
            return result

        schema_name, field_map = result
        created = insert_schema(schema_name, field_map)

        time_diff = (time.monotonic() - time_start) * 1000
        if not created:
            log.info(
                "action=schema.create component=api outcome=already_exists status=400 schema=%s duration_ms=%.1f",
                schema_name,
                time_diff,
            )
            return jsonify(
                Error="Schema name has already been taken", schema_name=schema_name
            ), 400

        log.info(
            "action=schema.create component=api outcome=success status=201 schema=%s duration_ms=%.1f",
            schema_name,
            time_diff,
        )
        return jsonify(schema_name=schema_name, fields=field_map), 201

    except Exception:
        time_diff = (time.monotonic() - time_start) * 1000
        log.exception(
            "action=schema.create component=api outcome=error duration_ms=%.1f",
            time_diff,
        )
        raise


@app.post("/generate-documents")  # pragma: no cover
def generate_documents():  # pragma: no cover
    """
    Generate documents for a schema.
    """
    time_start = time.monotonic()
    try:
        data = request.get_json()
        result = extract_schema_field_and_count(data)

        if isinstance(result[0], Response):
            status = result[1] if len(result) > 1 else 400
            time_diff = (time.monotonic() - time_start) * 1000
            log.warning(
                "action=docs.generate component=api outcome=validation_error status=%s duration_ms=%.1f",
                status,
                time_diff,
            )
            return result

        schema_fields, count = result
        schema_name = data.get("schema_name")
        accept = request.headers.get("Accept", "application/json")

        if accept == "application/x-ndjson":
            lines = [json.dumps(make_document(schema_fields)) for _ in range(count)]
            time_diff = (time.monotonic() - time_start) * 1000
            log.info(
                "action=docs.generate component=api outcome=success status=200 schema=%s count=%s mime=ndjson duration_ms=%.1f",
                schema_name,
                count,
                time_diff,
            )
            return Response(
                "\n".join(lines) + "\n", mimetype="application/x-ndjson", status=200
            )
        else:
            documents = [make_document(schema_fields) for _ in range(count)]
            time_diff = (time.monotonic() - time_start) * 1000
            log.info(
                "action=docs.generate component=api outcome=success status=200 schema=%s count=%s mime=json duration_ms=%.1f",
                schema_name,
                count,
                time_diff,
            )
            return jsonify(documents), 200

    except Exception:
        time_diff = (time.monotonic() - time_start) * 1000
        log.exception(
            "action=docs.generate component=api outcome=error duration_ms=%.1f",
            time_diff,
        )
        raise
