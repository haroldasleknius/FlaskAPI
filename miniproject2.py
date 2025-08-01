from flask import Flask, request, jsonify, Response
from faker import Faker
import json
import random
import iso3166
from db import DB
import pymysql

app = Flask(__name__)

db = DB()
db.init_schema()

faker = Faker("en_GB")
ALLOWED_TYPES = {
    "integer",
    "name",
    "dob",
    "country",
    "ip",
}


def fetch_schema_by_name(schema_name):
    row = db.query_one("SELECT `fields` FROM `schemas` WHERE name=%s", (schema_name,))
    if row:
        return json.loads(row["fields"])
    else:
        return None


def insert_schema(schema_name, field_map):
    try:
        db.execute(
            "INSERT INTO `schemas` (`name`, `fields`) VALUES (%s, %s)",
            (schema_name, json.dumps(field_map)),
        )
        return True
    except pymysql.err.IntegrityError:
        return False


def generate_integer(value):
    low = int(value.get("min", 1))
    high = int(value.get("max", 50000))
    return random.randint(low, high)


def generate_name(value):
    name_format = value.get("format", "full")
    match name_format:
        case "first":
            return faker.first_name()
        case "last":
            return faker.last_name()
        case "full":
            return f"{faker.first_name()} {faker.last_name()}"
        case _:
            raise ValueError("invalid name format entered")


def generate_dob(value):
    min_age = int(value.get("min", 1))
    max_age = int(value.get("max", 100))
    dob = faker.date_of_birth(minimum_age=min_age, maximum_age=max_age)
    return dob.isoformat()


def generate_ip(value):
    version = value.get("version", 4)
    visibility = str(value.get("visibility", None)).lower()

    match (version, visibility):
        case (4, "public"):
            return faker.ipv4(private=False)
        case (4, "private"):
            return faker.ipv4(private=True)
        case (6, _):
            return faker.ipv6()
        case (4, _):
            return faker.ipv4()
        case _:
            raise ValueError("ip version must be 4 or 6")


def generate_country(value):
    ### alpha2 = US, alpha3 = USA, name = United States
    country_format = value.get("format", "alpha2")
    countries = value.get("countries", None)

    if countries is not None:
        option = random.choice(countries)
        option = option.upper()
    else:
        option = faker.country_code()

    country = iso3166.countries.get(option)

    match country_format:
        case "alpha2":
            return option
        case "alpha3":
            return country.alpha3
        case "name":
            return country.name
        case _:
            raise ValueError("unsupported country format")


def make_document(key_pairs):
    """
    {
      "schema_name": "Haroldas's Generator",
      "fields": {
        "name": {"type": "name", "format": "full"},
        "id": {"type": "integer","min": 1,"max": 9999},
        "dob": {"type": "dob","min": 12,"max": 100},
        "ip": {"type": "ip","version": 4,"visibility": "public"},
        "country_code": {"type":"country", "format":"alpha2", "countries":["US","GB","FR"]}
      }
    }
    """

    document = {}
    for field_name, value in key_pairs.items():
        data_type = value["type"]

        match data_type:
            case "integer":
                document[field_name] = generate_integer(value)
            case "name":
                document[field_name] = generate_name(value)
            case "dob":
                document[field_name] = generate_dob(value)
            case "ip":
                document[field_name] = generate_ip(value)
            case "country":
                document[field_name] = generate_country(value)
            case _:
                raise ValueError(f"Unsupported type: {data_type}")

    return document


def process_fields(fields):
    field_map = {}
    bad_types = []
    for field_name, value in fields.items():
        if isinstance(value, str):
            value = {"type": value}

        data_type = value["type"]
        if data_type not in ALLOWED_TYPES:
            bad_types.append(data_type)
            continue

        field_map[field_name] = value

    return field_map, bad_types


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
    {
      "schema_name": "Haroldas's Generator",
      "fields": {
        "name": {"type": "name", "format": "full"},
        "id": {"type": "integer","min": 1,"max": 9999},
        "dob": {"type": "dob","min": 12,"max": 100},
        "ip": {"type": "ip","version": 4,"visibility": "public"},
        "country_code": {"type":"country", "format":"alpha2", "countries":["US","GB","FR"]}
      }
    }
    """
    data = request.get_json()

    result = extract_schema_name_and_fields(data)

    if isinstance(result[0], Response):
        return result

    schema_name, field_map = result

    schema = insert_schema(schema_name, field_map)
    if not schema:
        return jsonify(
            Error="Schema name has already been taken", schema_name=schema_name
        ), 400

    return jsonify(schema_name=schema_name, fields=field_map), 201


@app.post("/generate-documents")  # pragma: no cover
def generate_documents():  # pragma: no cover
    """
    {
      "schema_name": "Haroldas's Generator",
      "count": 5
    }
    """
    data = request.get_json()
    result = extract_schema_field_and_count(data)

    if isinstance(result[0], Response):
        return result

    schema_fields, count = result

    accept = request.headers.get("Accept", "application/json")

    if accept == "application/x-ndjson":
        lines = []
        for _ in range(count):
            lines.append(json.dumps(make_document(schema_fields)))
        return Response(
            "\n".join(lines) + "\n", mimetype="application/x-ndjson", status=200
        )
    else:
        documents = []
        for _ in range(count):
            documents.append(make_document(schema_fields))

    return jsonify(documents), 200
