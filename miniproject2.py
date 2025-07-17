from flask import Flask, request, jsonify
from faker import Faker
import random
from random import randint
import string

app = Flask(__name__)

SCHEMAS = {}
faker = Faker("en_GB")
ALLOWED_TYPES = {"integer", "string", "dob", "ip"}


def generate_integer(option):
    if option is None:
        return randint(1, 50000)

    if isinstance(option, int):
        return randint(1, option)

    option = str(option)

    if "-" in option:
        low, high = option.split("-", 1)
        return randint(int(low), int(high))

    return randint(1, int(option))


def generate_string(option):
    if option == "firstname":
        return faker.first_name()
    if option == "lastname":
        return faker.last_name()
    if option == "fullname":
        return f"{faker.first_name()} {faker.last_name()}"
    return "".join(random.choice(string.ascii_letters) for i in range(20))


def generate_dob(option):
    if option is None:
        dob = faker.date_of_birth(minimum_age=12, maximum_age=100)
        return dob.isoformat()

    if isinstance(option, int):
        dob = faker.date_of_birth(minimum_age=1, maximum_age=option)
        return dob.isoformat()

    option = str(option)

    if "-" in option:
        low, high = option.split("-", 1)
        dob = faker.date_of_birth(minimum_age=int(low), maximum_age=int(high))
        return dob.isoformat()

    dob = faker.date_of_birth(minimum_age=12, maximum_age=100)
    return dob.isoformat()


def generate_ip():
    return faker.ipv4()


def make_document(key_pairs):
    """
    {
      "schema_name": "Haroldas's Generator",
      "fields": {
        "name": {"type":"string", "option":"fullname"},
        "id": {"type":"integer","option":10000},
        "dob": {"type":"dob","option":"12-30"},
        "ip":   "ip"
      }
    }
    """

    document = {}
    for field_name, value in key_pairs.items():
        data_type = value["type"]
        option = value["option"]

        match data_type:
            case "integer":
                document[field_name] = generate_integer(option)
            case "string":
                document[field_name] = generate_string(option)
            case "dob":
                document[field_name] = generate_dob(option)
            case "ip":
                document[field_name] = generate_ip()
            case _:
                raise ValueError(f"Unsupported type: {data_type}")

    return document


@app.post("/schemas")
def create_schema():
    """
    {
      "schema_name": "Haroldas's Generator",
      "fields": {
        "name": {"type":"string", "option":"fullname"},
        "id": {"type":"integer","option":10000},
        "dob": {"type":"dob","option":"12-30"},
        "ip":   "ip"
      }
    }
    """
    data = request.get_json()

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

    if schema_name in SCHEMAS:
        return jsonify(
            Error="Schema name has already been taken", schema_name=schema_name
        ), 400

    field_map = {}
    bad_types = []
    for field_name, value in data["fields"].items():
        if isinstance(value, str):
            data_type = value
            option = None
        elif isinstance(value, dict):
            data_type = value["type"]
            option = value["option"]

        if data_type not in ALLOWED_TYPES:
            bad_types.append(data_type)
            continue

        field_map[field_name] = {"type": data_type, "option": option}

    if bad_types:
        return jsonify(
            Allowed_types=list(ALLOWED_TYPES),
            Unknown_types=bad_types,
            Error="unknown data types",
        ), 400

    SCHEMAS[schema_name] = field_map

    return jsonify(schema_name=schema_name, fields=field_map), 201


@app.post("/generate-documents")
def generate_documents():
    """
    {
      "schema_name": "Haroldas's Generator",
      "count": 5
    }
    """
    data = request.get_json()

    if not data or "schema_name" not in data:
        return jsonify(Error="schema_name is required"), 400

    if "count" not in data:
        return jsonify(Error="Count is required"), 400

    schema_name = data["schema_name"]
    count = data["count"]

    if not schema_name:
        return jsonify(Error="schema_name is missing"), 400

    if schema_name not in SCHEMAS:
        return jsonify(Error="schema not found", schema_name=schema_name), 404

    try:
        count = int(count)
    except (TypeError, ValueError):
        return jsonify(Error="Count must be an integer that's greater than 1"), 400

    if count < 1:
        return jsonify(Error="Count must be greater than 0"), 400

    schema = SCHEMAS[schema_name]

    documents = []
    for i in range(count):
        documents.append(make_document(schema))

    return jsonify(documents), 200
