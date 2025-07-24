import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from miniproject2 import (
    generate_integer,
    generate_name,
    generate_dob,
    generate_ip,
    generate_country,
)
from datetime import datetime, date
import ipaddress
import iso3166
import pytest


def test_generate_integer():
    test_data = generate_integer({"min": 1, "max": 20})
    assert 1 <= test_data <= 20


def test_generate_name_full():
    test_data = generate_name({"format": "full"})
    assert isinstance(test_data, str)


def test_generate_name_first():
    test_data = generate_name({"format": "first"})
    assert isinstance(test_data, str)


def test_generate_name_last():
    test_data = generate_name({"format": "last"})
    assert isinstance(test_data, str)


def test_generate_name_invalid_format():
    with pytest.raises(ValueError) as error:
        generate_name({"format": "lwdasdasdasd"})

    assert str(error.value) == "invalid name format entered"


def test_generate_dob_format():
    dob = generate_dob({"min": 5, "max": 15})
    try:
        dob = datetime.strptime(dob, "%Y-%m-%d")
    except ValueError:
        pytest.fail("DOB is not in ISO format YYYY-MM-DD")


def test_generate_dob_age_range():
    dob = generate_dob({"min": 5, "max": 15})
    dob = datetime.strptime(dob, "%Y-%m-%d").date()
    today = date.today()
    age = today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))
    assert 5 <= age <= 15


def test_generate_ip_v4():
    test_data = generate_ip({"version": 4})
    try:
        ip = ipaddress.IPv4Address(test_data)
    except ipaddress.AddressValueError:
        pytest.fail("outputted data is not a valid ipv4 address")
    assert isinstance(ip, ipaddress.IPv4Address)


def test_generate_ip_v4_public():
    test_data = generate_ip({"version": 4, "visibility": "public"})
    try:
        ip = ipaddress.IPv4Address(test_data)
    except ipaddress.AddressValueError:
        pytest.fail("outputted data is not a valid ipv4 address")

    assert not ip.is_private, "outputted ip is a private IP"


def test_generate_ip_v4_private():
    test_data = generate_ip({"version": 4, "visibility": "private"})
    try:
        ip = ipaddress.IPv4Address(test_data)
    except ipaddress.AddressValueError:
        pytest.fail("outputted data is not a valid ipv4 address")

    assert ip.is_private, "outputted ip is a public IP"


def test_generate_ip_v6():
    test_data = generate_ip({"version": 6})
    try:
        ip = ipaddress.IPv6Address(test_data)
    except ipaddress.AddressValueError:
        pytest.fail("outputted data is not a valid ipv6 address")

    assert isinstance(ip, ipaddress.IPv6Address)


def test_generate_ip_invalid_version():
    with pytest.raises(ValueError) as error:
        generate_ip({"version": "1"})

    assert str(error.value) == "ip version must be 4 or 6"


def test_generate_country_alpha2():
    test_data = generate_country({"format": "alpha2"})
    assert isinstance(test_data, str)
    assert len(test_data) == 2
    assert test_data in iso3166.countries


def test_generate_country_alpha2_with_list():
    test_data = generate_country({"format": "alpha2", "countries": ["US", "GB", "FR"]})
    assert isinstance(test_data, str)
    assert len(test_data) == 2
    assert test_data in {"US", "GB", "FR"}


def test_generate_country_alpha3_with_list():
    test_data = generate_country({"format": "alpha3", "countries": ["US", "GB", "FR"]})
    assert isinstance(test_data, str)
    assert len(test_data) == 3
    assert test_data in {"USA", "GBR", "FRA"}


def test_generate_country_name_with_list():
    test_data = generate_country({"format": "name", "countries": ["US", "GB", "FR"]})
    assert isinstance(test_data, str)
    assert test_data in {
        "United States of America",
        "United Kingdom of Great Britain and Northern Ireland",
        "France",
    }


def test_generate_country_invalid_format():
    with pytest.raises(ValueError) as error:
        generate_country({"format": "wawadsdwasd"})

    assert str(error.value) == "unsupported country format"


###not finished
# def test_make_document_with_name():
# schema = {"name": {"type": "name", "format": "full"}}
# test_data = make_document(schema)
# return test_data
