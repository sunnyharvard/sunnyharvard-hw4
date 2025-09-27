# tests/test_county_data.py
import sys, pathlib
from flask import Flask, request, Response, Request

sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import json
import os
import pytest
import api.county_data as county_data


@pytest.fixture(scope="session")
def app():
    os.environ["READ_ONLY_DB"] = "0"
    os.environ["DEBUG"] = "1"

    app = Flask(__name__)

    # Bind county_data *now* so NameError can't occur later
    @app.route("/county_data", methods=["POST", "GET"])
    def county_data_route(_mod=county_data):
        if hasattr(_mod, "handler"):
            return _mod.handler(request)
        # If you switched to a pure Flask app in api/county_data.py exposing `app`
        # and a view function named "county_data_route", fall back to it:
        return _mod.app.view_functions["county_data_route"]()

    return app


@pytest.fixture()
def client(app):
    return app.test_client()


def _post(client, payload):
    return client.post(
        "/county_data",
        data=json.dumps(payload),
        headers={"Content-Type": "application/json"},
    )


def test_county_data_success_200(client):
    resp = _post(client, {"zip": "02138", "measure_name": "Adult obesity"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list) and len(data) >= 1
    row = data[0]
    for k in [
        "State", "County", "State_code", "County_code", "Year_span",
        "Measure_name", "Measure_id", "Numerator", "Denominator",
        "Raw_value", "Confidence_Interval_Lower_Bound",
        "Confidence_Interval_Upper_Bound", "Data_Release_Year", "fipscode",
    ]:
        assert k in row


def test_teapot_418(client):
    resp = _post(client, {"zip": "02138", "measure_name": "Adult obesity", "coffee": "teapot"})
    assert resp.status_code == 418
    assert isinstance(resp.get_json(), dict)


def test_bad_request_400_missing_keys(client):
    resp = _post(client, {})
    assert resp.status_code == 400
    assert "error" in resp.get_json()

    resp = _post(client, {"zip": "2138", "measure_name": "Adult obesity"})
    assert resp.status_code == 400

    resp = _post(client, {"zip": "02138", "measure_name": "Adult Obesity"})
    assert resp.status_code == 400


def test_not_found_404_no_match(client):
    resp = _post(client, {"zip": "99999", "measure_name": "Adult obesity"})
    assert resp.status_code == 404
    assert "error" in resp.get_json()


def test_method_not_allowed_405_on_get(client):
    resp = client.get("/county_data")
    assert resp.status_code == 405
