# tests/test_county_data.py
import sys, pathlib
# add repo root to sys.path so `api/` is importable
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

import json
import os
import pytest
from flask import Flask, request

import api.county_data as county_data



@pytest.fixture(scope="session")
def app():
    """
    Minimal Flask app that routes /county_data to the Vercel-style handler().
    """
    # Make local DB connections writable for tests (friendlier errors).
    os.environ["READ_ONLY_DB"] = "0"
    os.environ["DEBUG"] = "1"

    app = Flask(__name__)

    @app.route("/county_data", methods=["POST", "GET"])
    def county_data_route():
        # Delegate to the serverless handler
        return county_data.handler(request)

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
    """
    Happy path: known zip + known measure_name that exist in your DB.
    Assumes your local data.db has rows for 02138 + 'Adult obesity'
    (you confirmed this earlier).
    """
    resp = _post(client, {"zip": "02138", "measure_name": "Adult obesity"})
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)
    assert len(data) >= 1
    row = data[0]
    # Check required keys from county_health_rankings schema
    for k in [
        "State",
        "County",
        "State_code",
        "County_code",
        "Year_span",
        "Measure_name",
        "Measure_id",
        "Numerator",
        "Denominator",
        "Raw_value",
        "Confidence_Interval_Lower_Bound",
        "Confidence_Interval_Upper_Bound",
        "Data_Release_Year",
        "fipscode",
    ]:
        assert k in row


def test_teapot_418(client):
    resp = _post(
        client, {"zip": "02138", "measure_name": "Adult obesity", "coffee": "teapot"}
    )
    assert resp.status_code == 418
    data = resp.get_json()
    # Shape can vary; just ensure JSON body returned
    assert isinstance(data, dict)


def test_bad_request_400_missing_keys(client):
    # Missing both keys
    resp = _post(client, {})
    assert resp.status_code == 400
    data = resp.get_json()
    assert "error" in data

    # Bad zip format
    resp = _post(client, {"zip": "2138", "measure_name": "Adult obesity"})
    assert resp.status_code == 400

    # Invalid measure value (case matters)
    resp = _post(client, {"zip": "02138", "measure_name": "Adult Obesity"})
    assert resp.status_code == 400


def test_not_found_404_no_match(client):
    # Use a zip that's unlikely to exist in your zip_county table
    resp = _post(client, {"zip": "99999", "measure_name": "Adult obesity"})
    assert resp.status_code == 404
    data = resp.get_json()
    assert "error" in data


def test_method_not_allowed_405_on_get(client):
    resp = client.get("/county_data")
    assert resp.status_code == 405
