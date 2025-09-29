# tests/test_county_data.py
import json
import os
import sys
import urllib.request
import urllib.error
import urllib.parse
import pytest
from pathlib import Path
from flask import Response  # used in the local fallback wrapper

# Make sure repo root is importable
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# If set, hit the deployed service over HTTP (integration mode)
RENDER_BASE_URL = os.getenv("https://sunnyharvard-hw4.onrender.com")


# -------------------- HTTP helpers (work for local or remote) --------------------
def _http_post(client_or_base, path, payload):
    if RENDER_BASE_URL:
        url = RENDER_BASE_URL.rstrip("/") + path
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST",
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.getcode()
                body = json.loads(resp.read().decode("utf-8") or "null")
                return status, body
        except urllib.error.HTTPError as e:
            status = e.code
            raw = e.read().decode("utf-8")
            try:
                body = json.loads(raw)
            except Exception:
                body = {"_raw": raw}
            return status, body
    else:
        resp = client_or_base.post(path, data=json.dumps(payload),
                                   headers={"Content-Type": "application/json"})
        # Flask resp.get_json() handles JSON; errors will be JSON per app
        return resp.status_code, (resp.get_json(silent=True) or resp.data.decode("utf-8"))

def _http_get(client_or_base, path):
    if RENDER_BASE_URL:
        url = RENDER_BASE_URL.rstrip("/") + path
        req = urllib.request.Request(url, method="GET")
        try:
            with urllib.request.urlopen(req) as resp:
                status = resp.getcode()
                body = json.loads(resp.read().decode("utf-8") or "null")
                return status, body
        except urllib.error.HTTPError as e:
            status = e.code
            raw = e.read().decode("utf-8")
            try:
                body = json.loads(raw)
            except Exception:
                body = {"_raw": raw}
            return status, body
    else:
        resp = client_or_base.get(path)
        try:
            body = resp.get_json()
        except Exception:
            body = resp.data.decode("utf-8")
        return resp.status_code, body


# -------------------- Local Flask app loader (unit mode) --------------------
def _load_local_flask_app():
    """
    Try importing Flask app in this order:
      1) from app import app            (your Render layout)
      2) from api.county_data import app
      3) from api.county_data import handler, and wrap it in a tiny Flask app
    """
    # 1) app.py at repo root
    try:
        from app import app as flask_app  # type: ignore
        return flask_app
    except Exception:
        pass

    # 2) api/county_data.py exporting app
    try:
        import api.county_data as cd  # type: ignore
        if hasattr(cd, "app"):
            return cd.app
        # 3) Wrap cd.handler in a small Flask app
        from flask import Flask, request
        if not hasattr(cd, "handler"):
            raise ImportError("api.county_data has neither 'app' nor 'handler'")
        wrapper = Flask(__name__)

        @wrapper.get("/")
        def _health():
            return {"ok": True}

        @wrapper.route("/county_data", methods=["POST", "GET"])
        def _route():
            if request.method == "GET":
                return Response('{"error":"Method Not Allowed"}',
                                status=405, mimetype="application/json")
            return cd.handler(request)

        return wrapper
    except Exception as e:
        # Re-raise with context so pytest shows why it failed to import
        raise ImportError(f"Could not load a local Flask app: {e}") from e


# -------------------- Pytest fixtures --------------------
@pytest.fixture(scope="session")
def client():
    if RENDER_BASE_URL:
        # Integration mode: we hit remote URL; no local Flask client
        return None
    os.environ["READ_ONLY_DB"] = "0"
    os.environ["DEBUG"] = "1"
    app = _load_local_flask_app()
    return app.test_client()


# -------------------- Tests --------------------
def test_health(client):
    status, body = _http_get(client, "/")
    assert status == 200
    assert isinstance(body, dict) and body.get("ok") is True

def test_county_data_success_200(client):
    status, body = _http_post(client, "/county_data",
                              {"zip": "02138", "measure_name": "Adult obesity"})
    assert status == 200
    assert isinstance(body, list) and len(body) >= 1
    row = body[0]
    for k in [
        "State", "County", "State_code", "County_code", "Year_span",
        "Measure_name", "Measure_id", "Numerator", "Denominator",
        "Raw_value", "Confidence_Interval_Lower_Bound",
        "Confidence_Interval_Upper_Bound", "Data_Release_Year", "fipscode",
    ]:
        assert k in row

def test_teapot_418(client):
    status, body = _http_post(client, "/county_data",
                              {"zip": "02138", "measure_name": "Adult obesity", "coffee": "teapot"})
    assert status == 418
    assert isinstance(body, dict)

def test_bad_request_400_missing_keys(client):
    status, body = _http_post(client, "/county_data", {})
    assert status == 400 and isinstance(body, dict) and "error" in body

    status, _ = _http_post(client, "/county_data", {"zip": "2138", "measure_name": "Adult obesity"})
    assert status == 400

    status, _ = _http_post(client, "/county_data", {"zip": "02138", "measure_name": "Adult Obesity"})
    assert status == 400

def test_not_found_404_no_match(client):
    status, body = _http_post(client, "/county_data",
                              {"zip": "99999", "measure_name": "Adult obesity"})
    assert status == 404 and isinstance(body, dict) and "error" in body

def test_method_not_allowed_405_on_get(client):
    status, _ = _http_get(client, "/county_data")
    assert status == 405
