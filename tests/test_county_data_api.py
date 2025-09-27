# tests/test_county_data_api.py
import os
import sqlite3
import tempfile
from pathlib import Path
from fastapi.testclient import TestClient
import importlib.util
import types
import pytest


def create_test_db(path: Path):
    conn = sqlite3.connect(path)
    try:
        # Minimal schemas (all TEXT), per Part 1 expectations
        conn.execute("""
            CREATE TABLE zip_county (
                zip TEXT, default_state TEXT, county TEXT, county_state TEXT,
                state_abbreviation TEXT, county_code TEXT, zip_pop TEXT,
                zip_pop_in_county TEXT, n_counties TEXT, default_city TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE county_health_rankings (
                state TEXT, county TEXT, state_code TEXT, county_code TEXT,
                year_span TEXT, measure_name TEXT, measure_id TEXT,
                numerator TEXT, denominator TEXT, raw_value TEXT,
                confidence_interval_lower_bound TEXT,
                confidence_interval_upper_bound TEXT,
                data_release_year TEXT, fipscode TEXT
            )
        """)

        # zip 02138 -> Middlesex County, MA, codes
        conn.execute(
            "INSERT INTO zip_county (zip, state_abbreviation, county, county_code) VALUES (?,?,?,?)",
            ("02138", "MA", "Middlesex County", "25017"),
        )
        # A second county for same zip to test multi-county zips (rare, but handled)
        conn.execute(
            "INSERT INTO zip_county (zip, state_abbreviation, county, county_code) VALUES (?,?,?,?)",
            ("02138", "MA", "Another County", "25099"),
        )

        # Two CHR rows for Adult obesity in Middlesex
        rows = [
            ("MA", "Middlesex County", "25", "017", "2009", "Adult obesity", "11",
             "60771.02", "263078", "0.23", "0.22", "0.24", "2012", "25017"),
            ("MA", "Middlesex County", "25", "017", "2010", "Adult obesity", "11",
             "266426", "1143459.228", "0.233", "0.224", "0.242", "2014", "25017"),
        ]
        conn.executemany("""
            INSERT INTO county_health_rankings
              (state, county, state_code, county_code, year_span, measure_name,
               measure_id, numerator, denominator, raw_value,
               confidence_interval_lower_bound, confidence_interval_upper_bound,
               data_release_year, fipscode)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, rows)

        # Another measure (should not be returned)
        conn.execute("""
            INSERT INTO county_health_rankings
              (state, county, state_code, county_code, year_span, measure_name,
               measure_id, numerator, denominator, raw_value,
               confidence_interval_lower_bound, confidence_interval_upper_bound,
               data_release_year, fipscode)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, ("MA", "Middlesex County", "25", "017", "2010", "Unemployment", "99",
              "0", "0", "0", "0", "0", "2010", "25017"))

        conn.commit()
    finally:
        conn.close()


def import_app_with_db(db_path: str) -> types.ModuleType:
    """
    Import api/county_data.py as a module after setting DB_PATH env var to a temp DB.
    """
    os.environ["DB_PATH"] = db_path

    file_path = Path(__file__).resolve().parents[1] / "api" / "county_data.py"
    spec = importlib.util.spec_from_file_location("county_data", str(file_path))
    mod = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(mod)  # type: ignore
    return mod


@pytest.fixture
def client():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = str(Path(tmpdir) / "test.db")
        create_test_db(Path(db_path))
        mod = import_app_with_db(db_path)
        app = getattr(mod, "app")
        with TestClient(app) as c:
            yield c


def test_get_returns_404(client: TestClient):
    r = client.get("/")
    assert r.status_code == 404


def test_teapot(client: TestClient):
    r = client.post("/", json={"coffee": "teapot"})
    assert r.status_code == 418
    assert r.json().get("error")


def test_missing_fields(client: TestClient):
    r = client.post("/", json={"zip": "02138"})
    assert r.status_code == 400
    r = client.post("/", json={"measure_name": "Adult obesity"})
    assert r.status_code == 400


def test_invalid_zip_format(client: TestClient):
    r = client.post("/", json={"zip": "2138", "measure_name": "Adult obesity"})
    assert r.status_code == 400
    r = client.post("/", json={"zip": "ABCDE", "measure_name": "Adult obesity"})
    assert r.status_code == 400


def test_invalid_measure_name(client: TestClient):
    r = client.post("/", json={"zip": "02138", "measure_name": "Not A Measure"})
    assert r.status_code == 400


def test_zip_not_found(client: TestClient):
    r = client.post("/", json={"zip": "99999", "measure_name": "Adult obesity"})
    assert r.status_code == 404


def test_measure_not_found_for_valid_zip(client: TestClient):
    r = client.post("/", json={"zip": "02138", "measure_name": "Mammography screening"})
    assert r.status_code == 404


def test_happy_path_returns_expected_shape(client: TestClient):
    r = client.post("/", json={"zip": "02138", "measure_name": "Adult obesity"})
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    assert len(data) == 2

    # Verify required keys exist and look like strings
    first = data[0]
    for key in ["state", "county", "state_code", "county_code", "year_span", "measure_name",
                "measure_id", "numerator", "denominator", "raw_value",
                "confidence_interval_lower_bound", "confidence_interval_upper_bound",
                "data_release_year", "fipscode"]:
        assert key in first
        assert first[key] is None or isinstance(first[key], str)

    # Spot-check the Middlesex example values we inserted
    subset = {
        "county": "Middlesex County",
        "state": "MA",
        "state_code": "25",
        "county_code": "017",
        "fipscode": "25017",
        "measure_name": "Adult obesity",
    }
    for k, v in subset.items():
        assert data[0][k] == v
