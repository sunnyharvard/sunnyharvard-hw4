# Serverless Python function for Vercel.
# Endpoint: POST /county_data  (rewritten to /api/county_data.py by vercel.json)
#
# Behavior:
# - Requires JSON body with keys: "zip" (5-digit), "measure_name" (one of allowed list)
# - Optional easter egg: {"coffee":"teapot"} -> HTTP 418
# - 400 if inputs missing/invalid
# - 404 if no matching rows
# - 405 for non-POST
#
# Returns: JSON array of rows in the same schema as county_health_rankings

import json
import os
import re
import sqlite3
from typing import List, Dict, Any
from http import HTTPStatus
from flask import Request, Response

# --- Allowed measures (exact strings) ---
ALLOWED_MEASURES = {
    "Violent crime rate",
    "Unemployment",
    "Children in poverty",
    "Diabetic screening",
    "Mammography screening",
    "Preventable hospital stays",
    "Uninsured",
    "Sexually transmitted infections",
    "Physical inactivity",
    "Adult obesity",
    "Premature Death",
    "Daily fine particulate matter",
}

ZIP_RE = re.compile(r"^\d{5}$")

# Location of the SQLite database (bundled read-only with the deployment)
DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data.db")

def _bad_request(msg: str) -> Response:
    return Response(
        json.dumps({"error": msg}),
        status=HTTPStatus.BAD_REQUEST,
        mimetype="application/json",
    )

def _not_found(msg: str) -> Response:
    return Response(
        json.dumps({"error": msg}),
        status=HTTPStatus.NOT_FOUND,
        mimetype="application/json",
    )

def _teapot() -> Response:
    return Response(
        json.dumps({"error": None, "result": "🫖"}),
        status=418,
        mimetype="application/json",
    )

def _method_not_allowed() -> Response:
    return Response(
        json.dumps({"error": "Method Not Allowed"}),
        status=HTTPStatus.METHOD_NOT_ALLOWED,
        mimetype="application/json",
    )

def _load_json(request: Request) -> Dict[str, Any]:
    if request.mimetype != "application/json":
        # Still try to parse, but enforce JSON header per spec
        try:
            data = request.get_json(force=True, silent=False)
        except Exception:
            return {}
        return data or {}
    return request.get_json(force=True, silent=True) or {}

def _rows_to_dicts(cursor, rows):
    # Preserve the DB’s exact column names (capitalized)
    cols = [d[0] for d in cursor.description]
    out = []
    for r in rows:
        obj = {}
        for key, val in zip(cols, r):
            obj[key] = None if val is None else (str(val) if not isinstance(val, (int, float)) else val)
        out.append(obj)
    return out

def _get_conn():
    # Local-friendly (READ_ONLY_DB=0 to open normal path)
    ro = os.getenv("READ_ONLY_DB", "1") != "0"
    path = DB_PATH if os.path.isabs(DB_PATH) else os.path.abspath(DB_PATH)
    if ro:
        return sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    else:
        return sqlite3.connect(path, check_same_thread=False)

def _query(zip_code: str, measure_name: str):
    conn = _get_conn()
    cur = conn.cursor()
    # Match your exact table/column names and join on FIPS
    sql = """
    SELECT
      chr.State,
      chr.County,
      chr.State_code,
      chr.County_code,
      chr.Year_span,
      chr.Measure_name,
      chr.Measure_id,
      chr.Numerator,
      chr.Denominator,
      chr.Raw_value,
      chr.Confidence_Interval_Lower_Bound,
      chr.Confidence_Interval_Upper_Bound,
      chr.Data_Release_Year,
      chr.fipscode
    FROM county_health_rankings AS chr
    JOIN zip_county AS z
      ON CAST(chr.fipscode AS TEXT) = CAST(z.county_code AS TEXT)
    WHERE z.zip = ? AND chr.Measure_name = ?
    ORDER BY CAST(chr.Data_Release_Year AS INTEGER) ASC, chr.Year_span ASC
    """
    cur.execute(sql, (zip_code, measure_name))
    rows = cur.fetchall()
    result = _rows_to_dicts(cur, rows)
    cur.close()
    conn.close()
    return result


# Vercel will call this function.
def handler(request: Request) -> Response:
    if request.method != "POST":
        return _method_not_allowed()

    body = _load_json(request) or {}

    # Teapot easter egg supersedes everything
    if body.get("coffee") == "teapot":
        return _teapot()

    zip_code = body.get("zip")
    measure_name = body.get("measure_name")

    # Validate presence
    if not zip_code or not measure_name:
        return _bad_request("Both 'zip' and 'measure_name' are required.")

    # Validate zip format
    if not ZIP_RE.match(str(zip_code)):
        return _bad_request("Invalid 'zip' (must be a 5-digit ZIP code).")

    # Validate measure_name
    if measure_name not in ALLOWED_MEASURES:
        return _bad_request("Invalid 'measure_name' value.")

    try:
        rows = _query(zip_code, measure_name)
    except sqlite3.Error as e:
        # Hide implementation details but keep useful hint
        return _bad_request(f"Database error while querying inputs provided.")

    if not rows:
        return _not_found("No data for given zip/measure_name.")

    return Response(
        json.dumps(rows),
        status=HTTPStatus.OK,
        mimetype="application/json",
    )
