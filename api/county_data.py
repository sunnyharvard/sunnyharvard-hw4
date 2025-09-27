import json
import sqlite3
import os
import re
from pathlib import Path

ALLOWED_MEASURES = {
    "Violent crime rate","Unemployment","Children in poverty","Diabetic screening",
    "Mammography screening","Preventable hospital stays","Uninsured",
    "Sexually transmitted infections","Physical inactivity","Adult obesity",
    "Premature Death","Daily fine particulate matter",
}
ZIP_RE = re.compile(r"^\d{5}$")

def get_db_path():
    return os.environ.get("DB_PATH") or str(Path(__file__).resolve().parents[1] / "data.db")

def handler(request):
    if request.method != "POST":
        return {"statusCode": 404, "body": json.dumps({"error": "Not found"})}

    try:
        body = request.get_json()
    except Exception:
        return {"statusCode": 400, "body": json.dumps({"error": "Invalid JSON"})}

    if body.get("coffee") == "teapot":
        return {"statusCode": 418, "body": json.dumps({"error": "I'm a teapot"})}

    zip_code = body.get("zip")
    measure_name = body.get("measure_name")
    if not zip_code or not measure_name:
        return {"statusCode": 400, "body": json.dumps({"error": "zip and measure_name required"})}
    if not ZIP_RE.fullmatch(str(zip_code)):
        return {"statusCode": 400, "body": json.dumps({"error": "zip must be 5 digits"})}
    if measure_name not in ALLOWED_MEASURES:
        return {"statusCode": 400, "body": json.dumps({"error": "invalid measure_name"})}

    db_path = get_db_path()
    if not os.path.exists(db_path):
        return {"statusCode": 500, "body": json.dumps({"error": "data.db not found"})}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        zc = conn.execute("SELECT state_abbreviation, county, county_code FROM zip_county WHERE zip=?", (zip_code,)).fetchall()
        if not zc:
            return {"statusCode": 404, "body": json.dumps({"error": "zip not found"})}

        states = {r["state_abbreviation"] for r in zc if r["state_abbreviation"]}
        counties = {r["county"] for r in zc if r["county"]}
        county_codes = {r["county_code"] for r in zc if r["county_code"]}

        q = ("SELECT * FROM county_health_rankings "
             "WHERE measure_name=? AND state IN ({}) AND county IN ({})").format(
                 ",".join("?"*len(states)), ",".join("?"*len(counties))
             )
        params = [measure_name] + list(states) + list(counties)
        rows = conn.execute(q, params).fetchall()
        if not rows:
            return {"statusCode": 404, "body": json.dumps({"error": "no data found"})}

        data = [{k: (v if v is not None else None) for k, v in dict(r).items()} for r in rows]
        return {"statusCode": 200, "body": json.dumps(data)}
    finally:
        conn.close()
