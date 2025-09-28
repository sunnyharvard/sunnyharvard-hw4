from flask import Flask, request, Response
import json, os, re, sqlite3

app = Flask(__name__)

ALLOWED_MEASURES = {
    "Violent crime rate","Unemployment","Children in poverty","Diabetic screening",
    "Mammography screening","Preventable hospital stays","Uninsured",
    "Sexually transmitted infections","Physical inactivity","Adult obesity",
    "Premature Death","Daily fine particulate matter",
}
ZIP_RE = re.compile(r"^\d{5}$")
DB_PATH = os.path.join(os.path.dirname(__file__), "data.db")

def _bad(status, msg):
    return Response(json.dumps({"error": msg}), status=status, mimetype="application/json")

def _rows(cur, rows):
    cols = [d[0] for d in cur.description]
    out = []
    for r in rows:
        out.append({k: (None if v is None else (v if isinstance(v,(int,float)) else str(v)))
                    for k, v in zip(cols, r)})
    return out

def _get_conn():
    # normal read/write path works on Render
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def _query(zip_code, measure_name):
    conn = _get_conn()
    cur = conn.cursor()
    # Uses your column names (capitalized) and your join on county FIPS
    sql = """
    SELECT
      chr.State, chr.County, chr.State_code, chr.County_code, chr.Year_span,
      chr.Measure_name, chr.Measure_id, chr.Numerator, chr.Denominator,
      chr.Raw_value, chr.Confidence_Interval_Lower_Bound,
      chr.Confidence_Interval_Upper_Bound, chr.Data_Release_Year, chr.fipscode
    FROM county_health_rankings AS chr
    JOIN zip_county AS z
      ON CAST(chr.fipscode AS TEXT) = CAST(z.county_code AS TEXT)
    WHERE z.zip = ? AND chr.Measure_name = ?
    ORDER BY CAST(chr.Data_Release_Year AS INTEGER) ASC, chr.Year_span ASC
    """
    cur.execute(sql, (zip_code, measure_name))
    rows = cur.fetchall()
    out = _rows(cur, rows)
    cur.close(); conn.close()
    return out

@app.get("/")
def health():
    return {"ok": True}

@app.post("/county_data")
def county_data():
    body = request.get_json(silent=True) or {}
    if body.get("coffee") == "teapot":
        return Response(json.dumps({"error": None, "result": "🫖"}), status=418, mimetype="application/json")

    zip_code = body.get("zip")
    measure_name = body.get("measure_name")
    if not zip_code or not measure_name:
        return _bad(400, "Both 'zip' and 'measure_name' are required.")
    if not ZIP_RE.match(str(zip_code)):
        return _bad(400, "Invalid 'zip' (must be a 5-digit ZIP code).")
    if measure_name not in ALLOWED_MEASURES:
        return _bad(400, "Invalid 'measure_name' value.")

    try:
        out = _query(zip_code, measure_name)
    except Exception:
        return _bad(400, "Database error while querying inputs provided.")
    if not out:
        return _bad(404, "No data for given zip/measure_name.")
    return Response(json.dumps(out), mimetype="application/json")
