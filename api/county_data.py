# api/county_data.py
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from typing import Dict, List
from pathlib import Path
import os
import re
import sqlite3

app = FastAPI()

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


def get_db_path() -> str:
    """Resolve DB path from env or default to project-root/data.db."""
    env = os.environ.get("DB_PATH")
    if env:
        return env
    # project root is one level above /api
    return str(Path(__file__).resolve().parents[1] / "data.db")


def qident(name: str) -> str:
    """
    Quote an identifier safely for SQLite.
    Our CSV loader only allowed [A-Za-z_][A-Za-z0-9_]*, so this is mostly belt & suspenders.
    """
    return '"' + name.replace('"', '""') + '"'


def find_col(columns: List[str], target: str) -> str:
    """
    Find the actual column name in `columns` that matches `target` case-insensitively,
    also ignoring underscores differences. Raises KeyError if not found.
    """
    t = target.lower()
    for c in columns:
        if c.lower() == t:
            return c
    t2 = t.replace("_", "")
    for c in columns:
        if c.lower().replace("_", "") == t2:
            return c
    raise KeyError(f"Column {target!r} not found in county_health_rankings")


def get_chr_columns(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("PRAGMA table_info(county_health_rankings)").fetchall()
    if not rows:
        raise HTTPException(status_code=500, detail="county_health_rankings table missing")
    return [r[1] for r in rows]


def rows_to_dicts(rows: List[sqlite3.Row], select_cols: List[str]) -> List[Dict]:
    out: List[Dict] = []
    for row in rows:
        obj = {}
        for col in select_cols:
            val = row[col]
            # Output strings (DB columns are TEXT); keep None as null in JSON
            obj[col] = None if val is None else str(val)
        out.append(obj)
    return out


@app.get("/")
async def _index_get():
    # Spec: it only accepts POST. Browsers doing GET should see 404.
    raise HTTPException(status_code=404, detail="Not found")


@app.post("/")
async def county_data(request: Request):
    # Enforce JSON
    ctype = request.headers.get("content-type", "")
    if "application/json" not in ctype.lower():
        raise HTTPException(status_code=400, detail="content-type must be application/json")

    payload = await request.json()

    # Teapot rule supersedes all (HTTP 418)
    if payload.get("coffee") == "teapot":
        return JSONResponse({"error": "I'm a teapot"}, status_code=418)

    # Validate required fields
    zip_code = payload.get("zip")
    measure_name = payload.get("measure_name")

    if zip_code is None or measure_name is None:
        raise HTTPException(status_code=400, detail="zip and measure_name are required")

    if not isinstance(zip_code, str) or not ZIP_RE.fullmatch(zip_code):
        raise HTTPException(status_code=400, detail="zip must be a 5-digit string")

    if measure_name not in ALLOWED_MEASURES:
        raise HTTPException(status_code=400, detail="measure_name is not allowed")

    db_path = get_db_path()
    if not os.path.exists(db_path):
        raise HTTPException(status_code=500, detail="data.db not found on server")

    # Each request gets a dedicated connection
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        # 1) Resolve zip -> possible counties/states/codes
        zc_rows = conn.execute(
            """
            SELECT zip, state_abbreviation, county, county_code
            FROM zip_county
            WHERE zip = ?
            """,
            (zip_code,),
        ).fetchall()

        if not zc_rows:
            raise HTTPException(status_code=404, detail="zip not found")

        states = sorted({r["state_abbreviation"] for r in zc_rows if r["state_abbreviation"]})
        county_names = sorted({r["county"] for r in zc_rows if r["county"]})
        codes = [str(r["county_code"]) for r in zc_rows if r["county_code"]]
        fips_codes = sorted({c for c in codes if len(c) >= 5})
        ccc_codes = sorted({c[-3:] for c in codes if len(c) >= 3})

        # 2) Build county_health_rankings query using introspected column names
        chr_cols = get_chr_columns(conn)
        # Columns used in filters:
        col_measure_name = find_col(chr_cols, "measure_name")
        col_state = find_col(chr_cols, "state")
        col_county = find_col(chr_cols, "county")
        col_county_code = find_col(chr_cols, "county_code")
        col_fipscode = find_col(chr_cols, "fipscode")

        # SELECT list returns ALL columns in county_health_rankings "as is"
        select_cols = chr_cols[:]  # preserve DB schema in the JSON keys
        select_sql = ", ".join([f'chr.{qident(c)} AS {qident(c)}' for c in select_cols])

        clauses = []
        params: List[str] = []

        if fips_codes:
            clauses.append(f"chr.{qident(col_fipscode)} IN ({','.join(['?']*len(fips_codes))})")
            params.extend(fips_codes)

        if states and ccc_codes:
            clauses.append(
                "("
                f"chr.{qident(col_state)} IN ({','.join(['?']*len(states))}) "
                "AND "
                f"chr.{qident(col_county_code)} IN ({','.join(['?']*len(ccc_codes))})"
                ")"
            )
            params.extend(states)
            params.extend(ccc_codes)

        if states and county_names:
            clauses.append(
                "("
                f"chr.{qident(col_state)} IN ({','.join(['?']*len(states))}) "
                "AND "
                f"chr.{qident(col_county)} IN ({','.join(['?']*len(county_names))})"
                ")"
            )
            params.extend(states)
            params.extend(county_names)

        if not clauses:
            # We couldn't build a safe mapping from zip to CHR; treat as not found
            raise HTTPException(status_code=404, detail="no county mapping for zip")

        sql = (
            f"SELECT {select_sql} "
            "FROM county_health_rankings AS chr "
            f"WHERE chr.{qident(col_measure_name)} = ? "
            "AND (" + " OR ".join(clauses) + ")"
        )
        rows = conn.execute(sql, (measure_name, *params)).fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail="zip/measure_name pair not found")

        return rows_to_dicts(rows, select_cols)

    finally:
        conn.close()
