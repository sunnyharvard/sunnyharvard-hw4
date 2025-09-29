# pytest tests for csv_to_sqlite.py, by Sunny Liu and ChatGPT-5
import csv
import os
import sqlite3
import subprocess
import sys
from pathlib import Path
import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "csv_to_sqlite.py"


def run_cli(db_path, csv_path):
    """Run the CLI and return CompletedProcess."""
    return subprocess.run(
        [sys.executable, str(SCRIPT_PATH), str(db_path), str(csv_path)],
        capture_output=True,
        text=True,
        check=False,
    )


def get_columns(conn, table):
    """Return a list of (name, type) for columns in table using PRAGMA."""
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [(row[1], row[2]) for row in cur.fetchall()]


def get_count(conn, table):
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def write_csv(path, header, rows, encoding="utf-8", add_bom=False):
    # Ensure parent exists
    path.parent.mkdir(parents=True, exist_ok=True)
    if add_bom:
        encoding = "utf-8-sig"
    with open(path, "w", newline="", encoding=encoding) as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for r in rows:
            writer.writerow(r)


def test_usage_message_when_args_missing(tmp_path):
    # Running with missing args should produce usage message and exit code 1
    proc = subprocess.run(
        [sys.executable, str(SCRIPT_PATH)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 1
    assert "Usage:" in proc.stdout or "Usage:" in proc.stderr


def test_import_zip_county_basic_schema_and_count(tmp_path):
    db = tmp_path / "data.db"
    csv_path = tmp_path / "zip_county.csv"
    header = [
        "zip","default_state","county","county_state","state_abbreviation",
        "county_code","zip_pop","zip_pop_in_county","n_counties","default_city"
    ]
    rows = [
        ["90210","CA","Los_Angeles","Los_Angeles","CA","06037","25000","25000","1","Beverly_Hills"],
        ["30301","GA","Fulton","Fulton","GA","13121","1000","1000","1","Atlanta"],
        ["99950","AK","Ketchikan_Gateway","Ketchikan_Gateway","AK","02130","50","50","1",""],  # empty default_city -> NULL
    ]
    write_csv(csv_path, header, rows)

    proc = run_cli(db, csv_path)
    assert proc.returncode == 0, f"STDERR: {proc.stderr}\nSTDOUT: {proc.stdout}"

    with sqlite3.connect(db) as conn:
        cols = get_columns(conn, "zip_county")
        assert [c for c, _ in cols] == header
        # All TEXT columns
        assert all(t.upper() == "TEXT" for _, t in cols)
        # Row count
        assert get_count(conn, "zip_county") == len(rows)
        # Empty string stored as NULL
        val = conn.execute(
            "SELECT default_city FROM zip_county WHERE zip = ?", ("99950",)
        ).fetchone()[0]
        assert val is None


def test_import_chr_case_and_types(tmp_path):
    db = tmp_path / "data.db"
    csv_path = tmp_path / "county_health_rankings.csv"
    header = [
        "State","County","State_code","County_code","Year_span","Measure_name","Measure_id",
        "Numerator","Denominator","Raw_value","Confidence_Interval_Lower_Bound",
        "Confidence_Interval_Upper_Bound","Data_Release_Year","fipscode"
    ]
    rows = [
        ["Wisconsin","Dane","55","025","2018-2020","Adult_obesity","M123","50000","250000","20","18","22","2025","55025"],
        ["California","Alameda","06","001","2018-2020","Adult_obesity","M123","250000","1250000","20","19","21","2025","06001"],
    ]
    write_csv(csv_path, header, rows)

    proc = run_cli(db, csv_path)
    assert proc.returncode == 0, f"STDERR: {proc.stderr}\nSTDOUT: {proc.stdout}"

    with sqlite3.connect(db) as conn:
        cols = get_columns(conn, "county_health_rankings")
        assert [c for c, _ in cols] == header  # header case preserved
        assert all(t.upper() == "TEXT" for _, t in cols)
        assert get_count(conn, "county_health_rankings") == len(rows)


def test_drop_and_recreate_on_reimport_with_changed_schema(tmp_path):
    db = tmp_path / "data.db"
    csv_path = tmp_path / "foo.csv"

    # First import with two columns
    header1 = ["a", "b"]
    rows1 = [["1", "2"], ["3", "4"]]
    write_csv(csv_path, header1, rows1)

    proc1 = run_cli(db, csv_path)
    assert proc1.returncode == 0, proc1.stderr

    with sqlite3.connect(db) as conn:
        cols1 = get_columns(conn, "foo")
        assert [c for c, _ in cols1] == header1
        assert get_count(conn, "foo") == len(rows1)

    # Second import with an additional column 'c' — table should be dropped & recreated
    header2 = ["a", "b", "c"]
    rows2 = [["1", "2", "3"]]
    write_csv(csv_path, header2, rows2)

    proc2 = run_cli(db, csv_path)
    assert proc2.returncode == 0, proc2.stderr

    with sqlite3.connect(db) as conn:
        cols2 = get_columns(conn, "foo")
        assert [c for c, _ in cols2] == header2
        assert get_count(conn, "foo") == len(rows2)


def test_invalid_table_name_fails(tmp_path):
    db = tmp_path / "data.db"
    bad_csv = tmp_path / "1bad.csv"  # starts with digit -> invalid identifier in our script
    write_csv(bad_csv, ["col1"], [["x"]])
    proc = run_cli(db, bad_csv)
    assert proc.returncode != 0
    assert "invalid table name" in (proc.stderr + proc.stdout).lower()


def test_invalid_column_name_fails(tmp_path):
    db = tmp_path / "data.db"
    csv_path = tmp_path / "bad_cols.csv"
    # Column with space should be rejected by the script's identifier check
    write_csv(csv_path, ["good", "bad name"], [["a", "b"]])
    proc = run_cli(db, csv_path)
    assert proc.returncode != 0
    assert "invalid column name" in (proc.stderr + proc.stdout).lower()


def test_empty_file_no_header_error(tmp_path):
    db = tmp_path / "data.db"
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("", encoding="utf-8")
    proc = run_cli(db, empty_csv)
    assert proc.returncode != 0
    assert "missing a header" in (proc.stderr + proc.stdout).lower()


def test_bom_in_header_is_handled(tmp_path):
    db = tmp_path / "data.db"
    csv_path = tmp_path / "bom_test.csv"
    header = ["zip", "city"]
    rows = [["02139", "Cambridge"]]
    # Write with UTF-8 BOM
    with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

    proc = run_cli(db, csv_path)
    assert proc.returncode == 0, proc.stderr

    with sqlite3.connect(db) as conn:
        cols = get_columns(conn, "bom_test")
        assert [c for c, _ in cols] == header
        assert get_count(conn, "bom_test") == len(rows)

def test_sql_injection_attempt_in_column_name(tmp_path):
    db = tmp_path / "data.db"
    csv_path = tmp_path / "injection.csv"
    
    # Dangerous column name attempt
    header = ["name", "value; DROP TABLE users"]
    rows = [["test", "123"]]

    write_csv(csv_path, header, rows)

    proc = run_cli(db, csv_path)
    assert proc.returncode != 0
    assert "invalid column name" in (proc.stderr + proc.stdout).lower()


def test_sql_keyword_as_column_name(tmp_path):
    db = tmp_path / "data.db"
    csv_path = tmp_path / "keywords.csv"

    # "SELECT" is a reserved keyword
    header = ["id", "SELECT"]
    rows = [["1", "hack"]]
    write_csv(csv_path, header, rows)

    proc = run_cli(db, csv_path)
    assert proc.returncode != 0
    assert "invalid column name" in (proc.stderr + proc.stdout).lower()
