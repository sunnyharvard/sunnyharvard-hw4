#!/usr/bin/env python3

# by Sunny Liu and ChatGPT-5

from flask import Flask, request, jsonify
import sqlite3
import os
import re

app = Flask(__name__)

# Acceptable health measures
ALLOWED_MEASURES = {
    "Violent crime rate", "Unemployment", "Children in poverty",
    "Diabetic screening", "Mammography screening",
    "Preventable hospital stays", "Uninsured",
    "Sexually transmitted infections", "Physical inactivity",
    "Adult obesity", "Premature Death", "Daily fine particulate matter"
}

# Validate ZIP using regex
ZIP_CODE_PATTERN = re.compile(r'^\d{5}$')


def locate_db():
    """Try different possible locations for the database."""
    possible_paths = [
        'data.db',
        os.path.join(os.path.dirname(__file__), 'data.db'),
        os.path.join(os.getcwd(), 'data.db'),
        '/var/task/data.db'  # for platforms like Vercel
    ]
    for path in possible_paths:
        if os.path.exists(path):
            return path
    return None


def get_db_connection():
    """Connect to SQLite database."""
    db_path = locate_db()
    if not db_path:
        return None
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None


def is_valid_zip(zip_code):
    """Return True if ZIP code is valid."""
    return bool(ZIP_CODE_PATTERN.match(str(zip_code)))


def fetch_data(zip_code, measure_name):
    """Fetch county data from database for a given ZIP and measure, with ordered fields."""
    conn = get_db_connection()
    if not conn:
        return None

    try:
        query = """
            SELECT chr.State, chr.County, chr.State_code, chr.County_code,
                   chr.Year_span, chr.Measure_name, chr.Measure_id,
                   chr.Numerator, chr.Denominator, chr.Raw_value,
                   chr.Confidence_Interval_Lower_Bound,
                   chr.Confidence_Interval_Upper_Bound,
                   chr.Data_Release_Year, chr.fipscode
            FROM county_health_rankings AS chr
            JOIN zip_county AS z ON CAST(chr.fipscode AS TEXT) = CAST(z.county_code AS TEXT)
            WHERE z.zip = ? AND chr.Measure_name = ?
            ORDER BY CAST(chr.Data_Release_Year AS INTEGER) ASC, chr.Year_span ASC
        """
        cursor = conn.execute(query, (zip_code, measure_name))
        rows = cursor.fetchall()

        # Build each row in the specific key order
        results = []
        for row in rows:
            results.append({
                "confidence_interval_lower_bound": row["Confidence_Interval_Lower_Bound"],
                "confidence_interval_upper_bound": row["Confidence_Interval_Upper_Bound"],
                "county": row["County"],
                "county_code": row["County_code"],
                "data_release_year": row["Data_Release_Year"],
                "denominator": row["Denominator"],
                "fipscode": row["fipscode"],
                "measure_id": row["Measure_id"],
                "measure_name": row["Measure_name"],
                "numerator": row["Numerator"],
                "raw_value": row["Raw_value"],
                "state": row["State"],
                "state_code": row["State_code"],
                "year_span": row["Year_span"]
            })
        return results

    except Exception:
        return None
    finally:
        conn.close()



@app.route("/", methods=["GET"])
def health_check():
    return jsonify({"ok": True})


@app.route("/county_data", methods=["POST"])
def county_data():
    if not request.is_json:
        return jsonify({"error": "Request must be JSON"}), 400

    data = request.get_json()

    # Easter egg
    if data.get("coffee") == "teapot":
        return jsonify({"error": "I'm a teapot"}), 418

    zip_code = data.get("zip")
    measure_name = data.get("measure_name")

    if not zip_code or not measure_name:
        return jsonify({"error": "Both 'zip' and 'measure_name' are required."}), 400

    if not is_valid_zip(zip_code):
        return jsonify({"error": "ZIP code must be a 5-digit number."}), 400

    if measure_name not in ALLOWED_MEASURES:
        return jsonify({
            "error": f"Invalid 'measure_name'. Choose from: {sorted(ALLOWED_MEASURES)}"
        }), 400

    results = fetch_data(zip_code, measure_name)

    if results is None:
        return jsonify({"error": "Database error"}), 500

    if not results:
        return jsonify({"error": "No data found for given zip and measure_name."}), 404

    return jsonify(results), 200


# --- Custom Error Handlers ---

@app.errorhandler(404)
def handle_404(e):
    return jsonify({"error": "Endpoint not found"}), 404


@app.errorhandler(405)
def handle_405(e):
    return jsonify({"error": "Method not allowed"}), 405


# --- Run the app ---

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    debug = os.environ.get("DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug)
