#!/usr/bin/env python3

# code by Sunny Liu and ChatGPT-5

import sys
import os
import csv
import sqlite3
from pathlib import Path

# Reserved keywords in SQLite (simplified subset)
SQLITE_KEYWORDS = {
    'SELECT', 'INSERT', 'DELETE', 'UPDATE', 'CREATE', 'DROP', 'TABLE', 'FROM',
    'WHERE', 'AND', 'OR', 'NOT', 'NULL', 'IN', 'IS', 'LIKE', 'BY', 'GROUP',
    'ORDER', 'HAVING', 'AS', 'JOIN', 'ON', 'UNION', 'VALUES', 'INTO'
}

def _validate_identifier(name: str) -> bool:
    """Strict check for safe SQL identifiers: alphanumeric + underscores, no keywords."""
    if not name:
        return False
    if not (name[0].isalpha() or name[0] == "_"):
        return False
    if not all(c.isalnum() or c == "_" for c in name):
        return False
    if name.upper() in SQLITE_KEYWORDS:
        return False
    return True

def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python3 csv_to_sqlite.py <database.db> <input.csv>")
        sys.exit(1)

    db_path = sys.argv[1]
    csv_path = sys.argv[2]
    table_name = Path(csv_path).stem

    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found: {csv_path}", file=sys.stderr)
        sys.exit(2)

    # Validate table name
    if not _validate_identifier(table_name):
        print(f"Error: invalid table name derived from CSV filename: {table_name!r}", file=sys.stderr)
        sys.exit(4)

    conn = sqlite3.connect(db_path)
    try:
        with open(csv_path, "r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                print("Error: CSV appears to be missing a header row.", file=sys.stderr)
                sys.exit(3)

            columns = [c.strip() for c in reader.fieldnames]

            for col in columns:
                if not _validate_identifier(col):
                    print(f"Error: invalid column name in header: {col!r}", file=sys.stderr)
                    sys.exit(5)

            # Drop table safely
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")

            # Create table
            col_defs = ", ".join(f"{col} TEXT" for col in columns)
            conn.execute(f"CREATE TABLE {table_name} ({col_defs})")

            # Insert rows
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            cur = conn.cursor()
            batch = []
            batch_size = 10000
            total = 0

            for row in reader:
                values = [row.get(col) or None for col in columns]
                batch.append(values)

                if len(batch) >= batch_size:
                    cur.executemany(insert_sql, batch)
                    total += len(batch)
                    batch.clear()

            if batch:
                cur.executemany(insert_sql, batch)
                total += len(batch)

            conn.commit()
            print(f"Imported {total} rows into table '{table_name}' in database '{db_path}'.")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
