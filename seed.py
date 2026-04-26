"""
seed.py
-------
ETL entry-point for the Personal Telemetry & Activity Analytics platform.

Delegates all CSV parsing to DataParser (data_parser.py) so the ETL logic
lives in exactly one place.

Usage
-----
    python seed.py                              # default CSV paths
    python seed.py UserInfo.csv Sensors.csv     # explicit paths
    DB_USER=myuser DB_PASSWORD=secret python seed.py
"""

from __future__ import annotations

import os
import sys

from data_parser import DataParser
from database_manager import DatabaseManager

# ── Database connection — every value overridable via environment variable ────
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "telemetry_db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)


def run_seed(user_file: str = "UserInfo.csv", sensor_file: str = "Sensors.csv") -> None:
    """
    Full ETL pipeline: ensure the database exists, create the schema,
    then load users followed by sensor readings via DataParser.
    DatabaseManager.__init__ handles creating the database if it is missing.
    """
    print("=" * 60)
    print("  Personal Telemetry Seed Script")
    print("=" * 60)

    print("\nChecking / creating database…")
    db = DatabaseManager(DATABASE_URL)  # creates DB if missing

    print("\nCreating schema (if tables do not already exist)…")
    db.init_schema()
    print("  Schema OK")

    session = db.Session()
    try:
        parser     = DataParser(session, user_file=user_file, sensor_file=sensor_file)
        known_uids = parser.parse_users()
        session.commit()

        parser.parse_telemetry(known_uids=known_uids)

        print("\n" + "=" * 60)
        print("  Seed complete!")
        print("=" * 60)

    except Exception as e:
        session.rollback()
        print(f"\nFATAL ERROR — rolled back: {e}")
        sys.exit(1)
    finally:
        session.close()
        db.engine.dispose()


if __name__ == "__main__":
    u = sys.argv[1] if len(sys.argv) > 1 else "UserInfo.csv"
    s = sys.argv[2] if len(sys.argv) > 2 else "Sensors.csv"
    run_seed(user_file=u, sensor_file=s)
