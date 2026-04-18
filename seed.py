"""
seed.py
-------
ETL entry-point for the Personal Telemetry & Activity Analytics platform.

Reads UserInfo.csv and Sensors.csv and persists them into the 5-table
PostgreSQL schema defined in models.py.

ORM models live exclusively in models.py — this file imports them from
there so the schema definition is never duplicated across the codebase.

Usage
-----
    python seed.py                              # default CSV paths
    python seed.py UserInfo.csv Sensors.csv     # explicit paths
    DB_USER=myuser DB_PASSWORD=secret python seed.py
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import datetime

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

# ── Import ORM models from the shared models module ───────────────────────────
# The Base and all five table classes (User, DeviceStatus, MotionLog,
# EnvironmentalLog, OrientationLog) are defined once in models.py.
# seed.py re-uses them directly so the schema is never duplicated.
from models import (
    Base,
    DeviceStatus,
    EnvironmentalLog,
    MotionLog,
    OrientationLog,
    User,
)


# ─────────────────────────────────────────────────────────────────────────────
# Database connection — every value overridable via environment variable
# ─────────────────────────────────────────────────────────────────────────────
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "telemetry_db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
)


# ─────────────────────────────────────────────────────────────────────────────
# Helper / cleaning functions
# ─────────────────────────────────────────────────────────────────────────────
def clean_float(val) -> float:
    if val is None or str(val).strip() == "":
        return 0.0
    try:
        return float(str(val).strip())
    except Exception:
        return 0.0


def clean_int(val) -> int:
    if val is None or str(val).strip() == "":
        return 0
    s = str(val).strip().replace("%", "")
    try:
        return int(float(s))
    except Exception:
        return 0


def clean_str(val) -> str:
    s = str(val).strip() if val is not None else ""
    return s if s else "0"


# ─────────────────────────────────────────────────────────────────────────────
# Database bootstrap
# ─────────────────────────────────────────────────────────────────────────────
def create_database_if_missing() -> None:
    """
    Connect to the postgres maintenance database and create DB_NAME if it
    does not yet exist.  Uses psycopg2 directly because SQLAlchemy cannot
    issue CREATE DATABASE inside a transaction block.
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASSWORD,
            dbname="postgres",
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (DB_NAME,))
        if cur.fetchone() is None:
            cur.execute(f'CREATE DATABASE "{DB_NAME}"')
            print(f"  Created database '{DB_NAME}'")
        else:
            print(f"  Database '{DB_NAME}' already exists")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"\nERROR: Could not connect to PostgreSQL.")
        print(f"  Host={DB_HOST}  Port={DB_PORT}  User={DB_USER}")
        print(f"  Detail: {e}")
        sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# User helpers
# ─────────────────────────────────────────────────────────────────────────────
def ensure_user(session, uid: str, known_uids: set) -> bool:
    """
    Guarantee a User row exists for *uid*.

    If the UID is not in *known_uids* a placeholder row is inserted with
    default demographic values.  Returns False only when uid == "0"
    (sentinel for a completely blank UID field).
    """
    if uid in known_uids:
        return True
    if uid == "0":
        return False

    session.execute(
        pg_insert(User)
        .values(uid=uid, age_range="0", gender="0", university="0")
        .on_conflict_do_nothing(constraint="users_pkey")
    )
    session.flush()
    known_uids.add(uid)
    return True


# ─────────────────────────────────────────────────────────────────────────────
# ETL — Users
# ─────────────────────────────────────────────────────────────────────────────
def load_users(session, filepath: str = "UserInfo.csv") -> set:
    """
    Parse *filepath* (UserInfo.csv) and upsert every row into the users table.

    Returns
    -------
    set[str]
        Set of all UIDs successfully loaded (used by load_sensors to decide
        whether a placeholder user needs to be created).
    """
    print(f"\nLoading: {filepath}")
    known_uids: set[str] = set()
    errors = 0

    with open(filepath, mode="r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=1):
            if not any(row.values()):
                continue
            try:
                uid = clean_str(row.get("UID"))
                if uid == "0":
                    raise ValueError("Missing UID")

                session.execute(
                    pg_insert(User)
                    .values(
                        uid        = uid,
                        age_range  = clean_str(row.get("age")),
                        gender     = clean_str(row.get("gender")),
                        university = clean_str(row.get("uni")),
                    )
                    .on_conflict_do_update(
                        constraint="users_pkey",
                        set_=dict(
                            age_range  = clean_str(row.get("age")),
                            gender     = clean_str(row.get("gender")),
                            university = clean_str(row.get("uni")),
                        ),
                    )
                )
                known_uids.add(uid)

            except Exception as e:
                errors += 1
                print(f"  [row {i}] ERROR: {e}")

    session.flush()
    print(f"  Loaded  : {len(known_uids)}")
    print(f"  Errors  : {errors}")
    return known_uids


# ─────────────────────────────────────────────────────────────────────────────
# ETL — Sensors
# ─────────────────────────────────────────────────────────────────────────────
def load_sensors(session, known_uids: set, filepath: str = "Sensors.csv") -> None:
    """
    Parse *filepath* (Sensors.csv) and insert one row per sensor snapshot
    into device_status plus its three child log tables.

    Duplicate (uid, recorded_at) pairs are silently skipped so the script
    is always safe to re-run.
    """
    print(f"\nLoading: {filepath}")

    inserted          = 0
    duplicates        = 0
    placeholder_users = 0
    errors            = 0
    BATCH             = 500

    with open(filepath, mode="r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)

        for i, row in enumerate(reader, start=1):
            if not any(row.values()):
                continue

            try:
                uid = clean_str(row.get("UID"))

                if uid not in known_uids:
                    if not ensure_user(session, uid, known_uids):
                        errors += 1
                        continue
                    placeholder_users += 1
                    print(f"  [row {i}] Placeholder user created for UID: {uid}")

                # Parse timestamp
                ts_raw      = (row.get("Date_time") or "").strip()
                recorded_at = None
                if ts_raw:
                    for fmt in (
                        "%m/%d/%Y %H:%M",
                        "%m/%d/%Y %H:%M:%S",
                        "%Y-%m-%d %H:%M:%S",
                        "%Y-%m-%dT%H:%M:%S",
                    ):
                        try:
                            recorded_at = datetime.strptime(ts_raw, fmt)
                            break
                        except ValueError:
                            continue

                # DeviceStatus — skip silently on (uid, recorded_at) conflict
                result = session.execute(
                    pg_insert(DeviceStatus)
                    .values(
                        uid           = uid,
                        recorded_at   = recorded_at,
                        battery_level = clean_int(row.get("BATTERY_LEVEL")),
                        gps_latitude  = clean_float(row.get("SENSORGPS_LATITUDE")),
                        gps_longitude = clean_float(row.get("SENSORGPS_LONGITUDE")),
                    )
                    .on_conflict_do_nothing(constraint="uq_device_uid_recorded")
                    .returning(DeviceStatus.reading_id)
                )
                row_id = result.scalar()

                if row_id is None:
                    duplicates += 1
                    continue

                # Child log tables
                session.execute(
                    pg_insert(MotionLog).values(
                        reading_id = row_id,
                        accel_x    = clean_float(row.get("ACCELEROMETER_X")),
                        accel_y    = clean_float(row.get("ACCELEROMETER_Y")),
                        accel_z    = clean_float(row.get("ACCELEROMETER_Z")),
                        grav_x     = clean_float(row.get("GRAV_X")),
                        grav_y     = clean_float(row.get("GRAV_Y")),
                        grav_z     = clean_float(row.get("GRAV_Z")),
                        gyro_x     = clean_float(row.get("GYROSCOPE_X")),
                        gyro_y     = clean_float(row.get("GYROSCOPE_Y")),
                        gyro_z     = clean_float(row.get("GYROSCOPE_Z")),
                    ).on_conflict_do_nothing()
                )
                session.execute(
                    pg_insert(EnvironmentalLog).values(
                        reading_id = row_id,
                        light      = clean_float(row.get("Light_v")),
                        mag_x      = clean_float(row.get("MAG_X")),
                        mag_y      = clean_float(row.get("MAG_Y")),
                        mag_z      = clean_float(row.get("MAG_Z")),
                    ).on_conflict_do_nothing()
                )
                session.execute(
                    pg_insert(OrientationLog).values(
                        reading_id = row_id,
                        azimuth    = clean_float(row.get("ORIENTATION_AZIMUTH")),
                        pitch      = clean_float(row.get("ORIENTATION_PITCH")),
                        roll       = clean_float(row.get("ORIENTATION_ROLL")),
                    ).on_conflict_do_nothing()
                )

                inserted += 1

                if inserted % BATCH == 0:
                    session.commit()
                    print(f"  ... committed {inserted} rows so far")

            except Exception as e:
                errors += 1
                session.rollback()
                print(f"  [row {i}] ERROR: {e}")

    session.commit()
    print(f"  Inserted          : {inserted}")
    print(f"  Duplicates skipped: {duplicates}  (already in DB — safe to re-run)")
    print(f"  Placeholder users : {placeholder_users}  (UID missing from UserInfo — defaults used)")
    print(f"  Errors            : {errors}")


# ─────────────────────────────────────────────────────────────────────────────
# Entry-point
# ─────────────────────────────────────────────────────────────────────────────
def run_seed(user_file: str = "UserInfo.csv", sensor_file: str = "Sensors.csv") -> None:
    """
    Full ETL pipeline: ensure the database exists, create the schema,
    then load users followed by sensor readings.

    Parameters
    ----------
    user_file   : path to UserInfo.csv
    sensor_file : path to Sensors.csv
    """
    print("=" * 60)
    print("  Personal Telemetry Seed Script")
    print("=" * 60)

    print("\nChecking database…")
    create_database_if_missing()

    engine  = create_engine(DATABASE_URL, echo=False)
    Session = sessionmaker(bind=engine)

    print("\nCreating schema (if tables do not already exist)…")
    # Uses the Base imported from models.py — same metadata the rest of
    # the application uses, so schema definitions stay perfectly in sync.
    Base.metadata.create_all(engine)
    print("  Schema OK")

    session = Session()
    try:
        known_uids = load_users(session, filepath=user_file)
        session.commit()

        load_sensors(session, known_uids=known_uids, filepath=sensor_file)

        print("\n" + "=" * 60)
        print("  Seed complete!")
        print("=" * 60)

    except Exception as e:
        session.rollback()
        print(f"\nFATAL ERROR — rolled back: {e}")
        sys.exit(1)
    finally:
        session.close()
        engine.dispose()


if __name__ == "__main__":
    u = sys.argv[1] if len(sys.argv) > 1 else "UserInfo.csv"
    s = sys.argv[2] if len(sys.argv) > 2 else "Sensors.csv"
    run_seed(user_file=u, sensor_file=s)
