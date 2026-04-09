from __future__ import annotations

import csv
import os
import sys
from datetime import datetime

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import (
    create_engine, Column, Integer, String, SmallInteger,
    Numeric, TIMESTAMP, ForeignKey, UniqueConstraint, text
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import declarative_base, sessionmaker, relationship


# Database connection

DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = os.getenv("DB_PORT",     "5432")
DB_NAME     = os.getenv("DB_NAME",     "telemetry_db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

Base = declarative_base()



# Helper functions


def clean_float(val):
    if val is None or str(val).strip() == "":
        return 0.0
    try:
        return float(str(val).strip())
    except Exception:
        return 0.0


def clean_int(val):
    if val is None or str(val).strip() == "":
        return 0
    s = str(val).strip().replace("%", "")
    try:
        return int(float(s))
    except Exception:
        return 0


def clean_str(val):
    s = str(val).strip() if val is not None else ""
    return s if s else "0"



# ORM Models


class User(Base):
    __tablename__ = "users"

    uid        = Column(String(50), primary_key=True)
    age_range  = Column(String(10))
    gender     = Column(String(50))
    university = Column(String(50))

    device_statuses = relationship("DeviceStatus", back_populates="user",
                                   cascade="all, delete-orphan")


class DeviceStatus(Base):
    __tablename__ = "device_status"
    __table_args__ = (
        UniqueConstraint("uid", "recorded_at", name="uq_device_uid_recorded"),
    )

    reading_id    = Column(Integer, primary_key=True, autoincrement=True)
    uid           = Column(String(50), ForeignKey("users.uid"), nullable=False)
    recorded_at   = Column(TIMESTAMP, nullable=True)
    battery_level = Column(SmallInteger)
    gps_latitude  = Column(Numeric(12, 7))
    gps_longitude = Column(Numeric(12, 7))

    user              = relationship("User", back_populates="device_statuses")
    motion_log        = relationship("MotionLog",        back_populates="device_status",
                                     uselist=False, cascade="all, delete-orphan")
    environmental_log = relationship("EnvironmentalLog", back_populates="device_status",
                                     uselist=False, cascade="all, delete-orphan")
    orientation_log   = relationship("OrientationLog",   back_populates="device_status",
                                     uselist=False, cascade="all, delete-orphan")


class MotionLog(Base):
    __tablename__ = "motion_logs"

    reading_id = Column(Integer, ForeignKey("device_status.reading_id"), primary_key=True)
    accel_x    = Column(Numeric(12, 7))
    accel_y    = Column(Numeric(12, 7))
    accel_z    = Column(Numeric(12, 7))
    grav_x     = Column(Numeric(12, 7))
    grav_y     = Column(Numeric(12, 7))
    grav_z     = Column(Numeric(12, 7))
    gyro_x     = Column(Numeric(12, 7))
    gyro_y     = Column(Numeric(12, 7))
    gyro_z     = Column(Numeric(12, 7))

    device_status = relationship("DeviceStatus", back_populates="motion_log")


class EnvironmentalLog(Base):
    __tablename__ = "environmental_logs"

    reading_id = Column(Integer, ForeignKey("device_status.reading_id"), primary_key=True)
    light      = Column(Numeric(10, 4))
    mag_x      = Column(Numeric(12, 7))
    mag_y      = Column(Numeric(12, 7))
    mag_z      = Column(Numeric(12, 7))

    device_status = relationship("DeviceStatus", back_populates="environmental_log")


class OrientationLog(Base):
    __tablename__ = "orientation_logs"

    reading_id = Column(Integer, ForeignKey("device_status.reading_id"), primary_key=True)
    azimuth    = Column(Numeric(12, 7))
    pitch      = Column(Numeric(12, 7))
    roll       = Column(Numeric(12, 7))

    device_status = relationship("DeviceStatus", back_populates="orientation_log")


# Auto create database if missing database
def create_database_if_missing():
    try:
        conn = psycopg2.connect(
            host=DB_HOST, port=DB_PORT,
            user=DB_USER, password=DB_PASSWORD,
            dbname="postgres"
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


# Ensure a user row exists, insert placeholder if not in UserInfo
def ensure_user(session, uid: str, known_uids: set) -> bool:
    """
    If uid is not in known_uids, insert a placeholder user with default
    values and add it to the set so it is only inserted once.
    """
    if uid in known_uids:
        return True
    if uid == "0":
        return False  # skip the completely empty UID

    stmt = (
        pg_insert(User)
        .values(uid=uid, age_range="0", gender="0", university="0")
        .on_conflict_do_nothing(constraint="users_pkey")
    )
    session.execute(stmt)
    session.flush()
    known_uids.add(uid)
    return True



# ETL for Users
def load_users(session, filepath="UserInfo.csv") -> set:
    print(f"\nLoading: {filepath}")
    known_uids = set()
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

                stmt = (
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
                        )
                    )
                )
                session.execute(stmt)
                known_uids.add(uid)

            except Exception as e:
                errors += 1
                print(f"  [row {i}] ERROR: {e}")

    session.flush()
    print(f"  Loaded  : {len(known_uids)}")
    print(f"  Errors  : {errors}")
    return known_uids



# ETL for Sensors


def load_sensors(session, known_uids: set, filepath="Sensors.csv"):
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

                # If UID missing from Users, auto-insert a placeholder with defaults
                if uid not in known_uids:
                    if not ensure_user(session, uid, known_uids):
                        errors += 1
                        continue
                    placeholder_users += 1
                    print(f"  [row {i}] Placeholder user created for UID: {uid}")

                # Parse timestamp from CSV column "Date_time", format "M/D/YYYY HH:MM"
                ts_raw = (row.get("Date_time") or "").strip()
                recorded_at = None
                if ts_raw:
                    for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y %H:%M:%S",
                                "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
                        try:
                            recorded_at = datetime.strptime(ts_raw, fmt)
                            break
                        except ValueError:
                            continue

                # Insert DeviceStatus, skip silently if (uid, recorded_at) duplicate
                stmt = (
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
                result = session.execute(stmt)
                row_id = result.scalar()

                if row_id is None:
                    duplicates += 1
                    continue

                # Insert three child log rows
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



# Entry point


def run_seed(user_file="UserInfo.csv", sensor_file="Sensors.csv"):
    print("=" * 60)
    print("  Personal Telemetry Seed Script")
    print("=" * 60)

    print("\nChecking database...")
    create_database_if_missing()

    engine  = create_engine(DATABASE_URL, echo=False)
    Session = sessionmaker(bind=engine)

    print("\nCreating schema (if tables do not already exist)...")
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
