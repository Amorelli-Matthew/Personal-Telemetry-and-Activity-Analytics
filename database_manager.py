"""
database_manager.py
-------------------
Handles all database connectivity and schema lifecycle for the
Personal Telemetry & Activity Analytics platform.

Public API
----------
  init_schema()                        – create tables if missing
  bulk_save(objects)                   – generic ORM bulk insert
  get_all_users()                      – all User rows (drives multiselect)
  get_analytics_data(uid, start, end)  – DeviceStatus rows + child logs
  ingest_csv(file_obj, progress_cb)    – parse & persist an uploaded CSV file
"""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime
from typing import BinaryIO, Callable, Dict, List, Optional

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import joinedload, Session, sessionmaker

from models import Base, DeviceStatus, EnvironmentalLog, MotionLog, OrientationLog, User


# ─────────────────────────────────────────────────────────────────────────────
# Connection defaults — every value overridable via environment variable
# ─────────────────────────────────────────────────────────────────────────────
_DB_USER     = os.getenv("DB_USER",     "postgres")
_DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
_DB_HOST     = os.getenv("DB_HOST",     "localhost")
_DB_PORT     = os.getenv("DB_PORT",     "5432")
_DB_NAME     = os.getenv("DB_NAME",     "telemetry_db")

DEFAULT_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{_DB_USER}:{_DB_PASSWORD}@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}",
)

# Timestamp formats accepted in uploaded CSV files
_TS_FORMATS = (
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)

# Commit every N successfully inserted readings to cap memory use
_BATCH_SIZE = 250


class DatabaseManager:
    """
    Wraps an SQLAlchemy Engine + Session factory.

    Attributes
    ----------
    engine  : sqlalchemy.engine.Engine
    Session : sqlalchemy.orm.sessionmaker
    """

    def __init__(self, database_url: str = DEFAULT_DATABASE_URL) -> None:
        self.engine: Engine        = create_engine(database_url, echo=False)
        self.Session: sessionmaker = sessionmaker(bind=self.engine)

    # ── Schema ────────────────────────────────────────────────────────────────
    def init_schema(self) -> None:
        """Create all tables if they do not yet exist. Safe to call repeatedly."""
        Base.metadata.create_all(self.engine)

    # ── Generic write ────────────────────────────────────────────────────────
    def bulk_save(self, objects: List[Base]) -> None:
        """
        Persist a list of ORM objects in a single transaction.

        Raises
        ------
        Exception
            Re-raises any database error after rolling back.
        """
        session: Session = self.Session()
        try:
            session.add_all(objects)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ── User reads ───────────────────────────────────────────────────────────
    def get_all_users(self) -> List[User]:
        """
        Return every row from the users table, ordered by uid.

        Called on startup to populate the multiselect widget with real
        UIDs from the database rather than requiring manual entry.
        """
        session: Session = self.Session()
        try:
            users = (
                session.query(User)
                .order_by(User.uid.asc())
                .all()
            )
            session.expunge_all()
            return users
        finally:
            session.close()

    # ── DB status (drives the Setup tab seed button) ─────────────────────
    def get_db_status(self) -> dict:
        """
        Return a snapshot of how many rows exist in key tables.

        Used by the dashboard Setup tab to decide whether to enable or
        disable the seed button.  A database is considered "populated"
        when at least one User row and one DeviceStatus row exist.

        Returns
        -------
        dict with keys:
            user_count         : int
            device_status_count: int
            is_populated       : bool  – True when both tables have rows
        """
        session: Session = self.Session()
        try:
            user_count          = session.query(User).count()
            device_status_count = session.query(DeviceStatus).count()
            return {
                "user_count":          user_count,
                "device_status_count": device_status_count,
                "is_populated":        user_count > 0 and device_status_count > 0,
            }
        finally:
            session.close()

    # ── Analytics read ───────────────────────────────────────────────────────
    def get_analytics_data(
        self,
        uid:        str,
        start_date: Optional[datetime] = None,
        end_date:   Optional[datetime] = None,
    ) -> List[DeviceStatus]:
        """
        Return DeviceStatus rows for *uid*, optionally filtered by date range.

        When both *start_date* and *end_date* are None the query returns every
        reading for the user (all-time mode).  Passing either bound applies
        that filter independently.

        All child log tables are eagerly loaded so callers receive fully
        hydrated objects without triggering additional round trips.

        Returns
        -------
        list[DeviceStatus]  ordered by recorded_at ascending.
        """
        session: Session = self.Session()
        try:
            q = (
                session.query(DeviceStatus)
                .filter(DeviceStatus.uid == uid)
            )
            if start_date is not None:
                q = q.filter(DeviceStatus.recorded_at >= start_date)
            if end_date is not None:
                q = q.filter(DeviceStatus.recorded_at <= end_date)

            results = (
                q
                .options(
                    joinedload(DeviceStatus.motion_log),
                    joinedload(DeviceStatus.environmental_log),
                    joinedload(DeviceStatus.orientation_log),
                )
                .order_by(DeviceStatus.recorded_at.asc())
                .all()
            )
            session.expunge_all()
            return results
        finally:
            session.close()

    # ── CSV ingest ───────────────────────────────────────────────────────────
    def ingest_csv(
        self,
        file_obj:    BinaryIO,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> Dict[str, int]:
        """
        Parse a user-uploaded CSV and persist every new row to the database.

        Expected columns (order does not matter, extra columns are ignored)::

            UID, Date_time,
            ACCELEROMETER_X/Y/Z, GRAV_X/Y/Z, GYROSCOPE_X/Y/Z,
            BATTERY_LEVEL, SENSORGPS_LATITUDE, SENSORGPS_LONGITUDE,
            Light_v, MAG_X/Y/Z,
            ORIENTATION_AZIMUTH/PITCH/ROLL

        Conflict strategy
        -----------------
        * Users      – INSERT … ON CONFLICT DO NOTHING (preserve existing demographics)
        * DeviceStatus – INSERT … ON CONFLICT DO NOTHING on (uid, recorded_at)
        * Child logs   – INSERT … ON CONFLICT DO NOTHING on their PK

        Parameters
        ----------
        file_obj : binary file-like object
            The raw bytes from ``st.file_uploader``.
        progress_cb : callable(current_row, total_rows) | None
            Optional callback invoked after each batch commit so callers
            can drive a progress bar.

        Returns
        -------
        dict with keys:
            inserted    – device-status rows successfully written
            duplicates  – rows skipped because (uid, recorded_at) already existed
            new_users   – placeholder User rows auto-created for unknown UIDs
            errors      – rows that raised an exception (logged to stderr)
        """
        # ── Decode bytes → text for csv.DictReader ────────────────────────
        text    = file_obj.read().decode("utf-8-sig", errors="replace")
        reader  = csv.DictReader(io.StringIO(text))
        all_rows = list(reader)               # materialise so we know total count
        total    = len(all_rows)

        counters = dict(inserted=0, duplicates=0, new_users=0, errors=0)

        session: Session = self.Session()
        known_uids: set[str] = {
            uid for (uid,) in session.query(User.uid).all()
        }

        try:
            for row_num, row in enumerate(all_rows, start=1):
                if not any(row.values()):
                    continue

                try:
                    uid = self._clean_str(row.get("UID"))
                    if uid == "0":
                        counters["errors"] += 1
                        continue

                    # ── Ensure user row exists ────────────────────────────
                    if uid not in known_uids:
                        session.execute(
                            pg_insert(User)
                            .values(uid=uid, age_range="0", gender="0", university="0")
                            .on_conflict_do_nothing(constraint="users_pkey")
                        )
                        session.flush()
                        known_uids.add(uid)
                        counters["new_users"] += 1

                    # ── Parse timestamp ───────────────────────────────────
                    recorded_at = self._parse_ts(row.get("Date_time") or "")

                    # ── DeviceStatus ──────────────────────────────────────
                    result = session.execute(
                        pg_insert(DeviceStatus)
                        .values(
                            uid           = uid,
                            recorded_at   = recorded_at,
                            battery_level = self._clean_int(row.get("BATTERY_LEVEL")),
                            gps_latitude  = self._clean_float(row.get("SENSORGPS_LATITUDE")),
                            gps_longitude = self._clean_float(row.get("SENSORGPS_LONGITUDE")),
                        )
                        .on_conflict_do_nothing(constraint="uq_device_uid_recorded")
                        .returning(DeviceStatus.reading_id)
                    )
                    reading_id = result.scalar()

                    if reading_id is None:          # duplicate timestamp for this user
                        counters["duplicates"] += 1
                        continue

                    # ── MotionLog ─────────────────────────────────────────
                    session.execute(
                        pg_insert(MotionLog)
                        .values(
                            reading_id = reading_id,
                            accel_x    = self._clean_float(row.get("ACCELEROMETER_X")),
                            accel_y    = self._clean_float(row.get("ACCELEROMETER_Y")),
                            accel_z    = self._clean_float(row.get("ACCELEROMETER_Z")),
                            grav_x     = self._clean_float(row.get("GRAV_X")),
                            grav_y     = self._clean_float(row.get("GRAV_Y")),
                            grav_z     = self._clean_float(row.get("GRAV_Z")),
                            gyro_x     = self._clean_float(row.get("GYROSCOPE_X")),
                            gyro_y     = self._clean_float(row.get("GYROSCOPE_Y")),
                            gyro_z     = self._clean_float(row.get("GYROSCOPE_Z")),
                        )
                        .on_conflict_do_nothing()
                    )

                    # ── EnvironmentalLog ──────────────────────────────────
                    session.execute(
                        pg_insert(EnvironmentalLog)
                        .values(
                            reading_id = reading_id,
                            light      = self._clean_float(row.get("Light_v")),
                            mag_x      = self._clean_float(row.get("MAG_X")),
                            mag_y      = self._clean_float(row.get("MAG_Y")),
                            mag_z      = self._clean_float(row.get("MAG_Z")),
                        )
                        .on_conflict_do_nothing()
                    )

                    # ── OrientationLog ────────────────────────────────────
                    session.execute(
                        pg_insert(OrientationLog)
                        .values(
                            reading_id = reading_id,
                            azimuth    = self._clean_float(row.get("ORIENTATION_AZIMUTH")),
                            pitch      = self._clean_float(row.get("ORIENTATION_PITCH")),
                            roll       = self._clean_float(row.get("ORIENTATION_ROLL")),
                        )
                        .on_conflict_do_nothing()
                    )

                    counters["inserted"] += 1

                except Exception as exc:
                    counters["errors"] += 1
                    session.rollback()
                    print(f"  [CSV row {row_num}] ERROR: {exc}")
                    # Reinitialise session after rollback so remaining rows can proceed
                    session = self.Session()
                    known_uids = {uid for (uid,) in session.query(User.uid).all()}
                    continue

                # ── Batch commit ──────────────────────────────────────────
                if counters["inserted"] % _BATCH_SIZE == 0:
                    session.commit()
                    if progress_cb:
                        progress_cb(row_num, total)

            session.commit()
            if progress_cb:
                progress_cb(total, total)

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return counters

    # ── Private helpers ───────────────────────────────────────────────────────
    @staticmethod
    def _clean_float(val) -> float:
        if val is None or str(val).strip() == "":
            return 0.0
        try:
            return float(str(val).strip())
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _clean_int(val) -> int:
        if val is None or str(val).strip() == "":
            return 0
        try:
            return int(float(str(val).strip().replace("%", "")))
        except (ValueError, TypeError):
            return 0

    @staticmethod
    def _clean_str(val) -> str:
        s = str(val).strip() if val is not None else ""
        return s if s else "0"

    @staticmethod
    def _parse_ts(raw: str) -> Optional[datetime]:
        raw = raw.strip()
        for fmt in _TS_FORMATS:
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
        return None

    # ── Context manager ──────────────────────────────────────────────────────
    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, *_) -> None:
        self.engine.dispose()
