from __future__ import annotations

import os
from datetime import datetime
from typing import Callable, Dict, List, Optional

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
from sqlalchemy.orm import joinedload, Session, sessionmaker

from models import Base, DeviceStatus, EnvironmentalLog, MotionLog, OrientationLog, User
from data_parser import DataParser

# Connection defaults
 
_DB_USER     = os.getenv("DB_USER",     "postgres")
_DB_PASSWORD = os.getenv("DB_PASSWORD", "password")
_DB_HOST     = os.getenv("DB_HOST",     "localhost")
_DB_PORT     = os.getenv("DB_PORT",     "5432")
_DB_NAME     = os.getenv("DB_NAME",     "telemetry_db")

DEFAULT_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql+psycopg2://{_DB_USER}:{_DB_PASSWORD}@{_DB_HOST}:{_DB_PORT}/{_DB_NAME}",
)


class DatabaseManager:
    """
    Wraps an SQLAlchemy Engine + Session factory.

    Attributes
    ----------
    engine  : sqlalchemy.engine.Engine
    Session : sqlalchemy.orm.sessionmaker
    """

    def __init__(self, database_url: str = DEFAULT_DATABASE_URL) -> None:
        self._database_url = database_url
        self.create_database_if_missing()
        self.engine: Engine        = create_engine(database_url, echo=False)
        self.Session: sessionmaker = sessionmaker(bind=self.engine)

    #  Database bootstrap 
    def create_database_if_missing(self) -> None:
        """
        Connect to the postgres maintenance database and create the target
        database if it does not yet exist. Called automatically on __init__
        so that init_schema() never fails with 'database does not exist'.
        """
        try:
            conn = psycopg2.connect(
                host     = _DB_HOST,
                port     = _DB_PORT,
                user     = _DB_USER,
                password = _DB_PASSWORD,
                dbname   = "postgres",
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (_DB_NAME,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{_DB_NAME}"')
                print(f"  Created database '{_DB_NAME}'")
            cur.close()
            conn.close()
        except Exception as e:
            raise RuntimeError(
                f"Could not connect to PostgreSQL at {_DB_HOST}:{_DB_PORT} "
                f"as '{_DB_USER}'. Detail: {e}"
            ) from e

    #  Schema 
    def init_schema(self) -> None:
        """Create all tables if they do not yet exist. Safe to call repeatedly."""
        Base.metadata.create_all(self.engine)

    #  Generic write 
    def bulk_save(self, objects: List[Base]) -> None:
        """
        Persist a list of ORM objects in a single transaction.

        Raises Exception
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

    #User reads 
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

    # DB status (drives the Setup tab seed button) 
    def get_db_status(self) -> dict:
        """
        Return a snapshot of how many rows exist in key tables.

        Used by the dashboard Setup tab to decide whether to enable or
        disable the seed button.  A database is considered "populated"
        when at least one User row and one DeviceStatus row exist.

        Returns a
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

    #  Analytics read 
    def get_analytics_data(
        self,
        uid:        str,
        start_date: Optional[datetime] = None,
        end_date:   Optional[datetime] = None,
    ) -> List[DeviceStatus]:
        """
        Return DeviceStatus rows for uid, optionally filtered by date range.

        When both *start_date* and end_date are None the query returns every
        reading for the user (all-time mode).  Passing either bound applies
        that filter independently.

        All child log tables are eagerly loaded so callers receive fully
        hydrated objects without triggering additional round trips.

        Returns a list[DeviceStatus]  ordered by recorded_at ascending.
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

    #  CSV ingest 
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

       Returns a dict with keys:
            inserted    – device-status rows successfully written
            duplicates  – rows skipped because (uid, recorded_at) already existed
            new_users   – placeholder User rows auto-created for unknown UIDs
            errors      – rows that raised an exception (logged to stderr)
        """
        session: Session = self.Session()
        try:
            parser = DataParser(session)
            counters = parser.parse_telemetry_from_bytes(file_obj, progress_cb=progress_cb)
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return counters
    
    def wipe_db(self) -> None:
        """Drop all tables and recreate the schema, effectively wiping all data."""
        Base.metadata.drop_all(self.engine)
        Base.metadata.create_all(self.engine)


    # ── CSV seed (file-path based; used by Setup tab) 
    def seed_from_csv(
        self,
        user_file:   str,
        sensor_file: str,
        log_cb: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, int]:
        """
        Full ETL seed from local CSV file paths.

        Loads users first, then sensor readings, in a single managed session.
        All DataParser interactions are routed through this method so that the
        software layer remains the sole gateway to the database while the
        system is running.

        Parameters
        ----------
        user_file   : path to UserInfo CSV
        sensor_file : path to Sensors CSV
        log_cb      : optional callable(str) — receives human-readable progress
                      messages that callers can stream to a UI log widget

        Returnsa dict with keys:
            users_loaded – number of user rows inserted
            inserted     – device-status rows successfully written
            duplicates   – rows skipped (uid, recorded_at) already existed
            new_users    – placeholder User rows auto-created for unknown UIDs
            errors       – rows that raised an exception
        """
        def _log(msg: str) -> None:
            if log_cb:
                log_cb(msg)

        session: Session = self.Session()
        counters: Dict[str, int] = {"inserted": 0, "duplicates": 0, "new_users": 0, "errors": 0}
        known_uids: set = set()
        try:
            parser = DataParser(session, user_file=user_file, sensor_file=sensor_file)

            _log(f"Loading users: {user_file}")
            known_uids = parser.parse_users()
            session.commit()
            _log(f"  Loaded {len(known_uids)} user(s)")

            _log(f"Loading sensors: {sensor_file}")
            with open(sensor_file, "rb") as f:
                counters = parser.parse_telemetry_from_bytes(f)
            _log(
                f"  Sensor load complete — {counters['inserted']:,} inserted, "
                f"{counters['duplicates']:,} duplicates, {counters['errors']:,} errors"
            )

        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

        return {"users_loaded": len(known_uids), **counters}

    #Context manager 
    def __enter__(self) -> "DatabaseManager":
        return self

    def __exit__(self, *_) -> None:
        self.engine.dispose()

