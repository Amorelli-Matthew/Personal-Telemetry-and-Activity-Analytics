import csv
import io
import sys
from datetime import datetime
from typing import Callable, Dict, Optional, Set

from sqlalchemy.dialects.postgresql import insert as pg_insert

from models import (
    DeviceStatus,
    EnvironmentalLog,
    MotionLog,
    OrientationLog,
    User,
)

# Timestamp formats accepted in both CSV sources
_TS_FORMATS = (
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d %b %Y %I:%M:%S %p",
)

_BATCH_SIZE = 3000


class DataParser:
    def __init__(self, session, user_file="UserInfo.csv", sensor_file="Sensors.csv", error_file="invalid_rows.csv"):
        self.session     = session
        self.user_file   = user_file
        self.sensor_file = sensor_file
        self.error_file  = error_file

    #Helpers

    @staticmethod
    def _clean_float(val) -> float:
        try:
            return float(str(val).strip()) if val else 0.0
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
        if not raw:
            return None
        raw = raw.strip()
        clean_raw = raw.replace("a.m.", "AM").replace("p.m.", "PM")
        clean_raw = clean_raw.replace("A.M.", "AM").replace("P.M.", "PM")

        for fmt in _TS_FORMATS:
            try:
                return datetime.strptime(clean_raw, fmt)
            except ValueError:
                continue
        return None

    # Users

    def parse_users(self) -> Set[str]:
        known_uids: Set[str] = set()
        with open(self.user_file, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                uid = str(row.get("UID", "")).strip()
                if not uid:
                    continue

                stmt = pg_insert(User).values(
                    uid        = uid,
                    age_range  = self._clean_str(row.get("Age_Range") or row.get("age_range") or row.get("age")),
                    gender     = self._clean_str(row.get("Gender")    or row.get("gender")),
                    university = self._clean_str(row.get("University") or row.get("university")),
                ).on_conflict_do_nothing()

                self.session.execute(stmt)
                known_uids.add(uid)

        self.session.flush()
        return known_uids

    # Telemetry Ingestion

    def parse_telemetry(self, known_uids: Optional[Set[str]] = None) -> None:
        with open(self.sensor_file, mode="r", encoding="utf-8-sig") as f:
            self._ingest_rows(csv.DictReader(f), known_uids=known_uids or set())

    def parse_telemetry_from_bytes(
        self,
        file_obj,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> Dict[str, int]:
        raw = file_obj.read() if hasattr(file_obj, "read") else file_obj
        text = raw.decode("utf-8-sig")

        total_rows = sum(1 for _ in io.StringIO(text)) - 1
        total_rows = max(total_rows, 1)

        reader = csv.DictReader(io.StringIO(text))
        counters = {"inserted": 0, "duplicates": 0, "new_users": 0, "errors": 0}

        known_uids: Set[str] = set()
        for user in self.session.query(User).all():
            known_uids.add(user.uid)

        self._ingest_rows(
            reader,
            known_uids   = known_uids,
            counters     = counters,
            total_rows   = total_rows,
            progress_cb  = progress_cb,
        )
        return counters

    def _ingest_rows(
        self,
        reader,
        known_uids:  Set[str],
        counters:    Optional[Dict[str, int]] = None,
        total_rows:  int = 0,
        progress_cb: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        track = counters is not None
        row_num = 0

        with open(self.error_file, mode="w", encoding="utf-8", newline="") as err_f:
            writer = None

            for row in reader:
                row_num += 1
                
                if writer is None:
                    writer = csv.DictWriter(err_f, fieldnames=row.keys())
                    writer.writeheader()

                try:
                    # START SUB-TRANSACTION: One row error won't kill the batch
                    with self.session.begin_nested():
                        uid    = str(row.get("UID", "Unknown")).strip()
                        ts_raw = (row.get("Date_time") or row.get("timestamp") or "").strip()
                        recorded_at = self._parse_ts(ts_raw) if ts_raw else None

                        if not recorded_at:
                            raise ValueError(f"Missing/unparseable timestamp: '{ts_raw}'")

                        if uid not in known_uids:
                            self.session.execute(
                                pg_insert(User).values(
                                    uid=uid, age_range="0", gender="0", university="0"
                                ).on_conflict_do_nothing()
                            )
                            known_uids.add(uid)
                            if track: counters["new_users"] += 1

                        ds_stmt = (
                            pg_insert(DeviceStatus)
                            .values(
                                uid           = uid,
                                recorded_at   = recorded_at,
                                battery_level = self._clean_int(row.get("BATTERY_LEVEL")),
                                gps_latitude  = self._clean_float(row.get("SENSORGPS_LATITUDE") or row.get("lat")),
                                gps_longitude = self._clean_float(row.get("SENSORGPS_LONGITUDE") or row.get("lon")),
                            )
                            .on_conflict_do_nothing()
                            .returning(DeviceStatus.reading_id)
                        )

                        res = self.session.execute(ds_stmt)
                        rid = res.scalar()

                        if rid is None:
                            if track: counters["duplicates"] += 1
                            writer.writerow(row)
                        else:
                            # Log linked sensor data
                            self.session.execute(
                                pg_insert(MotionLog).values(
                                    reading_id=rid,
                                    accel_x=self._clean_float(row.get("ACCELEROMETER_X")),
                                    accel_y=self._clean_float(row.get("ACCELEROMETER_Y")),
                                    accel_z=self._clean_float(row.get("ACCELEROMETER_Z")),
                                    grav_x=self._clean_float(row.get("GRAV_X")),
                                    grav_y=self._clean_float(row.get("GRAV_Y")),
                                    grav_z=self._clean_float(row.get("GRAV_Z")),
                                    gyro_x=self._clean_float(row.get("GYROSCOPE_X")),
                                    gyro_y=self._clean_float(row.get("GYROSCOPE_Y")),
                                    gyro_z=self._clean_float(row.get("GYROSCOPE_Z")),
                                ).on_conflict_do_nothing()
                            )
                            self.session.execute(
                                pg_insert(EnvironmentalLog).values(
                                    reading_id=rid,
                                    light=self._clean_float(row.get("Light_v")),
                                    mag_x=self._clean_float(row.get("MAG_X")),
                                    mag_y=self._clean_float(row.get("MAG_Y")),
                                    mag_z=self._clean_float(row.get("MAG_Z")),
                                ).on_conflict_do_nothing()
                            )
                            self.session.execute(
                                pg_insert(OrientationLog).values(
                                    reading_id=rid,
                                    azimuth=self._clean_float(row.get("ORIENTATION_AZIMUTH")),
                                    pitch=self._clean_float(row.get("ORIENTATION_PITCH")),
                                    roll=self._clean_float(row.get("ORIENTATION_ROLL")),
                                ).on_conflict_do_nothing()
                            )
                            if track: counters["inserted"] += 1

                except Exception as exc:
                    # Context manager automatically rolled back the savepoint here
                    writer.writerow(row)
                    print(f"  [row {row_num}] ERROR — exported to {self.error_file}: {exc}", file=sys.stderr)
                    if track: counters["errors"] += 1
                    continue
                
                # Batch commit for performance
                if row_num % _BATCH_SIZE == 0:
                    self.session.commit()
                    if progress_cb and total_rows: progress_cb(row_num, total_rows)

        self.session.commit()
        if progress_cb and total_rows: progress_cb(row_num, total_rows)

    def run_etl(self) -> None:
        """Convenience wrapper: parse users then telemetry in one call."""
        known_uids = self.parse_users()
        self.session.commit()
        self.parse_telemetry(known_uids=known_uids)