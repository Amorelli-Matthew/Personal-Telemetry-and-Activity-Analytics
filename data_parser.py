import csv
from datetime import datetime
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models import User, DeviceStatus, MotionLog, EnvironmentalLog, OrientationLog

class DataParser:
    def __init__(self, session, user_file="UserInfo.csv", sensor_file="Sensors.csv"):
        self.session = session
        self.user_file = user_file
        self.sensor_file = sensor_file

    def _clean_float(self, val):
        try: return float(str(val).strip()) if val else 0.0
        except: return 0.0

    def run_etl(self):
        """Consolidated ETL process."""
        self.parse_users()
        self.parse_telemetry()

    def parse_telemetry(self):
        """Updated to match your actual CSV columns (lat, speed_knots, etc)."""
        with open(self.sensor_file, mode="r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                uid = row.get("UID", "Unknown")
                ts_raw = row.get("timestamp")
                
                # Insert parent using your pg_insert logic
                stmt = pg_insert(DeviceStatus).values(
                    uid=uid,
                    recorded_at=datetime.fromisoformat(ts_raw) if ts_raw else None,
                    gps_latitude=self._clean_float(row.get("lat")),
                    gps_longitude=self._clean_float(row.get("lon"))
                ).on_conflict_do_nothing().returning(DeviceStatus.reading_id)
                
                res = self.session.execute(stmt)
                rid = res.scalar()
                
                if rid:
                    # Insert child logs using actual headers
                    self.session.execute(pg_insert(MotionLog).values(
                        reading_id=rid,
                        accel_x=self._clean_float(row.get("accel_x")),
                        gyro_x=self._clean_float(row.get("gyro_x"))
                    ))
            self.session.commit()