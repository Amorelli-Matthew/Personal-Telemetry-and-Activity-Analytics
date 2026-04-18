"""
models.py
---------
SQLAlchemy ORM models for the Personal Telemetry & Activity Analytics platform.
Matches the 5-table ERD (3NF):
    Users  →  Device_Status  →  Motion_Logs
                              →  Environmental_Logs
                              →  Orientation_Logs
"""

from __future__ import annotations

from sqlalchemy import (
    Column, Integer, String, SmallInteger,
    Numeric, TIMESTAMP, ForeignKey, UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# ─────────────────────────────────────────────
# Users
# ─────────────────────────────────────────────
class User(Base):
    """
    Stores static demographic information for each participant.
    PK: uid (VARCHAR 50)
    """
    __tablename__ = "users"

    uid        = Column(String(50), primary_key=True)
    age_range  = Column(String(10))
    gender     = Column(String(50))
    university = Column(String(50))

    # One user → many device readings
    device_statuses = relationship(
        "DeviceStatus",
        back_populates="user",
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return f"<User uid={self.uid!r} age={self.age_range!r}>"


# ─────────────────────────────────────────────
# Device Status  (central fact table)
# ─────────────────────────────────────────────
class DeviceStatus(Base):
    """
    One row per sensor snapshot.
    PK:  reading_id (auto-increment INT)
    FK:  uid → users.uid
    UQ:  (uid, recorded_at)   – prevents duplicate timestamps per device
    """
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

    # Parent
    user = relationship("User", back_populates="device_statuses")

    # Children (one-to-one per reading)
    motion_log        = relationship(
        "MotionLog",
        back_populates="device_status",
        uselist=False,
        cascade="all, delete-orphan",
    )
    environmental_log = relationship(
        "EnvironmentalLog",
        back_populates="device_status",
        uselist=False,
        cascade="all, delete-orphan",
    )
    orientation_log   = relationship(
        "OrientationLog",
        back_populates="device_status",
        uselist=False,
        cascade="all, delete-orphan",
    )

    def __repr__(self) -> str:
        return (
            f"<DeviceStatus id={self.reading_id} uid={self.uid!r} "
            f"at={self.recorded_at} battery={self.battery_level}>"
        )


# ─────────────────────────────────────────────
# Motion Logs  (accelerometer + gravity + gyro)
# ─────────────────────────────────────────────
class MotionLog(Base):
    """
    Accelerometer, gravity vector, and gyroscope readings.
    PK/FK: reading_id → device_status.reading_id
    All sensor columns: NUMERIC(12, 7)
    """
    __tablename__ = "motion_logs"

    reading_id = Column(
        Integer, ForeignKey("device_status.reading_id"), primary_key=True
    )

    # Accelerometer
    accel_x = Column(Numeric(12, 7))
    accel_y = Column(Numeric(12, 7))
    accel_z = Column(Numeric(12, 7))

    # Gravity vector
    grav_x = Column(Numeric(12, 7))
    grav_y = Column(Numeric(12, 7))
    grav_z = Column(Numeric(12, 7))

    # Gyroscope
    gyro_x = Column(Numeric(12, 7))
    gyro_y = Column(Numeric(12, 7))
    gyro_z = Column(Numeric(12, 7))

    device_status = relationship("DeviceStatus", back_populates="motion_log")

    def __repr__(self) -> str:
        return f"<MotionLog reading_id={self.reading_id}>"


# ─────────────────────────────────────────────
# Environmental Logs  (light + magnetometer)
# ─────────────────────────────────────────────
class EnvironmentalLog(Base):
    """
    Ambient light sensor and 3-axis magnetometer readings.
    PK/FK: reading_id → device_status.reading_id
    Light: NUMERIC(10, 4)  |  Mag axes: NUMERIC(12, 7)
    """
    __tablename__ = "environmental_logs"

    reading_id = Column(
        Integer, ForeignKey("device_status.reading_id"), primary_key=True
    )

    light = Column(Numeric(10, 4))   # lux — narrower precision per ERD
    mag_x = Column(Numeric(12, 7))
    mag_y = Column(Numeric(12, 7))
    mag_z = Column(Numeric(12, 7))

    device_status = relationship("DeviceStatus", back_populates="environmental_log")

    def __repr__(self) -> str:
        return f"<EnvironmentalLog reading_id={self.reading_id} light={self.light}>"


# ─────────────────────────────────────────────
# Orientation Logs  (azimuth / pitch / roll)
# ─────────────────────────────────────────────
class OrientationLog(Base):
    """
    Device orientation derived from the sensor fusion algorithm.
    PK/FK: reading_id → device_status.reading_id
    All columns: NUMERIC(12, 7)
    """
    __tablename__ = "orientation_logs"

    reading_id = Column(
        Integer, ForeignKey("device_status.reading_id"), primary_key=True
    )

    azimuth = Column(Numeric(12, 7))   # compass heading  (°)
    pitch   = Column(Numeric(12, 7))   # front-back tilt  (°)
    roll    = Column(Numeric(12, 7))   # left-right tilt  (°)

    device_status = relationship("DeviceStatus", back_populates="orientation_log")

    def __repr__(self) -> str:
        return (
            f"<OrientationLog reading_id={self.reading_id} "
            f"az={self.azimuth} p={self.pitch} r={self.roll}>"
        )
