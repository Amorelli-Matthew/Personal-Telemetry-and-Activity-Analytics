"""
analytics_dashboard.py
-----------------------
Streamlit-based Analytics Dashboard for the Personal Telemetry platform.

Layout
------
  Tab 1 – 📊 Analytics   : multiselect users, date range, all sensor charts + summary table
  Tab 2 – 📤 Upload CSV  : file uploader, column validation, live progress bar, result report
  Tab 3 – ⚙️  Setup      : seed the database from local CSV files; button greys out once populated

Usage
-----
    streamlit run analytics_dashboard.py
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from database_manager import DatabaseManager
from models import DeviceStatus, User
import seed as _seed_module

# Colour palette — one colour per user, cycles after 10
_PALETTE = [
    "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
    "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52",
]

# The exact columns the ingest pipeline expects
REQUIRED_COLUMNS = {
    "UID", "Date_time",
    "ACCELEROMETER_X", "ACCELEROMETER_Y", "ACCELEROMETER_Z",
    "BATTERY_LEVEL",
    "GRAV_X", "GRAV_Y", "GRAV_Z",
    "GYROSCOPE_X", "GYROSCOPE_Y", "GYROSCOPE_Z",
    "Light_v",
    "MAG_X", "MAG_Y", "MAG_Z",
    "ORIENTATION_AZIMUTH", "ORIENTATION_PITCH", "ORIENTATION_ROLL",
    "SENSORGPS_LATITUDE", "SENSORGPS_LONGITUDE",
}


class AnalyticsDashboard:
    """
    Wraps the entire Streamlit UI and Plotly visualisation layer.

    Attributes
    ----------
    db_manager  : DatabaseManager
    sensor_file : str  – reserved for future re-seed hooks
    """

    def __init__(
        self,
        db_manager:  DatabaseManager,
        sensor_file: str = "Sensors.csv",
    ) -> None:
        self.db_manager:  DatabaseManager = db_manager
        self.sensor_file: str             = sensor_file

    # Entry-point
    def run(self) -> None:
        """Top-level method invoked by Streamlit on every rerender."""
        st.set_page_config(
            page_title = "Personal Telemetry Analytics",
            page_icon  = "📡",
            layout     = "wide",
        )
        st.title("📡 Personal Telemetry & Activity Analytics")


        tab_analytics, tab_upload, tab_setup = st.tabs(
            ["📊 Analytics", "📤 Upload CSV", "⚙️ Setup"]
        )

        with tab_analytics:
            self._render_analytics_tab()

        with tab_upload:
            self._render_upload_tab()

        with tab_setup:
            self._render_setup_tab()

    # Tab 1 — Analytics
    def _render_analytics_tab(self) -> None:
        all_users = self._load_users_cached()

        selected_uids, start_dt, end_dt = self._render_sidebar(all_users)

        if not selected_uids:
            st.info("Select at least one user from the sidebar to get started.")
            return

        # Guard: date range mode but dates are invalid (start > end)
        if start_dt is None and end_dt is None:
            date_label = "all available dates"
        elif start_dt is not None and end_dt is not None:
            date_label = f"{start_dt.date()} → {end_dt.date()}"
        else:
            return  # start > end edge case — sidebar already shows error

        user_data: Dict[str, List[DeviceStatus]] = {}
        with st.spinner(f"Querying database for {len(selected_uids)} user(s) ({date_label})…"):
            for uid in selected_uids:
                user_data[uid] = self.db_manager.get_analytics_data(uid, start_dt, end_dt)

        total = sum(len(v) for v in user_data.values())
        if total == 0:
            st.warning("No readings found for the selected users" + (" in this date range." if start_dt else "."))
            return

        counts_md = "  |  ".join(
            f"`{uid}` — **{len(rows):,}** readings"
            for uid, rows in user_data.items()
        )
        st.success(f"Loaded **{total:,}** total readings   |   {counts_md}")

        self._display_plots(user_data)
        self._display_summary_table(user_data, all_users)

    # Tab 2 — Upload CSV
    def _render_upload_tab(self) -> None:
        st.header("Upload Sensor CSV")
        st.write(
            "Upload a CSV file in the standard format to add new readings directly "
            "into the database. Duplicate timestamps for the same user are silently "
            "skipped so re-uploads are always safe."
        )

        # ── Expected format reference ─────────────────────────────────────
        with st.expander("📋  Expected column format", expanded=False):
            st.code(
                "UID,ACCELEROMETER_X,ACCELEROMETER_Y,ACCELEROMETER_Z,"
                "BATTERY_LEVEL,Date_time,GRAV_X,GRAV_Y,GRAV_Z,"
                "GYROSCOPE_X,GYROSCOPE_Y,GYROSCOPE_Z,Light_v,"
                "MAG_X,MAG_Y,MAG_Z,ORIENTATION_AZIMUTH,ORIENTATION_PITCH,"
                "ORIENTATION_ROLL,SENSORGPS_LATITUDE,SENSORGPS_LONGITUDE",
                language="text",
            )
            st.caption(
                "Column order does not matter. Extra columns are ignored. "
                "Date_time accepts formats: `M/D/YYYY HH:MM`, `YYYY-MM-DD HH:MM:SS`, or ISO 8601."
            )

        st.divider()

        # ── File uploader ─────────────────────────────────────────────────
        uploaded_file = st.file_uploader(
            label = "Choose a CSV file",
            type  = ["csv"],
            help  = "File must contain at minimum the columns listed above.",
        )

        if uploaded_file is None:
            st.info("No file selected yet.")
            return

        # ── Read & validate ───────────────────────────────────────────────
        try:
            raw_bytes = uploaded_file.read()
            preview_df = pd.read_csv(io.BytesIO(raw_bytes), nrows=5, encoding="utf-8-sig")
        except Exception as exc:
            st.error(f"Could not parse the file: {exc}")
            return

        st.subheader("Preview  (first 5 rows)")
        st.dataframe(preview_df, use_container_width=True)

        # Check for missing required columns
        file_cols   = set(preview_df.columns)
        missing     = REQUIRED_COLUMNS - file_cols
        extra       = file_cols - REQUIRED_COLUMNS

        col_ok, col_warn = st.columns(2)
        if missing:
            col_ok.error(f"❌  Missing columns ({len(missing)}): `{'`, `'.join(sorted(missing))}`")
        else:
            col_ok.success("✅  All required columns present.")

        if extra:
            col_warn.warning(f"ℹ️  Extra columns will be ignored: `{'`, `'.join(sorted(extra))}`")

        # Count total rows for display
        try:
            total_rows = sum(1 for _ in pd.read_csv(
                io.BytesIO(raw_bytes), chunksize=1000, encoding="utf-8-sig"
            ))
        except Exception:
            total_rows = 0

        st.caption(f"**File:** `{uploaded_file.name}`  |  **Rows detected:** {total_rows:,}")

        if missing:
            st.warning("Fix the missing columns before loading.")
            return

        st.divider()

        #Load button 
        if st.button("⬆️  Load into Database", type="primary", use_container_width=True):
            self._run_ingest(raw_bytes, uploaded_file.name, total_rows)

    # Ingest runner (called from upload tab after button press)
    def _run_ingest(self, raw_bytes: bytes, filename: str, total_rows: int) -> None:
        """
        Drive DatabaseManager.ingest_csv() with a live Streamlit progress bar
        and render a detailed result report when done.
        """
        st.write(f"Processing **{filename}** …")

        progress_bar  = st.progress(0, text="Starting…")
        status_text   = st.empty()

        def _progress_cb(current: int, total: int) -> None:
            pct  = int((current / total) * 100) if total else 100
            frac = current / total if total else 1.0
            progress_bar.progress(frac, text=f"Processing row {current:,} of {total:,}…")
            status_text.caption(f"{pct}% complete")

        try:
            result = self.db_manager.ingest_csv(
                file_obj    = io.BytesIO(raw_bytes),
                progress_cb = _progress_cb,
            )
        except Exception as exc:
            progress_bar.empty()
            status_text.empty()
            st.error(f"Ingest failed: {exc}")
            return

        progress_bar.progress(1.0, text="Done!")
        status_text.empty()

        # Result report 
        st.divider()
        st.subheader("📋 Ingest Report")

        r1, r2, r3, r4 = st.columns(4)
        r1.metric("✅  Inserted",    result["inserted"],   help="New device-status rows written to the DB.")
        r2.metric("⏭️  Duplicates",  result["duplicates"], help="Rows skipped — (UID, timestamp) already existed.")
        r3.metric("👤  New Users",   result["new_users"],  help="Placeholder user rows auto-created for unknown UIDs.")
        r4.metric("❌  Errors",      result["errors"],     help="Rows that raised an exception during processing.")

        if result["inserted"] > 0:
            st.success(
                f"Successfully added **{result['inserted']:,}** new reading(s). "
                "Switch to the 📊 Analytics tab to query them."
            )
            # Clear cached user list so multiselect reflects any newly added UIDs
            st.session_state.pop("all_users", None)

        if result["duplicates"] > 0:
            st.info(
                f"**{result['duplicates']:,}** row(s) were already in the database and were skipped. "
                "This is expected when re-uploading a file."
            )

        if result["new_users"] > 0:
            st.warning(
                f"**{result['new_users']}** new UID(s) were created with placeholder demographics "
                "(age_range=0, gender=0, university=0). You can update them directly in the database."
            )

        if result["errors"] > 0:
            st.error(
                f"**{result['errors']}** row(s) failed to insert. Check the terminal / logs for details."
            )

    # ─────────────────────────────────────────────────────────────────────────
    # Tab 3 — Setup
    # ─────────────────────────────────────────────────────────────────────────
    def _render_setup_tab(self) -> None:
        """
        One-time database seeding UI.

        Checks whether the database already has data via get_db_status().
        If populated, the seed button is rendered disabled with a success
        banner explaining why.  If empty, the user can provide CSV paths
        and click the button to run the full ETL pipeline.
        """
        st.header("⚙️ Database Setup")
        st.write(
            "Use this tab to seed the database from the Kaggle Embedded "
            "Smartphone Sensor Dataset CSV files.  The seed operation only "
            "needs to run once — the button is disabled automatically once "
            "data exists."
        )
        st.divider()

        # ── Check current DB state ────────────────────────────────────────
        try:
            status = self.db_manager.get_db_status()
        except Exception as exc:
            st.error(f"Could not reach the database: {exc}")
            return

        already_populated = status["is_populated"]

        # ── Status metrics 
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Users in DB",    status["user_count"])
        col_b.metric("Readings in DB", f'{status["device_status_count"]:,}')
        col_c.metric(
            "Status",
            "✅ Populated" if already_populated else "⚠️ Empty",
        )

        st.divider()

        if already_populated:
            st.success(
                f"The database already contains **{status['user_count']} users** "
                f"and **{status['device_status_count']:,} readings**. "
                "Seeding is disabled to prevent unintentional re-runs. "
                "To re-seed, clear the database manually and refresh this page."
            )

        #  CSV path inputs 
        st.subheader("CSV File Paths")
        st.caption(
            "Enter paths relative to the directory where you run "
            "`streamlit run analytics_dashboard.py`, or use absolute paths."
        )

        user_file = st.text_input(
            "UserInfo CSV path",
            value    = "UserInfo.csv",
            disabled = already_populated,
            help     = "Demographic data: UID, age, gender, university",
        )
        sensor_file = st.text_input(
            "Sensors CSV path",
            value    = "Sensors.csv",
            disabled = already_populated,
            help     = "Sensor readings: accelerometer, gyroscope, GPS, battery, etc.",
        )

        # Validate file paths before enabling the button 
        user_exists   = os.path.isfile(user_file)
        sensor_exists = os.path.isfile(sensor_file)
        paths_ok      = user_exists and sensor_exists

        if not already_populated:
            if not user_exists:
                st.warning(f"File not found: `{user_file}`")
            if not sensor_exists:
                st.warning(f"File not found: `{sensor_file}`")

        st.divider()

        # Seed button 
        # Streamlit renders a greyed-out button when disabled=True
        btn_label = "🌱 Seed Database" if not already_populated else "✅ Already Seeded"
        btn_clicked = st.button(
            btn_label,
            type                = "primary",
            disabled            = already_populated or not paths_ok,
            use_container_width = True,
            help                = (
                "Disabled — database is already populated."
                if already_populated
                else "Provide valid CSV paths above to enable."
                if not paths_ok
                else "Load UserInfo.csv and Sensors.csv into the database."
            ),
        )

        if btn_clicked and not already_populated and paths_ok:
            self._run_seed(user_file, sensor_file)

    def _run_seed(self, user_file: str, sensor_file: str) -> None:
        """
        Execute the seed ETL pipeline in-process and stream log lines to the UI.

        Delegates to seed.py's load_users() and load_sensors() functions,
        which use the shared ORM models from models.py.  After completion the
        user cache is cleared so the Analytics multiselect picks up new UIDs.
        """
        log_lines:       list[str] = []
        log_placeholder = st.empty()

        def _log(msg: str) -> None:
            log_lines.append(msg)
            log_placeholder.code("\n".join(log_lines), language="text")

        st.info("Starting seed — this may take a few minutes for large CSV files…")
        progress = st.progress(0, text="Initialising…")

        try:
            progress.progress(0.05, text="Ensuring database exists…")
            _log("Checking / creating database…")
            _seed_module.create_database_if_missing()

            progress.progress(0.15, text="Creating schema tables…")
            _log("Creating schema (if tables are missing)…")
            self.db_manager.init_schema()
            _log("  Schema OK")

            progress.progress(0.25, text=f"Loading users from {user_file}…")
            _log(f"\nLoading users: {user_file}")

            session = self.db_manager.Session()
            try:
                known_uids = _seed_module.load_users(session, filepath=user_file)
                session.commit()
                _log(f"  Loaded {len(known_uids)} user(s)")

                progress.progress(0.40, text=f"Loading sensor readings from {sensor_file}…")
                _log(f"\nLoading sensors: {sensor_file}")
                _seed_module.load_sensors(
                    session, known_uids=known_uids, filepath=sensor_file
                )
                _log("  Sensor load complete")

            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

            progress.progress(1.0, text="Done!")
            st.success(
                "✅ Seed complete! Switch to the **📊 Analytics** tab "
                "to start exploring your data."
            )

            # Clear cached user list so multiselect reflects the new UIDs
            st.session_state.pop("all_users", None)

        except Exception as exc:
            progress.empty()
            st.error(f"Seed failed: {exc}")
            _log(f"\nFATAL ERROR: {exc}")

    # ─────────────────────────────────────────────────────────────────────────
    # DB helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _load_users_cached(self) -> List[User]:
        """Fetch all users once per session; cache in session_state."""
        if "all_users" not in st.session_state:
            try:
                st.session_state["all_users"] = self.db_manager.get_all_users()
            except Exception as exc:
                st.error(f"Could not connect to the database: {exc}")
                st.stop()
        return st.session_state["all_users"]

    # Sidebar
    def _render_sidebar(
        self,
        all_users: List[User],
    ) -> tuple[List[str], datetime | None, datetime | None]:
        """
        Build sidebar controls: multiselect from DB users + optional date range.

        The date range section is toggled by a radio button:
          • "All time"    — returns (None, None), query fetches every row for the user
          • "Date range"  — shows date pickers and returns (start_dt, end_dt)

        Returns
        -------
        (selected_uids, start_datetime_or_None, end_datetime_or_None)
        """
        st.sidebar.header("🔍 Query Controls")

        if all_users:
            uid_options = [u.uid for u in all_users]
            uid_labels  = {
                u.uid: f"{u.uid}  ({u.age_range}, {u.gender})"
                for u in all_users
            }
            selected_uids: List[str] = st.sidebar.multiselect(
                label       = "Select User(s)",
                options     = uid_options,
                format_func = lambda uid: uid_labels.get(uid, uid),
                help        = "Choose one or more users to compare. Data is pulled live from the database.",
            )
        else:
            st.sidebar.warning("No users found in the database. Run seed.py or upload a CSV first.")
            selected_uids = []

        st.sidebar.divider()

        # ── Date range toggle ─────────────────────────────────────────────
        st.sidebar.subheader("📅 Date Range")
        date_mode = st.sidebar.radio(
            label     = "Filter by date?",
            options   = ["All time", "Date range"],
            index     = 0,
            horizontal= True,
            help      = "All time fetches every reading in the database for the selected user(s).",
        )

        start_dt: datetime | None = None
        end_dt:   datetime | None = None

        if date_mode == "Date range":
            default_end   = datetime.today().date()
            default_start = (datetime.today() - timedelta(days=30)).date()

            start_date = st.sidebar.date_input("Start date", value=default_start)
            end_date   = st.sidebar.date_input("End date",   value=default_end)

            if start_date > end_date:
                st.sidebar.error("Start date must be before end date.")
            else:
                start_dt = datetime.combine(start_date, datetime.min.time())
                end_dt   = datetime.combine(end_date,   datetime.max.time())
        # else:
        #     st.sidebar.caption("Showing all available readings")

        st.sidebar.divider()
        st.sidebar.caption(
            f"**Database:** `{self.db_manager.engine.url.database}`\n\n"
            f"**Total users in DB:** {len(all_users)}"
        )

        return selected_uids, start_dt, end_dt

    # Charts
    def _display_plots(self, user_data: Dict[str, List[DeviceStatus]]) -> None:
        """Render all sensor chart sections, one trace per selected user."""

        # ── Battery ──────────────────────────────────────────────────────
        st.subheader("🔋 Battery Level")
        fig = go.Figure()
        for i, (uid, rows) in enumerate(user_data.items()):
            fig.add_trace(go.Scatter(
                x    = [r.recorded_at   for r in rows],
                y    = [r.battery_level for r in rows],
                mode = "lines",
                name = uid,
                line = dict(color=_PALETTE[i % len(_PALETTE)]),
            ))
        self._apply_layout(fig, "Battery Drain Over Time", "Battery %")
        st.plotly_chart(fig, use_container_width=True)
        st.divider()

        # Accelerometer 
        st.subheader("🏃 Accelerometer  (X / Y / Z)")
        self._render_axis_columns(user_data, "motion_log",
                                  [("accel_x","X"), ("accel_y","Y"), ("accel_z","Z")],
                                  "Accel", "m/s²")
        st.divider()

        # Gravity
        st.subheader("⬇️  Gravity Vector  (X / Y / Z)")
        self._render_axis_columns(user_data, "motion_log",
                                  [("grav_x","X"), ("grav_y","Y"), ("grav_z","Z")],
                                  "Gravity", "m/s²")
        st.divider()

        # Gyroscope
        st.subheader("🌀 Gyroscope  (X / Y / Z)")
        self._render_axis_columns(user_data, "motion_log",
                                  [("gyro_x","X"), ("gyro_y","Y"), ("gyro_z","Z")],
                                  "Gyro", "rad/s")
        st.divider()

        # Orientation 
        st.subheader("🧭 Orientation  (Azimuth / Pitch / Roll)")
        self._render_axis_columns(user_data, "orientation_log",
                                  [("azimuth","Azimuth"), ("pitch","Pitch"), ("roll","Roll")],
                                  "", "Degrees (°)")
        st.divider()

        # Environmental
        st.subheader("💡 Environmental  (Light & Magnetometer)")
        left_col, right_col = st.columns([1, 2])

        fig_light = go.Figure()
        for i, (uid, rows) in enumerate(user_data.items()):
            valid = [r for r in rows if r.environmental_log]
            fig_light.add_trace(go.Scatter(
                x    = [r.recorded_at for r in valid],
                y    = [float(r.environmental_log.light or 0) for r in valid],
                mode = "lines",
                name = uid,
                line = dict(color=_PALETTE[i % len(_PALETTE)]),
            ))
        self._apply_layout(fig_light, "Ambient Light", "Lux")
        left_col.plotly_chart(fig_light, use_container_width=True)

        mag_sub = right_col.columns(3)
        for sub_col, (attr, label) in zip(mag_sub,
                [("mag_x","X"), ("mag_y","Y"), ("mag_z","Z")]):
            fig = go.Figure()
            for i, (uid, rows) in enumerate(user_data.items()):
                valid = [r for r in rows if r.environmental_log]
                fig.add_trace(go.Scatter(
                    x    = [r.recorded_at for r in valid],
                    y    = [float(getattr(r.environmental_log, attr) or 0) for r in valid],
                    mode = "lines",
                    name = uid,
                    line = dict(color=_PALETTE[i % len(_PALETTE)]),
                ))
            self._apply_layout(fig, f"Mag {label}", "μT", compact=True)
            sub_col.plotly_chart(fig, use_container_width=True)

    def _render_axis_columns(
        self,
        user_data:  Dict[str, List[DeviceStatus]],
        log_attr:   str,
        axes:       list,
        prefix:     str,
        y_label:    str,
    ) -> None:
        """Render three side-by-side compact charts for one sensor group."""
        cols = st.columns(3)
        for col, (attr, axis_label) in zip(cols, axes):
            fig = go.Figure()
            for i, (uid, rows) in enumerate(user_data.items()):
                valid = [r for r in rows if getattr(r, log_attr)]
                fig.add_trace(go.Scatter(
                    x    = [r.recorded_at for r in valid],
                    y    = [float(getattr(getattr(r, log_attr), attr) or 0) for r in valid],
                    mode = "lines",
                    name = uid,
                    line = dict(color=_PALETTE[i % len(_PALETTE)]),
                ))
            title = f"{prefix} {axis_label}".strip()
            self._apply_layout(fig, title, y_label, compact=True)
            col.plotly_chart(fig, use_container_width=True)

    # Summary table
    def _display_summary_table(
        self,
        user_data: Dict[str, List[DeviceStatus]],
        all_users: List[User],
    ) -> None:
        """Demographic + reading-count summary for every selected user."""
        st.subheader("👤 User Summary")
        user_map = {u.uid: u for u in all_users}
        rows = []
        for uid, data in user_data.items():
            u = user_map.get(uid)
            if not u:
                continue
            bats = [r.battery_level for r in data if r.battery_level is not None]
            rows.append({
                "UID":           uid,
                "Age Range":     u.age_range,
                "Gender":        u.gender,
                "University":    u.university,
                "# Readings":    len(data),
                "Avg Battery %": f"{sum(bats)/len(bats):.1f}" if bats else "N/A",
                "Min Battery %": min(bats) if bats else "N/A",
                "Max Battery %": max(bats) if bats else "N/A",
            })
        if rows:
            st.dataframe(rows, use_container_width=True)

    # Figure helpers
    def _generate_polly_figure(
        self,
        timestamps:  List[datetime],
        series_data: List[List[float]],
        *,
        labels:  List[str] | None = None,
        title:   str = "",
        y_label: str = "",
    ) -> go.Figure:
        """Build a Plotly multi-line time-series Figure (kept for external callers)."""
        fig    = go.Figure()
        labels = labels or [f"Series {i+1}" for i in range(len(series_data))]
        for values, label in zip(series_data, labels):
            fig.add_trace(go.Scatter(x=timestamps, y=values, mode="lines", name=label))
        self._apply_layout(fig, title, y_label)
        return fig

    @staticmethod
    def _apply_layout(
        fig:     go.Figure,
        title:   str,
        y_label: str,
        compact: bool = False,
    ) -> None:
        fig.update_layout(
            title       = title,
            xaxis_title = "Time",
            yaxis_title = y_label,
            hovermode   = "x unified",
            height      = 280 if compact else 380,
            margin      = dict(l=40, r=20, t=40, b=40),
            legend      = dict(orientation="h", yanchor="bottom", y=1.02, x=0),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Streamlit entry-point
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    db = DatabaseManager()
    db.init_schema()
    dashboard = AnalyticsDashboard(db_manager=db)
    dashboard.run()
