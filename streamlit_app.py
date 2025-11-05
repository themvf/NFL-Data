from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import nflreadpy as nfl
import streamlit as st


ROOT_DIR = Path(__file__).resolve().parent
EXPORT_SCRIPT = ROOT_DIR / "scripts" / "season_to_sqlite.py"


st.set_page_config(page_title="NFL Data Refresh", page_icon="🏈", layout="centered")
st.title("NFL Data Refresh Dashboard")
st.write(
    "Use this app to manually trigger the SQLite export script without waiting for "
    "the scheduled GitHub Actions run."
)

current_season = nfl.get_current_season()
season_options = list(range(current_season, 1999 - 1, -1))

selected_seasons = st.multiselect(
    "Select seasons to refresh",
    options=season_options,
    default=[current_season],
    help="Pick one or more seasons. The exporter can combine multiple seasons "
    "into the same SQLite database.",
)

adv_summary = st.selectbox(
    "Advanced stats summary level",
    options=["week", "season"],
    index=0,
    help="Choose whether to download weekly or season-level Pro Football Reference "
    "advanced stats.",
)

summary_level = st.selectbox(
    "Team stats summary level",
    options=["week", "reg", "post", "reg+post"],
    index=1,
    help="Controls the level of aggregation for team statistics.",
)

st.divider()

if st.button("Refresh Data"):
    if not selected_seasons:
        st.warning("Please select at least one season before refreshing.")
    elif not EXPORT_SCRIPT.exists():
        st.error(
            f"Export script not found at {EXPORT_SCRIPT}. "
            "Ensure you are running the app from the repository root."
        )
    else:
        st.info("Refresh in progress… this may take a minute.")
        with st.spinner("Downloading data and updating SQLite database…"):
            command = [sys.executable, str(EXPORT_SCRIPT)]
            command.extend(["--advstats-summary", adv_summary, "--summary-level", summary_level])
            for season in sorted(selected_seasons):
                command.extend(["--season", str(season)])

            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                cwd=ROOT_DIR,
            )

        if result.returncode == 0:
            st.success("Refresh completed successfully.")
        else:
            st.error(f"Exporter exited with code {result.returncode}.")

        with st.expander("Show exporter output", expanded=result.returncode != 0):
            stdout = result.stdout.strip()
            stderr = result.stderr.strip()
            if stdout:
                st.subheader("stdout")
                st.code(stdout, language="text")
            if stderr:
                st.subheader("stderr")
                st.code(stderr, language="text")

st.sidebar.header("Quick Start")
st.sidebar.write(
    "1. Install dependencies with `pip install -r requirements.txt`.\n"
    "2. Run `streamlit run streamlit_app.py`.\n"
    "3. Use the controls on this page to refresh the SQLite database."
)
