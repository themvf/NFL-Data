# NFL Data Automation

This repository keeps a daily-refreshing SQLite snapshot of key NFL datasets sourced from [
flreadpy](https://github.com/nflverse/nflreadpy).

## Contents

- data/nflverse.sqlite – consolidated database (team stats, schedules, rosters, advanced stats).
- scripts/season_to_sqlite.py – exporter script used locally and by GitHub Actions.
- .github/workflows/daily-refresh.yml – scheduled workflow that refreshes the database every day at 10:00 UTC.
- equirements.txt – Python dependencies for local or CI runs.

## Local Usage

`ash
pip install -r requirements.txt
python scripts/season_to_sqlite.py --season 2025
`

Use the --season flag multiple times to pull additional years:

`ash
python scripts/season_to_sqlite.py --season 2024 --season 2025
`

The script writes/updates data/nflverse.sqlite and keeps an ingest_metadata table to track refresh timestamps. If a feed (e.g., injuries) is not yet published for the requested season, it is skipped with a warning instead of failing the run.

## Scheduled Refresh

The GitHub Actions workflow runs daily (and on manual dispatch) to:

1. Install the dependencies listed in equirements.txt.
2. Execute the exporter for the 2025 season (--advstats-summary week).
3. Commit and push changes to data/nflverse.sqlite when new data is available.

Adjust the cron expression or flags in .github/workflows/daily-refresh.yml to change the schedule or seasons being updated.

### Manual refresh with Streamlit

`ash
pip install -r requirements.txt  # ensures Streamlit is available
streamlit run streamlit_app.py
`

From the web UI you can choose one or more seasons, adjust aggregation levels, and trigger the SQLite export script without waiting for the scheduled workflow.
