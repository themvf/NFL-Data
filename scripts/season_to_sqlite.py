"""Download nflreadpy datasets for one or more seasons into SQLite."""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import nflreadpy as nfl
import polars as pl

ADV_STAT_TYPES = ["pass", "rush", "rec", "def"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Persist nflreadpy data to SQLite.")
    parser.add_argument(
        "--season",
        type=int,
        action="append",
        dest="seasons",
        help="Season to export (repeat flag for multiple seasons). If omitted, defaults to nflreadpy's current season.",
    )
    parser.add_argument(
        "--summary-level",
        choices=["week", "reg", "post", "reg+post"],
        default="reg",
        help="Summary level for team stats table.",
    )
    parser.add_argument(
        "--advstats-summary",
        choices=["week", "season"],
        default="week",
        help="Summary level for Pro Football Reference advanced stats tables.",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path("data/nflverse.sqlite"),
        help="SQLite database path (default: data/nflverse.sqlite).",
    )
    return parser.parse_args()


def to_pandas(frame: pl.DataFrame | pd.DataFrame) -> pd.DataFrame:
    if isinstance(frame, pd.DataFrame):
        return frame
    return frame.to_pandas()


def try_load(label: str, loader: Callable[[], pl.DataFrame]) -> pl.DataFrame | None:
    try:
        return loader()
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: {label} unavailable ({exc}). Skipping.")
        return None


def export_season(
    season: int,
    *,
    summary_level: str,
    advstats_summary: str,
    conn: sqlite3.Connection,
    timestamp: str,
) -> list[dict[str, object]]:
    print(f"\n=== Exporting season {season} ===")
    team_stats = nfl.load_team_stats(season, summary_level=summary_level)
    schedules = nfl.load_schedules(season)
    rosters = try_load("rosters", lambda: nfl.load_rosters(season))
    injuries = try_load("injuries", lambda: nfl.load_injuries(season))

    adv_tables: dict[str, pd.DataFrame] = {}
    for stat_type in ADV_STAT_TYPES:
        table_name = f"pfr_advstats_{stat_type}_{advstats_summary}"
        adv_df = try_load(
            f"pfr advanced stats ({stat_type})",
            lambda st=stat_type: nfl.load_pfr_advstats(
                season, stat_type=st, summary_level=advstats_summary
            ),
        )
        if adv_df is not None:
            adv_tables[table_name] = to_pandas(adv_df)

    metadata_rows: list[dict[str, object]] = []
    created_tables: set[str] = set()

    def persist(
        table_name: str,
        frame: pl.DataFrame | pd.DataFrame | None,
        summary: str | None,
    ) -> None:
        if frame is None:
            return

        pandas_frame = to_pandas(frame)
        if pandas_frame.empty and not list(pandas_frame.columns):
            print(f"Warning: {table_name} empty; skipping.")
            return

        conn.execute(
            "DELETE FROM ingest_metadata WHERE table_name = ? AND season = ?",
            (table_name, season),
        )
        try:
            conn.execute(f"DELETE FROM {table_name} WHERE season = ?", (season,))
        except sqlite3.OperationalError:
            pass

        pandas_frame.to_sql(table_name, conn, if_exists="append", index=False)
        metadata_rows.append(
            {
                "table_name": table_name,
                "season": season,
                "summary_level": summary,
                "ingested_at": timestamp,
            }
        )
        created_tables.add(table_name)

    persist("team_stats", team_stats, summary_level)
    persist("schedules", schedules, None)
    persist("rosters", rosters, None)
    persist("injuries", injuries, None)

    for table_name, df in adv_tables.items():
        persist(table_name, df, advstats_summary)

    if "team_stats" in created_tables:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_team_stats_season_team ON team_stats(season, team)"
        )
    if "schedules" in created_tables:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_schedules_season_week ON schedules(season, week)"
        )
    if "rosters" in created_tables:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rosters_season_team ON rosters(season, team)"
        )
    if "injuries" in created_tables:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_injuries_season_week_team ON injuries(season, week, team)"
        )
    for stat_type in ADV_STAT_TYPES:
        table_name = f"pfr_advstats_{stat_type}_{advstats_summary}"
        if table_name in created_tables:
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_season ON {table_name}(season)"
            )

    return metadata_rows


def main() -> None:
    args = parse_args()
    seasons = args.seasons or [nfl.get_current_season()]

    args.db_path.parent.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).isoformat()

    with sqlite3.connect(args.db_path) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS ingest_metadata (table_name TEXT NOT NULL, season INTEGER NOT NULL, summary_level TEXT, ingested_at TEXT NOT NULL)"
        )

        all_metadata: list[dict[str, object]] = []
        for season in seasons:
            metadata_rows = export_season(
                season,
                summary_level=args.summary_level,
                advstats_summary=args.advstats_summary,
                conn=conn,
                timestamp=timestamp,
            )
            all_metadata.extend(metadata_rows)

        if all_metadata:
            pd.DataFrame(all_metadata).to_sql(
                "ingest_metadata", conn, if_exists="append", index=False
            )

    print("\nDone. Updated tables stored in", args.db_path)


if __name__ == "__main__":
    main()
