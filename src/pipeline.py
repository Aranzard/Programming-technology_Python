from __future__ import annotations
import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import create_engine, text

from load import main as load_mart_file


def utc_now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")


def read_state(path: Path) -> dict:
    if not path.exists() or path.stat().st_size == 0:
        return {
            "last_successful_watermark": "2026-02-18T00:00:00Z",
            "last_run_at_utc": None,
            "last_mode": None,
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def extract_from_api(state: dict, mode: str, base_dir: Path):
    """Извлечение данных из USGS API"""
    end_date = datetime.now(timezone.utc)

    if mode == "incremental" and state.get("last_successful_watermark"):
        start_date = datetime.fromisoformat(state["last_successful_watermark"])
        print(f"Incremental mode: watermark={start_date.date()}")
    else:
        start_date = end_date - timedelta(days=30)
        print(f"Full mode: last 30 days from {start_date.date()}")

    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "minmagnitude": 4.0,
        "minlatitude": 30.0,
        "maxlatitude": 46.0,
        "minlongitude": 129.0,
        "maxlongitude": 146.0,
        "starttime": start_date.strftime("%Y-%m-%d"),
        "endtime": end_date.strftime("%Y-%m-%d"),
    }

    print(f"Request URL: {url}")

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    events = data.get("features", [])
    print(f"Fetched {len(events)} events")

    if events:
        mags = [
            e["properties"].get("mag") for e in events if e["properties"].get("mag")
        ]
        if mags:
            print(f"Magnitude range: min={min(mags):.1f}, max={max(mags):.1f}")

    run_tag = utc_now_tag()
    raw_dir = base_dir / "data" / "raw" / "variant_16"
    raw_dir.mkdir(parents=True, exist_ok=True)
    raw_path = raw_dir / f"raw_{run_tag}.json"

    payload = {
        "metadata": {
            "mode": mode,
            "extracted_at_utc": run_tag,
            "row_count": len(events),
            "start_date": params["starttime"],
            "end_date": params["endtime"],
        },
        "records": data,
    }

    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Saved raw snapshot: {raw_path.name}")
    return raw_path, data


def transform(raw_path: Path, base_dir: Path):
    """Нормализация данных (raw -> normalized)"""
    with open(raw_path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    data = raw["records"]
    features = data.get("features", [])

    records = []
    for feature in features:
        props = feature["properties"]
        coords = feature["geometry"]["coordinates"]

        record = {
            "event_id": str(feature.get("id")),
            "ts": pd.to_datetime(props["time"], unit="ms", errors="coerce"),
            "mag": float(props["mag"]) if props["mag"] is not None else None,
            "place": str(props["place"]),
            "depth_km": float(coords[2]),
            "lat": float(coords[1]),
            "lon": float(coords[0]),
            "region_id": "JP_HON",
            "region_name": "Япония (Хонсю)",
        }
        records.append(record)

    df = pd.DataFrame(records)

    original_count = len(df)
    df = df.drop_duplicates(subset=["event_id"], keep="first")
    df = df.dropna(subset=["ts"])
    df = df.sort_values("ts").reset_index(drop=True)

    print(f"Normalized: original={original_count}, after_clean={len(df)}")

    run_tag = raw["metadata"]["extracted_at_utc"]
    normalized_dir = base_dir / "data" / "normalized" / "variant_16"
    normalized_dir.mkdir(parents=True, exist_ok=True)
    out_path = normalized_dir / f"normalized_{run_tag}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved normalized dataset: {out_path.name}")

    return out_path, df


def build_mart(normalized_df: pd.DataFrame, run_tag: str, base_dir: Path):
    """Создание витрины (normalized -> mart)"""
    df = normalized_df.copy()
    df["date"] = df["ts"].dt.date

    daily = (
        df.groupby("date")
        .agg(
            cnt_events=("event_id", "count"),
            max_mag_day=("mag", "max"),
            avg_mag=("mag", "mean"),
            min_mag_day=("mag", "min"),
        )
        .reset_index()
    )

    deep_ratio = (df["depth_km"] > 70).mean() * 100

    mart = daily.copy()
    mart["deep_events_percent"] = deep_ratio
    mart["total_events"] = len(df)
    mart["max_magnitude_overall"] = df["mag"].max()
    mart["region_id"] = "JP_HON"
    mart["region_name"] = "Япония (Хонсю)"

    print(
        f"Mart created: {len(mart)} rows, total_events={len(df)}, max_mag={df['mag'].max():.2f}"
    )

    mart_dir = base_dir / "data" / "mart" / "variant_16"
    mart_dir.mkdir(parents=True, exist_ok=True)
    out_path = mart_dir / f"mart_daily_{run_tag}.csv"
    mart.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved mart dataset: {out_path.name}")

    return out_path, mart


def load_mart_via_external_script():
    """Загрузка витрины через внешний load.py скрипт"""
    import subprocess
    import sys

    print("Calling load.py to load mart into PostgreSQL...")

    result = subprocess.run(
        [sys.executable, "src/load.py"],
        capture_output=True,
        text=True,
        cwd=Path(__file__).parent.parent,
    )

    if result.returncode != 0:
        print(f"load.py failed with error: {result.stderr}")
        raise RuntimeError("load.py execution failed")

    print(f"load.py output: {result.stdout}")
    print("Load completed successfully")


def run_pipeline(mode: str) -> dict:
    """Запуск пайплайна"""
    base_dir = Path(__file__).parent.parent

    state_path = base_dir / "data" / "state" / "state.json"
    state = read_state(state_path)

    print("=" * 70)
    print(f"Starting pipeline | mode={mode}")
    print(f"Current watermark: {state.get('last_successful_watermark')}")

    # EXTRACT
    raw_path, raw_data = extract_from_api(state, mode, base_dir)

    # TRANSFORM
    normalized_path, normalized_df = transform(raw_path, base_dir)

    # BUILD MART
    run_tag = raw_path.stem.replace("raw_", "")
    mart_path, mart_df = build_mart(normalized_df, run_tag, base_dir)

    # LOAD
    load_mart_via_external_script()

    # UPDATE WATERMARK
    features = raw_data.get("features", [])
    if features:
        max_time = max(f["properties"]["time"] for f in features)
        max_date = datetime.fromtimestamp(max_time / 1000).isoformat()
        state = {
            "last_successful_watermark": max_date,
            "last_run_at_utc": utc_now_tag(),
            "last_mode": mode,
        }
        write_state(state_path, state)
        print(f"Watermark updated -> {max_date}")

    final_rows = len(mart_df)
    print(f"Pipeline finished successfully. Final target rows={final_rows}")
    print("=" * 70)

    return {
        "status": "ok",
        "rows_in_batch": len(mart_df),
        "final_target_rows": final_rows,
        "state": state,
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Earthquake ETL pipeline")
    p.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="full",
        help="Pipeline mode: full or incremental (default: full)",
    )
    return p


if __name__ == "__main__":
    args = build_parser().parse_args()
    result = run_pipeline(mode=args.mode)
    print(json.dumps(result, ensure_ascii=False, indent=2))
