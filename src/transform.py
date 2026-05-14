import argparse
import pandas as pd
import json
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="Дата запуска (YYYY-MM-DD)")
    args = parser.parse_args()

    run_date = args.ds
    raw_path = Path(f'data/raw/variant_16/raw_{run_date}.json')

    if not raw_path.exists():
        print(f'[TRANSFORM] No raw file for {run_date}, skipping')
        exit(0)

    with open(raw_path) as f:
        data = json.load(f)

    records = []
    for f in data.get('features', []):
        p = f['properties']
        c = f['geometry']['coordinates']
        records.append({
            'event_id': f.get('id'),
            'ts': pd.to_datetime(p['time'], unit='ms'),
            'mag': p.get('mag'),
            'place': p.get('place'),
            'depth_km': c[2],
            'region_id': 'JP_HON',
            'region_name': 'Япония (Хонсю)'
        })

    df = pd.DataFrame(records)
    df = df.drop_duplicates('event_id').dropna(subset=['ts'])
    print(f'[TRANSFORM] normalized rows for {run_date}: {len(df)}')

    if len(df) == 0:
        print(f'[TRANSFORM] No events for {run_date}, creating empty mart')
        mart = pd.DataFrame({
            'date': [run_date],
            'cnt_events': [0],
            'max_mag_day': [None],
            'avg_mag': [None],
            'min_mag_day': [None],
            'deep_events_percent': [0],
            'total_events': [0],
            'max_magnitude_overall': [None],
            'region_id': ['JP_HON'],
            'region_name': ['Япония (Хонсю)']
        })
    else:
        df['date'] = df['ts'].dt.date
        daily = df.groupby('date').agg(
            cnt_events=('event_id', 'count'),
            max_mag_day=('mag', 'max'),
            avg_mag=('mag', 'mean'),
            min_mag_day=('mag', 'min')
        ).reset_index()

        deep = (df['depth_km'] > 70).mean() * 100
        mart = daily.copy()
        mart['deep_events_percent'] = deep
        mart['total_events'] = len(df)
        mart['max_magnitude_overall'] = df['mag'].max()
        mart['region_id'] = 'JP_HON'
        mart['region_name'] = 'Япония (Хонсю)'

    norm_path = Path(f'data/normalized/variant_16/normalized_{run_date}.csv')
    norm_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(norm_path, index=False)
    print(f'[TRANSFORM] normalized: {norm_path}')

    mart_path = Path(f'data/mart/variant_16/mart_{run_date}.csv')
    mart_path.parent.mkdir(parents=True, exist_ok=True)
    mart.to_csv(mart_path, index=False)
    print(f'[TRANSFORM] mart rows: {len(mart)}')
    print(f'[TRANSFORM] mart: {mart_path}')

if __name__ == "__main__":
    main()