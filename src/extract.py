import argparse
import requests
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="Дата запуска (YYYY-MM-DD)")
    args = parser.parse_args()

    run_date = args.ds
    start = datetime.strptime(run_date, '%Y-%m-%d')
    end = start + timedelta(days=1)

    params = {
        'format': 'geojson',
        'minmagnitude': 4.0,
        'minlatitude': 30.0,
        'maxlatitude': 46.0,
        'minlongitude': 129.0,
        'maxlongitude': 146.0,
        'starttime': start.strftime('%Y-%m-%d'),
        'endtime': end.strftime('%Y-%m-%d'),
    }

    r = requests.get('https://earthquake.usgs.gov/fdsnws/event/1/query', params=params, timeout=30)
    data = r.json()
    events = data.get('features', [])

    raw_dir = Path('data/raw/variant_16')
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f'raw_{run_date}.json'

    with open(path, 'w') as f:
        json.dump(data, f)

    print(f'[EXTRACT] saved: {path} ({path.stat().st_size} bytes)')
    print(f'[EXTRACT] events for {run_date}: {len(events)}')

if __name__ == "__main__":
    main()