from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow"

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="etl_variant_16",
    start_date=datetime(2026, 4, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["etl", "earthquake"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python -c "
import requests, json
from datetime import datetime, timedelta, timezone
from pathlib import Path

end = datetime.now(timezone.utc)
start = end - timedelta(days=30)

params = {{
    'format': 'geojson',
    'minmagnitude': 4.0,
    'minlatitude': 30.0,
    'maxlatitude': 46.0,
    'minlongitude': 129.0,
    'maxlongitude': 146.0,
    'starttime': start.strftime('%Y-%m-%d'),
    'endtime': end.strftime('%Y-%m-%d'),
}}

r = requests.get('https://earthquake.usgs.gov/fdsnws/event/1/query', params=params, timeout=30)
data = r.json()
events = data.get('features', [])

raw_dir = Path('data/raw/variant_16')
raw_dir.mkdir(parents=True, exist_ok=True)
tag = datetime.now(timezone.utc).strftime('%Y-%m-%d_%H-%M-%S')
path = raw_dir / f'raw_{{tag}}.json'

with open(path, 'w') as f:
    json.dump(data, f)

print(f'[EXTRACT] saved: {{path}} ({{path.stat().st_size}} bytes)')
print(f'[EXTRACT] events: {{len(events)}}')
"
        """,
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python -c "
import pandas as pd, json
from pathlib import Path

raw_dir = Path('data/raw/variant_16')
raw_files = list(raw_dir.glob('raw_*.json'))
latest = max(raw_files, key=lambda f: f.stat().st_mtime)

with open(latest) as f:
    data = json.load(f)

records = []
for f in data.get('features', []):
    p = f['properties']
    c = f['geometry']['coordinates']
    records.append({{
        'event_id': f.get('id'),
        'ts': pd.to_datetime(p['time'], unit='ms'),
        'mag': p.get('mag'),
        'place': p.get('place'),
        'depth_km': c[2],
        'region_id': 'JP_HON',
        'region_name': 'Япония (Хонсю)'
    }})

df = pd.DataFrame(records)
df = df.drop_duplicates('event_id').dropna(subset=['ts'])
print(f'[TRANSFORM] normalized rows: {{len(df)}}')

run_tag = latest.stem.replace('raw_', '')
norm_path = Path(f'data/normalized/variant_16/normalized_{{run_tag}}.csv')
norm_path.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(norm_path, index=False)
print(f'[TRANSFORM] normalized: {{norm_path}}')

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

print(f'[TRANSFORM] mart rows: {{len(mart)}}')
mart_path = Path(f'data/mart/variant_16/mart_daily_{{run_tag}}.csv')
mart_path.parent.mkdir(parents=True, exist_ok=True)
mart.to_csv(mart_path, index=False)
print(f'[TRANSFORM] mart: {{mart_path}}')
"
        """,
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python -c "
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

mart_dir = Path('data/mart/variant_16')
mart_files = list(mart_dir.glob('mart_daily_*.csv'))
latest = max(mart_files, key=lambda f: f.stat().st_mtime)

df = pd.read_csv(latest)
print(f'[LOAD] mart rows to load: {{len(df)}}')

engine = create_engine('postgresql+psycopg2://student:student_pw@host.docker.internal:5434/analytics')

with engine.begin() as conn:
    df.to_sql('mart_earthquake_jp', conn, if_exists='replace', index=False)
    res = conn.execute(text('SELECT COUNT(*) FROM mart_earthquake_jp'))
    cnt = res.scalar()
    print(f'[LOAD] loaded to postgres: {{cnt}} rows')
"
        """,
    )

    dq = BashOperator(
        task_id="dq",
        bash_command=f"""
            cd {PROJECT_DIR} && \
            python -c "
import pandas as pd, json
from pathlib import Path

mart_dir = Path('data/mart/variant_16')
mart_files = list(mart_dir.glob('mart_daily_*.csv'))
df = pd.read_csv(max(mart_files, key=lambda f: f.stat().st_mtime))

checks = [
    ('non_empty', len(df) > 0, 'FAIL'),
    ('not_null_date', df['date'].isnull().sum() == 0, 'FAIL'),
    ('unique_key', df.duplicated(subset=['date','region_id']).sum() == 0, 'FAIL'),
    ('positive_events', (df['cnt_events'] > 0).all(), 'WARNING'),
]

passed = sum(1 for c in checks if c[1])
failed = sum(1 for c in checks if not c[1] and c[2] == 'FAIL')
warnings = sum(1 for c in checks if not c[1] and c[2] == 'WARNING')

print(f'[DQ] PASS={{passed}} WARNING={{warnings}} FAIL={{failed}}')

report = {{
    'checks': [{{'name': c[0], 'status': 'PASS' if c[1] else 'FAIL' if c[2]=='FAIL' else 'WARNING'}} for c in checks],
    'summary': {{'passed': passed, 'failed': failed, 'warnings': warnings}}
}}

with open('data/dq_report.json', 'w') as f:
    json.dump(report, f, indent=2)
print('[DQ] report saved: data/dq_report.json')
"
        """,
    )

    extract >> transform >> load >> dq
