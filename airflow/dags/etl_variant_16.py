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
    schedule="0 0 * * *",  
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

# Используем переданную дату
run_date = '{{{{ ds }}}}'
start = datetime.strptime(run_date, '%Y-%m-%d')
end = start + timedelta(days=1)

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
path = raw_dir / f'raw_{{run_date}}.json'

with open(path, 'w') as f:
    json.dump(data, f)

print(f'[EXTRACT] saved: {{path}} ({{path.stat().st_size}} bytes)')
print(f'[EXTRACT] events for {{run_date}}: {{len(events)}}')
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

run_date = '{{{{ ds }}}}'
raw_path = Path(f'data/raw/variant_16/raw_{{run_date}}.json')

if not raw_path.exists():
    print(f'[TRANSFORM] No raw file for {{run_date}}, skipping')
    exit(0)

with open(raw_path) as f:
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
print(f'[TRANSFORM] normalized rows for {{run_date}}: {{len(df)}}')

if len(df) == 0:
    print(f'[TRANSFORM] No events for {{run_date}}, creating empty mart')
    mart = pd.DataFrame({{
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
    }})
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

norm_path = Path(f'data/normalized/variant_16/normalized_{{run_date}}.csv')
norm_path.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(norm_path, index=False)
print(f'[TRANSFORM] normalized: {{norm_path}}')

mart_path = Path(f'data/mart/variant_16/mart_{{run_date}}.csv')
mart_path.parent.mkdir(parents=True, exist_ok=True)
mart.to_csv(mart_path, index=False)
print(f'[TRANSFORM] mart rows: {{len(mart)}}')
print(f'[TRANSFORM] mart: {{mart_path}}')
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

run_date = '{{{{ ds }}}}'
mart_path = Path(f'data/mart/variant_16/mart_{{run_date}}.csv')

if not mart_path.exists():
    print(f'[DQ] No mart file for {{run_date}}, skipping')
    exit(0)

df = pd.read_csv(mart_path)

checks = [
    ('non_empty', len(df) > 0, 'FAIL'),
    ('not_null_date', df['date'].isnull().sum() == 0, 'FAIL'),
    ('unique_key', df.duplicated(subset=['date','region_id']).sum() == 0, 'FAIL'),
    ('positive_events', (df['cnt_events'] >= 0).all(), 'WARNING'),
]

passed = sum(1 for c in checks if c[1])
failed = sum(1 for c in checks if not c[1] and c[2] == 'FAIL')
warnings = sum(1 for c in checks if not c[1] and c[2] == 'WARNING')

print(f'[DQ] PASS={{passed}} WARNING={{warnings}} FAIL={{failed}}')

if failed > 0:
    print(f'[DQ] QUALITY GATE FAILED - stopping pipeline')
    exit(1)

report = {{
    'checks': [{{'name': c[0], 'status': 'PASS' if c[1] else 'FAIL' if c[2]=='FAIL' else 'WARNING'}} for c in checks],
    'summary': {{'passed': passed, 'failed': failed, 'warnings': warnings}},
    'date': run_date
}}

with open('data/dq_report.json', 'w') as f:
    json.dump(report, f, indent=2)
print('[DQ] report saved: data/dq_report.json')
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

run_date = '{{{{ ds }}}}'
mart_path = Path(f'data/mart/variant_16/mart_{{run_date}}.csv')

if not mart_path.exists():
    print(f'[LOAD] No mart file for {{run_date}}, skipping')
    exit(0)

df = pd.read_csv(mart_path)
print(f'[LOAD] mart rows to load for {{run_date}}: {{len(df)}}')

engine = create_engine('postgresql+psycopg2://student:student_pw@host.docker.internal:5432/analytics')

with engine.begin() as conn:
    # Удаляем старые данные за этот день (если есть)
    conn.execute(text(f\"DELETE FROM mart_earthquake_jp WHERE date = '{{run_date}}'\"))
    # Вставляем новые
    df.to_sql('mart_earthquake_jp', conn, if_exists='append', index=False)
    res = conn.execute(text('SELECT COUNT(*) FROM mart_earthquake_jp WHERE date = {{run_date}}'))
    cnt = res.scalar()
    print(f'[LOAD] loaded {{cnt}} rows for {{run_date}}')
"
        """,
    )

    extract >> transform >> dq >> load