import argparse
import pandas as pd
import json
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="Дата запуска (YYYY-MM-DD)")
    args = parser.parse_args()

    run_date = args.ds
    mart_path = Path(f'data/mart/variant_16/mart_{run_date}.csv')

    if not mart_path.exists():
        print(f'[DQ] No mart file for {run_date}, skipping')
        exit(0)

    df = pd.read_csv(mart_path)

    checks = [
        ('non_empty', len(df) > 0, 'FAIL'),
        ('not_null_date', df['date'].isnull().sum() == 0, 'FAIL'),
        ('unique_key', df.duplicated(subset=['date', 'region_id']).sum() == 0, 'FAIL'),
        ('positive_events', (df['cnt_events'] >= 0).all(), 'WARNING'),
    ]

    passed = sum(1 for c in checks if c[1])
    failed = sum(1 for c in checks if not c[1] and c[2] == 'FAIL')
    warnings = sum(1 for c in checks if not c[1] and c[2] == 'WARNING')

    print(f'[DQ] PASS={passed} WARNING={warnings} FAIL={failed}')

    if failed > 0:
        print(f'[DQ] QUALITY GATE FAILED - stopping pipeline')
        exit(1)

    report = {
        'checks': [{'name': c[0], 'status': 'PASS' if c[1] else 'FAIL' if c[2]=='FAIL' else 'WARNING'} for c in checks],
        'summary': {'passed': passed, 'failed': failed, 'warnings': warnings},
        'date': run_date
    }

    with open('data/dq_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    print('[DQ] report saved: data/dq_report.json')

if __name__ == "__main__":
    main()