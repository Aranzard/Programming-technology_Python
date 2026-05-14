import argparse
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="Дата запуска (YYYY-MM-DD)")
    args = parser.parse_args()

    run_date = args.ds
    mart_path = Path(f'data/mart/variant_16/mart_{run_date}.csv')

    if not mart_path.exists():
        print(f'[LOAD] No mart file for {run_date}, skipping')
        exit(0)

    df = pd.read_csv(mart_path)
    print(f'[LOAD] mart rows to load for {run_date}: {len(df)}')

    engine = create_engine('postgresql+psycopg2://student:student_pw@host.docker.internal:5432/analytics')

    with engine.begin() as conn:
        # Удаляем старые данные за этот день
        conn.execute(text(f"DELETE FROM mart_earthquake_jp WHERE date = '{run_date}'"))

        # Вставляем новые
        df.to_sql('mart_earthquake_jp', conn, if_exists='append', index=False)

        # Проверяем результат
        res = conn.execute(text(f"SELECT COUNT(*) FROM mart_earthquake_jp WHERE date = '{run_date}'"))
        cnt = res.scalar()
        print(f'[LOAD] loaded {cnt} rows for {run_date}')

if __name__ == "__main__":
    main()