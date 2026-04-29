from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine


def find_latest_mart_file(mart_dir: Path) -> Path:
    mart_files = list(mart_dir.glob("mart_daily_*.csv"))
    if not mart_files:
        raise FileNotFoundError(f"No mart files found in {mart_dir}")
    latest = max(mart_files, key=lambda f: f.stat().st_mtime)
    return latest


def main():
    mart_dir = Path("data/mart/variant_16")
    mart_path = find_latest_mart_file(mart_dir)
    table_name = "mart_earthquake_jp"

    connection_url = "postgresql+psycopg2://student:student_pw@localhost:5432/analytics"

    engine = create_engine(connection_url)

    print(f"[INFO] reading file: {mart_path.resolve()}")
    df = pd.read_csv(mart_path)

    print(f"[INFO] rows={len(df)}, cols={len(df.columns)}")
    print(f"[INFO] loading to table: {table_name}")

    with engine.begin() as conn:
        df.to_sql(table_name, conn, if_exists="replace", index=False)

    print("[OK] load finished successfully")


if __name__ == "__main__":
    main()
