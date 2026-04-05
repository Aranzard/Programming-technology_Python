from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine

def main():
    mart_path = Path("data/mart/variant_16/mart_daily_2026-03-20_11-46-40.csv")
    
    table_name = "mart_earthquake_jp"

    connection_url = "postgresql+psycopg2://student:student@localhost:5432/analytics"
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