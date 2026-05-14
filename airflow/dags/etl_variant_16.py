from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow"
CONFIG = f"{PROJECT_DIR}/configs/variant_16.yml"

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
        bash_command=f"cd {PROJECT_DIR} && python src/extract.py --ds {{{{ ds }}}}",
    )

    transform = BashOperator(
        task_id="transform",
        bash_command=f"cd {PROJECT_DIR} && python src/transform.py --ds {{{{ ds }}}}",
    )

    dq = BashOperator(
        task_id="dq",
        bash_command=f"cd {PROJECT_DIR} && python src/dq.py --ds {{{{ ds }}}}",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT_DIR} && python src/load.py --ds {{{{ ds }}}}",
    )

    extract >> transform >> dq >> load