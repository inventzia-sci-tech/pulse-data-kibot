# ~/airflow/dags/kibot_daily_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG

from airflow.airflow_pipeline_adapters import AirflowPipelineContext
from kibot.pipelines.KibotHistDataPipeline import KibotDailyFtpDump

''''
    ln -s /path/to/here/dags.py ~/airflow/dags/
''''

# DAG configuration
default_args = {
    "owner": "Pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=25),
    # your static config path
    "config_path": "/home/pulse/configs/kibot_config.ini",
}

with DAG(
    dag_id="kibot_daily_pipeline",
    default_args=default_args,
    description="Daily Kibot FTP dump -> process -> store pipeline",
    schedule_interval="@daily",          # daily scheduling
    start_date=datetime(2025, 1, 1),    # earliest execution date (use past date)
    catchup=False,                      # don't run historical DAG runs unless wanted
    max_active_runs=1,
    tags=["kibot", "ftp"],
) as dag:

    # Provide config_path via op_kwargs so PipelineContext picks it up:
    config_path = "/home/pulse/configs/kibot_config.ini"

    kibot_dump_task = KibotDailyFtpDump.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        op_kwargs={"config_path": config_path},  # forwarded into airflow_kwargs
    )
