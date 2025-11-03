# ~/airflow_pipelines/dags/kibot_daily_pipeline.py
from datetime import datetime, timedelta
from airflow import DAG

from airflow_pipelines.airflow_pipeline_adapters import AirflowPipelineContext
from kibot.pipelines.KibotHistDataPipeline import KibotDailyFtpDump, KibotCdfProcessor

'''
    ln -s /path/to/here/dags.py ~/airflow_pipelines/dags/
'''

# DAG configuration
default_args = {
    "owner": "Pulse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=25),
    # your static config path
    "config_path": "kibot_dag_config.ini",
}

with DAG(
    dag_id="kibot_daily_pipeline",
    default_args=default_args,
    description="Daily Kibot FTP dump -> process -> store pipeline",
    schedule_interval="@daily",          # daily scheduling
    start_date=datetime(2025, 1, 1),    # earliest execution date (use past date)
    catchup=False,                      # don't run historical DAG runs unless wanted
    max_active_runs=1,
    tags=["kibot", "ftp", "cdf"],
) as dag:

    # Provide config_path via op_kwargs so PipelineContext picks it up:
    config_path = "/home/pulse/configs/kibot_config.ini"

    # Stage 1: FTP dump
    kibot_dump_task = KibotDailyFtpDump.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        op_kwargs={"config_path": config_path},  # forwarded into airflow_kwargs
    )


    # Stage 2: CDF processing
    kibot_process_task = KibotCdfProcessor.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        op_kwargs={"config_path": config_path},
    )

    # Define pipeline order
    kibot_dump_task >> kibot_process_task
