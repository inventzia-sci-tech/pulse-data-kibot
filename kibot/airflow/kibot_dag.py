# ~/airflow_pipelines/dags/kibot_daily_pipeline.py
import sys
from datetime import datetime, timedelta
from airflow import DAG

for path in (
    "/home/magrino_bini/Code_Repos/py_algotrading_infrastructure",
):
    if path not in sys.path:
        sys.path.append(path)

from airflow_pipelines.airflow_pipeline_adapters import AirflowPipelineContext
from pipelines.SchemaStoreStage import SchemaStoreStage
from kibot.pipelines.KibotHistDataPipeline import (
    KibotDailyFtpDump,
    KibotDailyArchiveExtract,
    KibotDailyArchiveCleanup,
    KibotCdfProcessor,
)

'''
    ln -s /path/to/here/dags.py ~/airflow_pipelines/dags/
'''

# DAG configuration
default_args = {
    "owner": "Pulse",
    "depends_on_past": False,
    "retries": 10,
    "retry_delay": timedelta(hours=1),
    # your static config path
    "config_path_daily": "/home/magrino_bini/Pulse_Repos/pulse-data-kibot/kibot/airflow/private_kibot_dag_config_daily.ini",
    "config_path_intra": "/home/magrino_bini/Pulse_Repos/pulse-data-kibot/kibot/airflow/private_kibot_dag_config_intra.ini",
}

with DAG(
    dag_id="Kibot_daily_pipeline",
    default_args=default_args,
    description="Daily Kibot FTP dump -> process -> store pipeline",
    schedule="0 10 * * *",               # fires at 10:00 UTC daily — ~7h after Kibot's typical ~03:00 UTC publish for prior day; retries (10 × 1h) extend coverage to ~20:00 UTC
    start_date=datetime(2020, 1, 1),    # earliest execution date — admit pre-2025 manual triggers (backfill)
    catchup=False,                      # don't run historical DAG runs unless wanted
    max_active_runs=1,
    tags=["kibot", "ftp", "cdf"],
) as dag:

    # Provide config_path via op_kwargs so PipelineContext picks it up:
    config_daily = dag.default_args.get("config_path_daily")
    config_intra = dag.default_args.get("config_path_intra")
    config_var_daily = "kibot_dag_config_daily"
    config_var_intra = "kibot_dag_config_intra"
    # -------------------------------
    # Stage 1: FTP dump
    kibot_dump_task = KibotDailyFtpDump.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        op_kwargs={
            "configuration_file": config_daily,
            "configuration_variable": config_var_daily,
        },  # forwarded into airflow_kwargs
    )
    # -------------------------------
    # Stage 1.5: Archive extraction (shared)
    kibot_extract_task = KibotDailyArchiveExtract.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_archive_extract",
        op_kwargs={
            "configuration_file": config_daily,
            "configuration_variable": config_var_daily,
        },
    )
    # -------------------------------
    # Stage 2: CDF processing daily (parallel by stage)
    kibot_process_task_daily_stocks = KibotCdfProcessor.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_cdf_process_daily_stocks",
        op_kwargs={
            "configuration_file": config_daily,
            "configuration_variable": config_var_daily,
            "processing_stages_override": ["Stocks Daily"],
        },
    )
    kibot_process_task_daily_etfs = KibotCdfProcessor.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_cdf_process_daily_etfs",
        op_kwargs={
            "configuration_file": config_daily,
            "configuration_variable": config_var_daily,
            "processing_stages_override": ["Etfs Daily"],
        },
    )
    # -------------------------------
    # Stage 3: CDF processing intra (parallel by stage)
    kibot_process_task_intra_stocks = KibotCdfProcessor.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_cdf_process_intra_stocks",
        op_kwargs={
            "configuration_file": config_intra,
            "configuration_variable": config_var_intra,
            "processing_stages_override": ["Stocks Intraday"],
        },
    )
    kibot_process_task_intra_etfs = KibotCdfProcessor.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_cdf_process_intra_etfs",
        op_kwargs={
            "configuration_file": config_intra,
            "configuration_variable": config_var_intra,
            "processing_stages_override": ["Etfs Intraday"],
        }
    )
    # -------------------------------
    # Stage 3.5: Store processed CDFs to DB (daily/intra)
    kibot_store_task_daily = SchemaStoreStage.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_store_daily",
        op_kwargs={
            "configuration_file": config_daily,
            "configuration_variable": config_var_daily,
        },
    )
    kibot_store_task_intra = SchemaStoreStage.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_store_intra",
        op_kwargs={
            "configuration_file": config_intra,
            "configuration_variable": config_var_intra,
        },
    )
    # -------------------------------
    # Stage 4: Archive cleanup
    kibot_cleanup_task = KibotDailyArchiveCleanup.to_task(
        dag=dag,
        ctx_factory=AirflowPipelineContext.from_airflow_context,
        task_id="Kibot_archive_cleanup",
        op_kwargs={
            "configuration_file": config_daily,
            "configuration_variable": config_var_daily,
        },
    )

    # Define pipeline order
    kibot_dump_task >> kibot_extract_task >> [
        kibot_process_task_daily_stocks,
        kibot_process_task_daily_etfs,
        kibot_process_task_intra_stocks,
        kibot_process_task_intra_etfs,
    ]
    [kibot_process_task_daily_stocks, kibot_process_task_daily_etfs] >> kibot_store_task_daily
    [kibot_process_task_intra_stocks, kibot_process_task_intra_etfs] >> kibot_store_task_intra
    [kibot_store_task_daily, kibot_store_task_intra] >> kibot_cleanup_task
