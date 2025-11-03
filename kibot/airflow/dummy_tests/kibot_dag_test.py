from commons.utils.LoggingUtils import instantiate_logging
from airflow_pipelines.airflow_pipeline_adapters import AirflowPipelineContext
from kibot.pipelines.KibotHistDataPipeline import KibotDailyFtpDump, KibotCdfProcessor

# --- Mock airflow_kwargs as if passed by Airflow ---
airflow_kwargs = {
    "dag_id": "kibot_daily_pipeline",
    "task_id": "KibotDailyFtpDump",
    "execution_date": "2025-10-29T00:00:00Z",
    "configuration_file": "/home/magrino_bini/Pulse_Repos/pulse-data-kibot/kibot/airflow/kibot_dag_config.ini",  # matches your DAG setup
}
# --- Create the context ---
ctx = AirflowPipelineContext.from_airflow_context(airflow_kwargs)
# Create the stage
stageFtp = KibotDailyFtpDump(name="KibotDailyFtpDump")
# Run the ftp stage
result, msg = stageFtp.run(ctx)
print('Ftp:')
print(result, msg)
# Run the cdf process
stageCdf = KibotCdfProcessor(name="KibotCdfProcessor")
result, msg = stageCdf.run(ctx)
print('Cdf:')
print(result, msg)

