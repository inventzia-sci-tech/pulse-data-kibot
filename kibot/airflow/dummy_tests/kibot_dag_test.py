from commons.utils.LoggingUtils import instantiate_logging
from airflow_pipelines.airflow_pipeline_adapters import AirflowPipelineContext
from kibot.pipelines.KibotHistDataPipeline import KibotDailyFtpDump

# --- Mock airflow_kwargs as if passed by Airflow ---
airflow_kwargs = {
    "dag_id": "kibot_daily_pipeline",
    "task_id": "KibotDailyFtpDump",
    "execution_date": "2025-10-29T00:00:00Z",
    "configuration_file": "/home/magrino_bini/Pulse_Repos/pulse-data-kibot/kibot/airflow/private_kibot_dag_config.ini",  # matches your DAG setup
}
# --- Create the context ---
ctx = AirflowPipelineContext.from_airflow_context(airflow_kwargs)
# Create the stage
stage = KibotDailyFtpDump(name="KibotDailyFtpDump")
# Run the stage
result, msg = stage.run(ctx)
print(result, msg)