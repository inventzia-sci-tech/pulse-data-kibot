
from commons.utils.DateAndTimeUtils import prev_business_day
from airflow_pipelines.airflow_pipeline_adapters import AirflowPipelineContext
from kibot.pipelines.KibotHistDataPipeline import KibotDailyFtpDump, KibotCdfProcessor

# --- Mock airflow_kwargs as if passed by Airflow ---
from dateutil import parser
dt = parser.isoparse("2025-10-29T00:00:00Z")
airflow_kwargs = {
    "dag_id": "kibot_daily_pipeline",
    "task_id": "KibotDailyFtpDump",
    "logical_date": dt,
    "configuration_file": "/home/magrino_bini/Pulse_Repos/pulse-data-kibot/kibot/airflow/private_kibot_dag_config_daily.ini",  # matches your DAG setup
}
# --- Create the context ---
ctx = AirflowPipelineContext.from_airflow_context(airflow_kwargs)
test_ftp = False
if test_ftp:
    # Create the stage
    stageFtp = KibotDailyFtpDump(name="KibotDailyFtpDump")
    # Run the ftp stage
    result, msg = stageFtp.run(ctx)
    print('Ftp:')
    print(result, msg)
# --------------------
# Test also the Intra
test_daily = False
if test_daily:
    # Run the cdf process
    stageCdf = KibotCdfProcessor(name="KibotCdfProcessor")
    result, msg = stageCdf.run(ctx)
    print('Cdf:')
    print(result, msg)
# --------------------
# Test also the Intra
test_intra = True
if test_intra:
    airflow_kwargs_i = {
        "dag_id": "kibot_daily_pipeline",
        "task_id": "KibotDailyFtpDump",
        "logical_date": dt,
        "configuration_file": "/home/magrino_bini/Pulse_Repos/pulse-data-kibot/kibot/airflow/private_kibot_dag_config_intra.ini",
    }
    ctx_i = AirflowPipelineContext.from_airflow_context(airflow_kwargs_i)
    # Create the stage
    stageCdf_i= KibotCdfProcessor(name="KibotCdfProcessor")
    result, msg = stageCdf_i.run(ctx_i)
    print('Cdf intra:')
    print(result, msg)
