# SPDX-License-Identifier: AGPL-3.0-or-later OR LicenseRef-Inventzia-Commercial
# Copyright (c) 2013-2026 Magrino Bini, Paola Apruzzese, Inventzia Science and Technology Ltd.
#
# This file is part of pulse-data-kibot.
#
# pulse-data-kibot is dual-licensed:
#   - Under the GNU Affero General Public License v3.0 or later (see LICENSE-AGPL-3.0).
#   - Under a commercial license (see LICENSE-COMMERCIAL.txt).
#     Contact operations@inventzia.com.

from commons.utils.LoggingUtils import instantiate_logging
from airflow_pipelines.airflow_pipeline_adapters import AirflowPipelineContext
from kibot.pipelines.KibotHistDataPipeline import KibotDailyFtpDump, KibotCdfProcessor

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

