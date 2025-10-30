import os
from typing import Tuple
from airflow_pipelines.airflow_pipeline_adapters import AirflowStageMixin
from pipelines.DataPipeline import BaseStage, StageResult, PipelineContext
from commons.ftp.FtpWrapper import FtpWrapper, FtpWrapperParameters


class KibotDailyFtpDump(BaseStage, AirflowStageMixin):
    """
        Downloads daily Kibot dumps from FTP and validates their presence.
        Can also be used as an AirflowStageMixin.
    """

    kibot_ftp_section = 'Kibot FTP'

    def run(self, ctx: PipelineContext) -> Tuple[StageResult, str]:
        errors : str = None
        try:
            outfolder  = ctx.params.get(self.kibot_ftp_section, 'outfolder')
            ftp_params = FtpWrapperParameters(config_parser=ctx.params,
                                              ftp_section=self.kibot_ftp_section)
            ftp_worker = FtpWrapper(ftp_params,
                                    ctx.logger)
            os.makedirs(outfolder, exist_ok=True)

        except Exception as e:
            errors = 'Error while running KibotDailyFtpDump ' + self.name + ': ' + str(e)
            ctx.logger.error(errors)

        # Verify if the data was dumped
        yyyymmdd = ctx.params.config_parser.getstr(self.kibot_ftp_section, "yyyymmdd")
        etf_daily_file = outfolder + '/All\ ETFs/Daily/' + str(yyyymmdd) +'.exe'
        stk_daily_file = outfolder + '/All\ Stocks/Daily/' + str(yyyymmdd) + '.exe'
        etf_daily_file_exists = os.path.exists(etf_daily_file)
        message = 'Etf file downloaded: ' + str(etf_daily_file_exists)
        stk_daily_file_exists = os.path.exists(stk_daily_file)
        message += '; Stock file downloaded: ' + str(stk_daily_file_exists)
        if stk_daily_file_exists and etf_daily_file_exists:
            if errors == '':
                stage = StageResult.SUCCESS
            else:
                stage = StageResult.SUCCESS_WITH_WARNINGS
        else:
            stage = StageResult.FAIL
        final_msg = message + '. errors: ' + str(errors)
        return (stage, final_msg)

