from core.etl_utils import ETL_Base
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from core.db_utils import pdf_to_sdf
import core.logger as log
logger = log.setup_logging('Job')


class ETL_Incremental_Base(ETL_Base):
    def __init__(self, pre_jargs={}, jargs=None, loaded_inputs={}):
        super(ETL_Incremental_Base, self).__init__(pre_jargs, jargs, loaded_inputs)
        self.last_attempted_period = None

    def transform(self, **loaded_datasets):
        return self.get_transform_inc_from_last_days(**loaded_datasets)  # transform_inc passed with self

    def transform_inc(self, **app_args):
        """ The function that needs to be overriden by each specific job."""
        raise NotImplementedError

    def get_transform_inc_from_last_days(self, **loaded_datasets):
        """ Incremental assumes last available month from the previous output was fully loaded."""
        first_day = self.jargs.merged_args['first_day']
        if not self.last_attempted_period:
            self.last_attempted_period  = self.get_previous_output_max_timestamp() or first_day  # TODO: if get_output_max_timestamp()=None, means new build, so should delete instance in oracle.

        periods = self.get_last_output_to_last_day(self.last_attempted_period, first_day)
        if len(periods) == 0:
            self.logger.info('Output up to date. Nothing to run. last processed period={} and last period from now={}'.format(self.last_attempted_period, self.get_last_day()))
            self.final_inc = True
            # empty_df = pd.DataFrame()
            # return pdf_to_sdf(empty_df, self.OUTPUT_TYPES, self.sc, self.sc_sql)
            # TODO: look at output empty table instead of skipping output. i.e. removing that if.
            return None

        self.logger.info('Periods remaining to load: {}'.format(periods))
        period = periods[0]
        self.logger.info('Period to be loaded in this run: {}'.format(period))
        self.final_inc = period == periods[-1]
        self.last_attempted_period = period
        self.jargs.merged_args['file_tag'] = period

        df = self.transform_inc(period)  # TODO: check to pass it as arg so code more retracable from my classes.
        return df

    @staticmethod
    def get_last_day():
        # import ipdb; ipdb.set_trace()
        last_day_dt = datetime.utcnow() + relativedelta(days=-1)
        last_day = last_day_dt.strftime("%Y-%m-%d")
        return last_day

    @staticmethod
    def get_start_to_last_day(first_day):
        now = datetime.utcnow()
        start = datetime.strptime(first_day, "%Y-%m-%d")
        delta = relativedelta(now, start)
        number_days = delta.days # delta.years * 12 + delta.months

        periods = []
        iter_days = start
        for item in range(number_days):
            periods.append(iter_days.strftime("%Y-%m-%d"))
            iter_days = iter_days + relativedelta(days=+1)
        # import ipdb; ipdb.set_trace()
        return periods

    def get_last_output_to_last_day(self, last_output, first_day):
        periods = self.get_start_to_last_day(first_day)
        return [item for item in periods if item > last_output]

# def get_transform_inc_from_last_month(Job, api_params, lines_override, first_month=FIRST_MONTH):
#     """ Incremental assumes last available month from the previous output was fully loaded."""
#
#     if not Job.last_attempted_period:
#         Job.last_attempted_period  = Job.get_previous_output_max_timestamp() or first_month  # TODO: if get_output_max_timestamp()=None, means new build, so should delete instance in oracle.
#
#     periods = get_last_output_to_last_month(Job.last_attempted_period, first_month)
#     if len(periods) == 0:
#         Job.logger.info('Output up to date. Nothing to run. last processed period={} and last period from now={}'.format(Job.last_attempted_period, get_last_month()))
#         Job.final_inc = True
#         empty_df = pd.DataFrame()
#         return pdf_to_sdf(empty_df, Job.OUTPUT_TYPES, Job.sc, Job.sc_sql)
#         # TODO: look at output empty table instead of skipping output. i.e. removing that if.
#
#     Job.logger.info('Periods remaining to load: {}'.format(periods))
#     period = periods[0]
#     Job.logger.info('Period to be loaded in this run: {}'.format(period))
#     Job.final_inc = period == periods[-1]
#     Job.last_attempted_period = period
#     Job.args['file_tag'] = period
#
#     df = Job.transform_inc(api_params, [period], lines_override)  # TODO: check to pass it as arg so code more retracable from my classes.
#     sdf = pdf_to_sdf(df, Job.OUTPUT_TYPES, Job.sc, Job.sc_sql)
#     return sdf

# def get_last_month():
#     last_month_dt = datetime.utcnow() + relativedelta(months=-1)
#     last_month = last_month_dt.strftime("%Y-%m")
#     return last_month
#
# def get_start_to_last_month(first_month):
#     now = datetime.utcnow()
#     start = datetime.strptime(first_month, "%Y-%m")
#     delta = relativedelta(now, start)
#     number_months = delta.years * 12 + delta.months
#
#     periods = []
#     iter_month = start
#     for item in range(number_months):
#         periods.append(iter_month.strftime("%Y-%m"))
#         iter_month = iter_month + relativedelta(months=+1)
#     return periods
#
# def get_last_output_to_last_month(last_output, first_month):
#     periods = get_start_to_last_month(first_month)
#     return [item for item in periods if item > last_output]
