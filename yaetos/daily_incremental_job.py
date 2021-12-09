"""OBSOLETE. Will be deleted."""

from yaetos.etl_utils import ETL_Base
from datetime import datetime, date
from dateutil.relativedelta import relativedelta


class ETL_Daily_Incremental_Base(ETL_Base):
    def __init__(self, pre_jargs={}, jargs=None, loaded_inputs={}):
        super(ETL_Daily_Incremental_Base, self).__init__(pre_jargs, jargs, loaded_inputs)
        self.last_attempted_period = None

    def transform(self, **loaded_datasets):
        return self.get_transform_inc_from_last_days(**loaded_datasets)

    def transform_inc(self, period, **loaded_datasets):
        """ The function that needs to be overriden by each specific job."""
        raise NotImplementedError

    def get_transform_inc_from_last_days(self, **loaded_datasets):
        """ Incremental assumes last available month from the previous output was fully loaded."""
        first_day = self.jargs.merged_args['first_day']
        if not self.last_attempted_period:
            previous_output_max_timestamp = self.get_previous_output_max_timestamp()
            self.last_attempted_period  = previous_output_max_timestamp.strftime("%Y-%m-%d") if previous_output_max_timestamp else first_day  # TODO: if get_output_max_timestamp()=None, means new build, so should delete instance in DBs.

        periods = self.get_last_output_to_last_day(self.last_attempted_period, first_day)
        if len(periods) == 0:
            self.logger.info('Output up to date. Nothing to run. last processed period={} and last period from now={}'.format(self.last_attempted_period, self.get_last_day()))
            self.final_inc = True
            return None

        self.logger.info('Periods remaining to load: {}'.format(periods))
        period = periods[0]
        self.logger.info('Period to be loaded in this run: {}'.format(period))
        self.final_inc = period == periods[-1]
        self.last_attempted_period = period
        self.jargs.merged_args['file_tag'] = period

        df = self.transform_inc(period, **loaded_datasets)
        return df

    @staticmethod
    def get_last_day():
        last_day_dt = datetime.utcnow() + relativedelta(days=-1)
        last_day = last_day_dt.strftime("%Y-%m-%d")
        return last_day

    @staticmethod
    def get_start_to_last_day(first_day):
        now = datetime.utcnow()
        start = datetime.strptime(first_day, "%Y-%m-%d")
        delta = now - start
        number_days = delta.days

        periods = []
        iter_days = start
        for item in range(number_days):
            periods.append(iter_days.strftime("%Y-%m-%d"))
            iter_days = iter_days + relativedelta(days=+1)
        return periods

    def get_last_output_to_last_day(self, last_output, first_day):
        periods = self.get_start_to_last_day(first_day)
        return [item for item in periods if item > last_output]
