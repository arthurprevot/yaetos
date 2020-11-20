from core.etl_utils import ETL_Base, Commandliner


# class Job(ETL_Base):
#     """To run/deploy any jobs, using --job_name arg."""
#
#     # def set_job_file(self):
#     #     job_file=self.args['job_name']
#     #     logger.info("job_file: '{}'".format(job_file))
#     #     return job_file
#
#     def transform(self, **kwargs):
#         import ipdb; ipdb.set_trace()
#         return df


# class SQLCommandliner(Commandliner):
#     @staticmethod
#     def define_commandline_args():
#         parser = Commandliner.define_commandline_args()
#         parser.add_argument("-h", "--job_name", help="Name of ")
#         return parser

if __name__ == "__main__":
    # SQLCommandliner(Job)
    Commandliner(Job=None)
