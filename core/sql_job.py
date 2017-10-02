from core.etl_utils import etl #, launch


class sql_job(etl):
    """To run/deploy sql jobs, using --sql_file arg."""

    # def transform(self, sql_file, **ignored):
    def transform(self, **ignored):
        sql = self.read_sql_file(self.args['sql_file'])
        df = self.query(sql)
        return df

    # def set_app_args(self, cmd_args):
    #     # Isolated in function for overridability
    #     self.app_args = cmd_args

    def get_app_name(self):
        # import ipdb; ipdb.set_trace()
        return self.args['sql_file'].split('/')[-1].replace('.sql','')  # Quick and dirty, forces name of sql file to match schedule entry

    # @staticmethod
    def define_commandline_args(self):
        # Defined here separatly for overridability.
        # import ipdb; ipdb.set_trace()
        # super(define_commandline_args).define_commandline_args()
        parser = etl.define_commandline_args()
        parser.add_argument("-s", "--sql_file", help="path of sql file to run")
        return parser


    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql


if __name__ == "__main__":
    sql_job().commandline_launch(aws_setup='perso')  # TODO: pass aws_setup as arg to make this generic.
    # launch(job_class=sql_job, sql_job=True, aws_setup='perso')  # TODO: pass aws_setup as arg to make this generic.
