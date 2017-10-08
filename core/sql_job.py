from core.etl_utils import etl_base


class sql_job(etl_base):
    """To run/deploy sql jobs, using --sql_file arg."""

    def transform(self, **ignored):
        sql = self.read_sql_file(self.args['sql_file'])
        df = self.query(sql)
        return df

    def get_app_name(self, args):
        return args['sql_file'].split('/')[-1].replace('.sql','')  # TODO: redo better

    @staticmethod
    def define_commandline_args():
        parser = etl_base.define_commandline_args()
        parser.add_argument("-s", "--sql_file", help="path of sql file to run")
        return parser

    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql


if __name__ == "__main__":
    sql_job().commandline_launch(aws_setup='perso')  # aws_setup can be overriden in commandline if required
