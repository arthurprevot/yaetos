from core.etl_utils import etl


class sql_job(etl):
    """To run/deploy sql jobs, using --sql_file arg."""

    def transform(self, **ignored):
        sql = self.read_sql_file(self.args['sql_file'])
        df = self.query(sql)
        return df

    def get_app_name(self):
        return self.args['sql_file'].split('/')[-1].replace('.sql','')  # Quick and dirty, forces name of sql file to match schedule entry

    @staticmethod
    def define_commandline_args():
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
    sql_job().commandline_launch(aws_setup='perso')  # aws_setup can be overriden in commandline if required
