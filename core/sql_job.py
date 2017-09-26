from core.etl_utils import etl, launch


class sql_job(etl):

    def run(self, sql_file, **ignored):
        sql = self.read_sql_file(sql_file)
        tb = self.query(sql)
        return tb

    @staticmethod
    def read_sql_file(fname):
        fh = open(fname, 'r')
        sql = fh.read()
        fh.close()
        return sql


if __name__ == "__main__":
    launch(job_class=sql_job, aws_setup='perso')  # TODO: pass aws_setup as arg to make this generic.
