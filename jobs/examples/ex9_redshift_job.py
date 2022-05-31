"""Job to show code to read/write data to redshift using spark redshift connector, instead of using pandas/sqlalchemy.
Typically not needed since data is read/written to redshift from framework, as defined in job_metadata.yml.
May require VPN to access redshift.
"""
from yaetos.etl_utils import (
    ETLBase,
    Commandliner,
    Cred_Ops_Dispatcher,
    REDSHIFT_S3_TMP_DIR,
)


class Job(ETLBase):
    def transform(self, some_events):
        creds = Cred_Ops_Dispatcher().retrieve_secrets(
            self.jargs.storage, creds=self.jargs.connection_file
        )
        creds_section = self.jargs.yml_args["copy_to_redshift"]["creds"]
        db = creds[creds_section]
        url = "jdbc:redshift://{host}:{port}/{service}".format(
            host=db["host"], port=db["port"], service=db["service"]
        )
        dbtable = "sandbox.test_ex9_redshift"

        # Writing to redshift
        self.logger.info(
            'Sending table "{}" to redshift, size "{}".'.format(
                dbtable, some_events.count()
            )
        )
        some_events.write.format("com.databricks.spark.redshift").option(
            "tempdir", REDSHIFT_S3_TMP_DIR
        ).option("url", url).option("user", db["user"]).option(
            "password", db["password"]
        ).option(
            "dbtable", dbtable
        ).mode(
            "overwrite"
        ).save()
        self.logger.info("Done sending table")

        # Reading from redshift
        self.logger.info('Pulling table "{}" from redshift'.format(dbtable))
        df = (
            self.sc_sql.read.format("com.databricks.spark.redshift")
            .option("tempdir", REDSHIFT_S3_TMP_DIR)
            .option("url", url)
            .option("user", db["user"])
            .option("password", db["password"])
            .option("dbtable", dbtable)
            .load()
        )
        count = df.count()
        self.logger.info("Done pulling table, row count:{}".format(count))

        # Output table will also be sent to redshift as required by job_metadata.yml
        return some_events


if __name__ == "__main__":
    args = {"job_param_file": "conf/jobs_metadata.yml"}
    Commandliner(Job, **args)
