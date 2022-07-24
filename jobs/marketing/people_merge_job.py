from yaetos.etl_utils import ETL_Base, Commandliner


class Job(ETL_Base):
    def transform(self, perso, linkedin):
        df = self.query("""
            WITH
            linkedin_mod as (
                SELECT concat(`First Name`, ' ', `Last Name`) as lk_id,
                    Company as company_lk,
                    `Connected On` as Connected_On,
                    `Email Address` as email_address,
                    `First Name` as first_name_lk,
                    `Last Name` as last_name_lk,
                    Position
                FROM linkedin
            ),
            perso_mod as (
                SELECT concat(first_name, ' ', ` last_name`) as ps_id, *
                FROM perso
            )
            SELECT coalesce(lk.lk_id, ps.ps_id) as id, lk.*, ps.*
            FROM linkedin_mod lk
            FULL OUTER JOIN perso_mod as ps on ps.ps_id=lk.lk_id
            order by 1
            """)
        return df.repartition(1)


if __name__ == "__main__":
    args = {'job_param_file': 'conf/jobs_metadata.yml'}
    Commandliner(Job, **args)
