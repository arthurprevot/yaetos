from core.etl_utils import ETL_Base, CommandLiner


class Job(ETL_Base):
    def transform(self, processed_events):
        df = self.query("""
            SELECT timestamp_obj as other_timestamp, *
            FROM processed_events se
            order by timestamp_obj
            """)
        return df


if __name__ == "__main__":
    CommandLiner(Job, aws_setup='perso')
