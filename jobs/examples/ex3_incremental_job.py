from core.etl_utils import etl_base


class Job(etl_base):
    def transform(self, processed_events):
        # import ipdb; ipdb.set_trace()
        df = self.query("""
            SELECT timestamp_obj as other_timestamp, *
            FROM processed_events se
            order by timestamp_obj
            """)
        return df


if __name__ == "__main__":
    Job().commandline_launch(aws_setup='perso')
