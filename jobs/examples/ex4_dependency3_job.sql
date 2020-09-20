-- Path of 'some_events' and 'other_events' tables can be found in conf/jobs_metadata.yml or conf/jobs_metadata_local.yml
-- to run: python core/sql_job.py --sql_file=jobs/examples/ex4_dependency3_job.sql


SELECT se.session_id, session_length, doubled_length, session_length*4 as quadrupled_length
FROM some_events se
