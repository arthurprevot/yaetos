------------------------------
--- Beginning output_types
--- each line with type info must have the same format as below, starting with '----- ' and finishing with '-----'
----- 'session_id':   VARCHAR(16) -----
----- 'count_events': INT() -----
------------------------------
-- Creds to run job to be specified in conf/connections.cfg with cred profile to be set in conf/jobs_metadata.yml
-- to run: python yaetos/oracle_sql_job.py --sql_file=jobs/examples/ex5_oracle_sql_job.sql

SELECT session_id, count_events
FROM test_ex5_pyspark_job
where rownum < 200
