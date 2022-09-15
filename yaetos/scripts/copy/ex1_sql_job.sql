-- SQL Query
-- Job params below (yaetos will parse lines starting with "----param---- " and finishing with "----", like below)
----param---- 'job_param_file' : 'conf/jobs_metadata.yml' ----
----param---- 'param_a' : 'value_a' ----
----param---- 'param_b' : 'value_b' ----

SELECT se.session_id, count(*) as count_events
FROM some_events se
JOIN other_events oe on se.session_id=oe.session_id
WHERE se.action='searchResultPage' and se.n_results>0
group by se.session_id
order by count(*) desc

-- Path of 'some_events' and 'other_events' tables can be found in conf/jobs_metadata.yml
-- to run over pandas using job_metadata.yml launcher (implying params above will be ignored): python jobs/generic/launcher.py --job_name=examples/ex1_sql_job.sql
-- to run over spark using job_metadata.yml launcher (implying params above will be ignored):  python jobs/generic/launcher.py --job_name=examples/ex1_sql_spark_job
-- to run over pandas using standalone sql launcher: python jobs/generic/sql_pandas_job.py  --sql_file=jobs/examples/ex1_sql_job.sql  # i.e. job_name derived from sql_file
-- to run over spark using standalone sql launcher:  python jobs/generic/sql_spark_job.py --sql_file=jobs/examples/ex1_sql_job.sql --job_name=examples/ex1_sql_spark_job
