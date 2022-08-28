-- SQL Query
-- Job params below (yaetos will parse lines starting with "----param---- " and finishing with "----", like below)
----param---- 'job_param_file' : 'conf/jobs_metadata.yml' ----
----param---- 'job_name' : 'examples/ex1_sql_job.sql' ----
----param---- 'param_a' : 'value_a' ----
----param---- 'param_b' : 'value_b' ----

SELECT se.session_id, count(*) as count_events
FROM some_events se
JOIN other_events oe on se.session_id=oe.session_id
WHERE se.action='searchResultPage' and se.n_results>0
group by se.session_id
order by count(*) desc

-- Path of 'some_events' and 'other_events' tables can be found in conf/jobs_metadata.yml
-- to run from jobs_metadata.yml over spark (implying params above will be ignored):  python jobs/generic/launcher.py --job_name=examples/ex1_sql_job.sql
-- to run from jobs_metadata.yml over pandas (implying params above will be ignored): python jobs/generic/launcher.py --job_name=examples/ex1_sql_pandas_job
-- to run standalone over spark (requires changing job_name to 'examples/ex1_sql_job.sql' above):     python jobs/generic/sql_spark_job.py  --sql_file=jobs/examples/ex1_sql_job.sql
-- to run standalone over pandas (requires changing job_name to 'examples/ex1_sql_pandas_job' above): python jobs/generic/sql_pandas_job.py --sql_file=jobs/examples/ex1_sql_job.sql
