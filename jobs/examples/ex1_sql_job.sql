-- Params (lines starting with "----- " below) --
----- 'repartition' : 2 -----

SELECT se.session_id, count(*) as count_events
FROM some_events se
JOIN other_events oe on se.session_id=oe.session_id
WHERE se.action='searchResultPage' and se.n_results>0
group by se.session_id
order by count(*) desc

-- Path of 'some_events' and 'other_events' tables can be found in conf/jobs_metadata.yml or conf/jobs_metadata_local.yml
-- to run from jobs_metadata.yml: python jobs/generic/launcher.py --job_name=examples/ex1_sql_job.sql
-- to run standalone: python jobs/generic/sql_job.py --sql_file=jobs/examples/ex1_sql_job.sql
