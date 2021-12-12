-- Path of 'some_events' and 'other_events' tables can be found in conf/jobs_metadata.yml or conf/jobs_metadata_local.yml
-- to run: python jobs/generic/launcher.py --job_name=examples/ex1_full_sql_job.sql

SELECT se.session_id, count(*) as count_events
FROM some_events se
JOIN other_events oe on se.session_id=oe.session_id
WHERE se.action='searchResultPage' and se.n_results>0
group by se.session_id
order by count(*) desc
