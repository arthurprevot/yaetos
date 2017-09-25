-- to run: python core/sql_generic_job.py --sql_file=jobs/examples/ex1_full_sql_job.sql

SELECT se.session_id, count(*)
FROM some_events se
JOIN other_events oe on se.session_id=oe.session_id
WHERE se.action='searchResultPage' and se.n_results>0
group by se.session_id
order by count(*) desc
