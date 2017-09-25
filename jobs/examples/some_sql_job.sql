-- to run: 

SELECT se.session_id, count(*)
FROM some_events se
JOIN other_events oe on se.session_id=oe.session_id
WHERE se.action='searchResultPage' and se.n_results>0
group by se.session_id
order by count(*) desc
