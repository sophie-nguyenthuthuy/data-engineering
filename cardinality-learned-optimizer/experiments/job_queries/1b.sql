SELECT COUNT(*)
FROM title t,
     cast_info ci,
     name n
WHERE t.id = ci.movie_id
  AND n.id = ci.person_id
  AND n.name LIKE '%Hanks%'
  AND t.production_year > 2000;
