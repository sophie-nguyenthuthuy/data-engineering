SELECT MIN(an.name), MIN(t.title)
FROM title t,
     cast_info ci,
     name n,
     aka_name an,
     role_type rt
WHERE t.id = ci.movie_id
  AND n.id = ci.person_id
  AND n.id = an.person_id
  AND rt.id = ci.role_id
  AND rt.role = 'director'
  AND t.production_year BETWEEN 1990 AND 2005;
