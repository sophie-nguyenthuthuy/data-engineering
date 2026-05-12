SELECT MIN(n.name), MIN(t.title)
FROM cast_info ci,
     name n,
     title t,
     aka_title at_alias,
     movie_info mi,
     info_type it
WHERE n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = at_alias.movie_id
  AND t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND it.info = 'release dates'
  AND n.gender = 'f'
  AND t.production_year > 2000;
