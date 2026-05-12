SELECT MIN(t.title), MIN(mi.info)
FROM title t,
     movie_info mi,
     info_type it
WHERE t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND it.info = 'rating'
  AND t.production_year BETWEEN 2000 AND 2010;
