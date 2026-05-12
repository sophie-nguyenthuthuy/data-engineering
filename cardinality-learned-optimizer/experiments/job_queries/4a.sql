SELECT MIN(mi_idx.info), MIN(t.title)
FROM title t,
     movie_info_idx mi_idx,
     info_type it,
     cast_info ci,
     name n
WHERE t.id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id
  AND t.id = ci.movie_id
  AND n.id = ci.person_id
  AND it.info = 'rating'
  AND t.production_year BETWEEN 2005 AND 2010;
