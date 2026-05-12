SELECT COUNT(*)
FROM title t,
     movie_info mi,
     info_type it,
     movie_keyword mk,
     keyword k
WHERE t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND mk.movie_id = t.id
  AND k.id = mk.keyword_id
  AND it.info = 'rating'
  AND k.keyword = 'sequel';
