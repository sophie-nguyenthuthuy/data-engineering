SELECT MIN(cn.name), MIN(t.title)
FROM title t,
     movie_companies mc,
     company_name cn,
     movie_keyword mk,
     keyword k,
     movie_info mi,
     info_type it
WHERE t.id = mc.movie_id
  AND cn.id = mc.company_id
  AND t.id = mk.movie_id
  AND k.id = mk.keyword_id
  AND t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND it.info = 'rating'
  AND k.keyword IN ('action', 'adventure')
  AND t.production_year > 2000;
