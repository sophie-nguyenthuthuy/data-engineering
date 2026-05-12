SELECT MIN(mi_idx.info), MIN(t.title)
FROM title t,
     movie_info_idx mi_idx,
     info_type it,
     movie_companies mc,
     company_name cn,
     company_type ct
WHERE t.id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id
  AND t.id = mc.movie_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND it.info = 'rating'
  AND ct.kind = 'production companies'
  AND t.production_year > 2005;
