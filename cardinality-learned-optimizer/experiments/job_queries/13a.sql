SELECT MIN(cn.name), MIN(t.title)
FROM company_name cn,
     title t,
     movie_companies mc,
     movie_info mi,
     info_type it,
     movie_info_idx mi_idx,
     info_type it2
WHERE cn.id = mc.company_id
  AND t.id = mc.movie_id
  AND t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND t.id = mi_idx.movie_id
  AND it2.id = mi_idx.info_type_id
  AND it.info = 'genres'
  AND it2.info = 'rating'
  AND mi.info IN ('Action', 'Thriller')
  AND cn.country_code = '[us]'
  AND t.production_year BETWEEN 2000 AND 2010;
