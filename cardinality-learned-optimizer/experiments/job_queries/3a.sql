SELECT MIN(t.title)
FROM title t,
     movie_info mi,
     info_type it,
     movie_companies mc,
     company_name cn
WHERE t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND t.id = mc.movie_id
  AND cn.id = mc.company_id
  AND it.info = 'genres'
  AND mi.info IN ('Drama', 'Horror')
  AND cn.country_code = '[us]'
  AND t.production_year > 1990;
