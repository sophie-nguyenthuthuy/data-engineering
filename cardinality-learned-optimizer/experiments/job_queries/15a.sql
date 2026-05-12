SELECT MIN(at_alias.title), MIN(t.title)
FROM title t,
     aka_title at_alias,
     movie_companies mc,
     company_name cn,
     movie_info mi,
     info_type it
WHERE t.id = at_alias.movie_id
  AND t.id = mc.movie_id
  AND cn.id = mc.company_id
  AND t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND it.info = 'rating'
  AND cn.country_code = '[us]'
  AND t.production_year > 1993;
