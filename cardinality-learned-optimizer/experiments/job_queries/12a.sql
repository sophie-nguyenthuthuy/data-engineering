SELECT MIN(cn.name), MIN(t.title)
FROM company_name cn,
     title t,
     movie_companies mc,
     movie_link ml,
     link_type lt,
     movie_keyword mk,
     keyword k
WHERE cn.id = mc.company_id
  AND t.id = mc.movie_id
  AND t.id = ml.movie_id
  AND lt.id = ml.link_type_id
  AND t.id = mk.movie_id
  AND k.id = mk.keyword_id
  AND lt.link = 'follows'
  AND cn.country_code = '[us]';
