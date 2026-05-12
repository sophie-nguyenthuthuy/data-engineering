SELECT MIN(an.name), MIN(t.title)
FROM title t,
     movie_companies mc,
     company_name cn,
     company_type ct,
     cast_info ci,
     name n,
     aka_name an
WHERE t.id = mc.movie_id
  AND cn.id = mc.company_id
  AND ct.id = mc.company_type_id
  AND t.id = ci.movie_id
  AND n.id = ci.person_id
  AND n.id = an.person_id
  AND ct.kind = 'production companies'
  AND cn.country_code = '[us]';
