SELECT MIN(chn.name), MIN(t.title)
FROM title t,
     movie_companies mc,
     company_name cn,
     cast_info ci,
     char_name chn,
     role_type rt
WHERE t.id = mc.movie_id
  AND cn.id = mc.company_id
  AND t.id = ci.movie_id
  AND chn.id = ci.person_role_id
  AND rt.id = ci.role_id
  AND cn.country_code = '[us]'
  AND rt.role IN ('actor', 'actress');
