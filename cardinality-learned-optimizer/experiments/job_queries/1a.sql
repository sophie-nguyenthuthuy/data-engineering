SELECT COUNT(*)
FROM title t,
     cast_info ci,
     name n,
     role_type rt
WHERE t.id = ci.movie_id
  AND n.id = ci.person_id
  AND rt.id = ci.role_id
  AND rt.role = 'actress'
  AND n.name LIKE '%Winslet%';
