SELECT MIN(n.name), MIN(t.title)
FROM cast_info ci,
     name n,
     title t,
     movie_link ml,
     link_type lt
WHERE ci.person_id = n.id
  AND ci.movie_id = t.id
  AND t.id = ml.movie_id
  AND lt.id = ml.link_type_id
  AND lt.link = 'references';
