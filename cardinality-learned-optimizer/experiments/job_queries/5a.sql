SELECT MIN(t.title)
FROM title t,
     movie_keyword mk,
     keyword k,
     cast_info ci
WHERE t.id = mk.movie_id
  AND k.id = mk.keyword_id
  AND t.id = ci.movie_id
  AND k.keyword IN ('murder', 'death', 'blood');
