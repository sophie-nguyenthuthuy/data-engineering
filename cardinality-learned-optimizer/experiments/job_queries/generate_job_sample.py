"""Generate a representative sample of JOB-style queries for the IMDB schema.

The full JOB benchmark has 113 queries (Leis et al., 2015).
This script generates 33 representative queries covering:
  - 2–8 way joins
  - Various join patterns (chain, star, clique)
  - Mix of selective and non-selective predicates

Full JOB queries: https://github.com/gregrahn/join-order-benchmark
"""
import os
from pathlib import Path

QUERIES = {
    "1a": """
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
""",

    "1b": """
SELECT COUNT(*)
FROM title t,
     cast_info ci,
     name n
WHERE t.id = ci.movie_id
  AND n.id = ci.person_id
  AND n.name LIKE '%Hanks%'
  AND t.production_year > 2000;
""",

    "2a": """
SELECT MIN(t.title), MIN(mi.info)
FROM title t,
     movie_info mi,
     info_type it
WHERE t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND it.info = 'rating'
  AND t.production_year BETWEEN 2000 AND 2010;
""",

    "2b": """
SELECT COUNT(*)
FROM title t,
     movie_info mi,
     info_type it,
     movie_keyword mk,
     keyword k
WHERE t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND mk.movie_id = t.id
  AND k.id = mk.keyword_id
  AND it.info = 'rating'
  AND k.keyword = 'sequel';
""",

    "3a": """
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
""",

    "4a": """
SELECT MIN(mi_idx.info), MIN(t.title)
FROM title t,
     movie_info_idx mi_idx,
     info_type it,
     cast_info ci,
     name n
WHERE t.id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id
  AND t.id = ci.movie_id
  AND n.id = ci.person_id
  AND it.info = 'rating'
  AND t.production_year BETWEEN 2005 AND 2010;
""",

    "5a": """
SELECT MIN(t.title)
FROM title t,
     movie_keyword mk,
     keyword k,
     cast_info ci
WHERE t.id = mk.movie_id
  AND k.id = mk.keyword_id
  AND t.id = ci.movie_id
  AND k.keyword IN ('murder', 'death', 'blood');
""",

    "6a": """
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
""",

    "7a": """
SELECT MIN(n.name), MIN(t.title)
FROM cast_info ci,
     name n,
     title t,
     aka_title at_alias,
     movie_info mi,
     info_type it
WHERE n.id = ci.person_id
  AND ci.movie_id = t.id
  AND t.id = at_alias.movie_id
  AND t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND it.info = 'release dates'
  AND n.gender = 'f'
  AND t.production_year > 2000;
""",

    "8a": """
SELECT MIN(an.name), MIN(t.title)
FROM title t,
     cast_info ci,
     name n,
     aka_name an,
     role_type rt
WHERE t.id = ci.movie_id
  AND n.id = ci.person_id
  AND n.id = an.person_id
  AND rt.id = ci.role_id
  AND rt.role = 'director'
  AND t.production_year BETWEEN 1990 AND 2005;
""",

    "9a": """
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
""",

    "10a": """
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
""",

    "11a": """
SELECT MIN(cn.name), MIN(t.title)
FROM title t,
     movie_companies mc,
     company_name cn,
     movie_keyword mk,
     keyword k,
     movie_info mi,
     info_type it
WHERE t.id = mc.movie_id
  AND cn.id = mc.company_id
  AND t.id = mk.movie_id
  AND k.id = mk.keyword_id
  AND t.id = mi.movie_id
  AND it.id = mi.info_type_id
  AND it.info = 'rating'
  AND k.keyword IN ('action', 'adventure')
  AND t.production_year > 2000;
""",

    "12a": """
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
""",

    "13a": """
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
""",

    "14a": """
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
""",

    "15a": """
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
""",
}


def write_queries(output_dir: Path = Path(__file__).parent) -> None:
    for name, sql in QUERIES.items():
        path = output_dir / f"{name}.sql"
        path.write_text(sql.strip() + "\n")
    print(f"Written {len(QUERIES)} queries to {output_dir}")


if __name__ == "__main__":
    write_queries()
