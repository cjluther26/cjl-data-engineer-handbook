-- SELECT * FROM public.actor_films
-- ORDER BY actorid ASC, filmid ASC LIMIT 100

------------------------------------------
------------------------------------------
------ HOMEWORK: `actor_films` data ------
------------------------------------------
------------------------------------------



----------------------------
----------------------------
-------- Question 1 --------
----------------------------
----------------------------
/*
DDL for `actors` table: Create a DDL for an actors with:
  - STRUCT `films`
  - `quality_class`
  - `is_active`
*/

-- CREATE TYPE films AS (
--   filmid TEXT 
-- , film TEXT
-- , year INTEGER
-- , votes INTEGER
-- , rating REAL 
-- )


-- CREATE TYPE quality_class AS ENUM(
--   'star'
-- , 'good'
-- , 'average'
-- , 'bad'
-- )

-- CREATE TABLE actors (
--   actorid TEXT
-- , actor TEXT
-- , year INTEGER
-- , quality_class quality_class
-- , is_active BOOLEAN
-- , films films[]
-- )


----------------------------
----------------------------
-------- Question 2 --------
----------------------------
----------------------------
/*
Cumulative table generation query:
   - Write a query that populates the actors table one year at a time.
*/

-- INSERT INTO actors
-- WITH min_max_years AS (
-- SELECT 
--   MIN(year) AS min_year
-- , MAX(year) AS max_year 
-- FROM actor_films
-- )

-- , years AS (
--   SELECT 
--     GENERATE_SERIES(min_year, max_year) AS year
--   FROM min_max_years
-- )

-- -- SELECT * FROM years

-- , actor_first_year AS (
--   SELECT 
--     actorid
--   , actor
--   , MIN(year) AS first_year
--   FROM actor_films
--   GROUP BY 1,2
-- )

-- , actor_years AS (
--   SELECT 
--     a.actorid
--   , a.actor
--   , b.year
--   FROM actor_first_year a
--   INNER JOIN years b
--     ON a.first_year <= b.year
-- )

-- -- SELECT *
-- -- FROM actor_years
-- -- WHERE 1=1
-- -- 	  AND actor = 'Leonardo DiCaprio'

-- , rolling_table AS (
--   SELECT 
--     actor_years.actorid
--   , actor_years.actor
--   , actor_years.year
--   , AVG(actor_films.rating) OVER(PARTITION BY actor_years.actorid, actor_years.year) AS avg_rating_in_year
--   , ARRAY_REMOVE(
--     ARRAY_AGG (
--       CASE WHEN actor_films.year IS NOT NULL THEN 
-- 	    ROW(
--           actor_films.filmid
-- 		, actor_films.film
-- 		, actor_films.year
-- 		, actor_films.votes
-- 		, actor_films.rating
-- 		)::films
-- 	  END
-- 	) OVER(PARTITION BY actor_years.actorid ORDER BY COALESCE(actor_years.year, actor_films.year))
--   , NULL) AS films
--   , ROW_NUMBER() OVER(PARTITION BY actor_years.actorid, COALESCE(actor_years.year, actor_films.year)) AS r
--   FROM actor_years
--   LEFT JOIN actor_films
--     ON actor_years.actorid = actor_films.actorid AND actor_years.year = actor_films.year
--   ORDER BY 1,2,3
-- )

-- -- SELECT *
-- -- FROM rolling_table
-- -- WHERE 1=1
-- -- 	  AND actor = 'Leonardo DiCaprio'
-- -- ORDER BY 1,2,3

-- , rolling_table_with_adds AS (
--   SELECT 
--     actorid
--   , actor
--   , year 
--   , ROUND(CAST(COALESCE(avg_rating_in_year, 0) AS NUMERIC), 1) AS avg_rating_in_year
--   , CASE 
--       WHEN avg_rating_in_year > 8 THEN 'star'
-- 	  WHEN avg_rating_in_year > 7 THEN 'good'
-- 	  WHEN avg_rating_in_year > 6 THEN 'average'
-- 	  ELSE 'bad'
-- 	END::quality_class AS quality_class
--   , (films[CARDINALITY(films)]::films).year = year AS is_active
--   , films
--   FROM rolling_table
--   WHERE 1=1
--   	    AND r = 1
-- )

-- -- SELECT *
-- -- FROM rolling_table_with_adds
-- -- WHERE 1=1
-- -- 	  AND actor = 'Leonardo DiCaprio'
-- -- ORDER BY 1,2,3

-- SELECT 
--   actorid 
-- , actor
-- , year
-- , quality_class
-- , is_active
-- , films
-- FROM rolling_table_with_adds

-- SELECT * FROM actors
-- WHERE 1=1
--       -- AND actor = 'Leonardo DiCaprio'
-- 	  AND actor = 'Marlon Brando'
-- ORDER BY 1,2,3



/*
3. Create a DDL for an `actors_history_scd` table with the following features:
     - Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
     - Tracks quality_class and is_active status for each actor in the actors table.
*/

-- CREATE TABLE actors_history_scd (
--   actorid TEXT
-- , actor TEXT
-- , quality_class quality_class
-- , is_active BOOLEAN
-- , start_date INTEGER
-- , end_date INTEGER
-- , PRIMARY KEY (actorid, start_date, end_date)
-- )

------- Note: 'current' year = 2021, so we'll hardcode that for this assignment

----------------------------
----------------------------
-------- Question 4 --------
----------------------------
----------------------------
/* 
Backfill query for `actors_history_scd`: 
  - Write a "backfill" query that can populate the entire actors_history_scd table in a single query.
*/


-- INSERT INTO actors_history_scd
-- WITH previous_scd AS (
--   SELECT 
--     actorid
--   , actor
--   , year
--   , LAG(quality_class) OVER(PARTITION BY actorid ORDER BY year) AS previous_quality_class
--   , quality_class
--   , LAG(is_active) OVER(PARTITION BY actorid ORDER BY year) AS previous_is_active
--   , is_active
--   FROM actors
-- )

-- , with_indicators AS (
--   SELECT 
--     actorid
--   , actor
--   , year
--   , quality_class
--   , is_active
--   , CASE 
--       WHEN previous_quality_class <> quality_class THEN 1
-- 	  WHEN previous_is_active <> is_active THEN 1
-- 	  ELSE 0
-- 	END AS change_indicator
--   FROM previous_scd
--   WHERE 1=1
--   	    AND year <= 2020
-- )

-- , with_groupings AS (
--   SELECT 
--     actorid
--   , actor
--   , year
--   , quality_class
--   , is_active
--   , SUM(change_indicator) OVER(PARTITION BY actorid ORDER BY year) AS change_group
--   , 2021 AS current_year
--   FROM with_indicators
--   WHERE 1=1
-- )

-- -- SELECT * FROM with_groupings WHERE actor = 'Leonardo DiCaprio'

-- , final_data AS (
--   SELECT 
--     actorid
--   , actor
--   , quality_class
--   , is_active
--   , change_group
--   , MIN(year) AS start_date
--   , MAX(year) AS end_date
--   FROM with_groupings
--   GROUP BY 1,2,3,4,5
-- )

-- SELECT 
--   actorid
-- , actor
-- , quality_class
-- , is_active
-- , start_date
-- , end_date
-- FROM final_data
-- ORDER BY actorid,start_date


-- SELECT * FROM actors WHERE actor = 'Leonardo DiCaprio' ORDER BY year 
-- SELECT * FROM final_data WHERE actor = 'Leonardo DiCaprio' ORDER BY actor, start_date
-- SELECT * FROM actors_history_scd WHERE actor = 'Leonardo DiCaprio' ORDER BY actor, start_date


----------------------------
----------------------------
-------- Question 5 --------
----------------------------
----------------------------
/* 
Incremental query for `actors_history_scd`: 
  - Write an "incremental" query that combines the previous year's SCD data with new incoming data from the actors table. 
*/


-- CREATE TYPE scd_type AS (
--   quality_class quality_class
-- , is_active BOOLEAN
-- , start_date INTEGER
-- , end_date INTEGER
-- );


-- CREATE TABLE actors_history_scd_prod (
--   actorid TEXT
-- , actor TEXT
-- , quality_class quality_class
-- , is_active BOOLEAN
-- , start_date INTEGER
-- , end_date INTEGER
-- , PRIMARY KEY (actorid, start_date, end_date)
-- )


INSERT INTO actors_history_scd_prod
WITH existing_data AS (
  SELECT 
    actorid
  , actor
  , quality_class
  , is_active
  , start_date
  , end_date
  FROM actors_history_scd
  WHERE 1=1
  		AND end_date < 2020
)

, last_year_data AS (
  SELECT 
    actorid
  , actor
  , quality_class
  , is_active
  , start_date
  , end_date
  FROM actors_history_scd
  WHERE 1=1
  		AND end_date = 2020
)

, new_data AS (
  SELECT 
    actorid
  , actor
  , quality_class
  , is_active
  , year
  FROM actors
  WHERE 1=1
  		AND year = 2021
)

, unchanged_records AS (
  SELECT 
    new_data.actorid
  , new_data.actor
  , new_data.quality_class
  , new_data.is_active
  , ly.start_date
  , new_data.year AS end_date
  FROM last_year_data ly
  INNER JOIN new_data
    ON 1=1
	   AND ly.actorid = new_data.actorid 
	   AND ly.quality_class = new_data.quality_class
	   AND ly.is_active = new_data.is_active
)

, changed_records AS (
  SELECT 
    new_data.actorid
  , new_data.actor
  , UNNEST(
    ARRAY[
      ROW(
        ly.quality_class
	  , ly.is_active
	  , ly.start_date
	  , ly.end_date
	  )::scd_type
	, ROW(
        new_data.quality_class
	  , new_data.is_active
	  , new_data.year
	  , new_data.year
	  )::scd_type
	]
  ) AS records
  FROM new_data
  LEFT JOIN last_year_data ly
    ON new_data.actorid = ly.actorid
  WHERE 1=1
        AND (new_data.quality_class <> ly.quality_class OR new_data.is_active <> ly.is_active)
)

-- SELECT * FROM changed_records WHERE actor = 'Javier Bardem'

, unnested_changed_records AS (
  SELECT 
    actorid
  , actor
  , (records::scd_type).quality_class
  , (records::scd_type).is_active
  , (records::scd_type).start_date
  , (records::scd_type).end_date
  FROM changed_records
)

-- SELECT * FROM unnested_changed_records WHERE actor = 'Javier Bardem'

-- , new_records AS (
--   SELECT 
--   FROM new_data
--   LEFT JOIN last_year_data ly
--     ON new_data.actorid = ly.actorid AND ly.actorid
--   WHERE 1=1
-- )

, unioned_data AS (

  SELECT * FROM existing_data

  UNION ALL 

  SELECT * FROM unchanged_records

  UNION ALL 
  
  SELECT * FROM unnested_changed_records
)

SELECT 
  actorid
, actor
, quality_class
, is_active
, start_date
, CASE WHEN end_date = 2021 THEN 2999 ELSE end_date END AS end_date
FROM unioned_data
WHERE 1=1
	  -- AND actor = 'Leonardo DiCaprio'
	  -- AND actor = 'Javier Bardem'

/* 
a MERGE INTO statement would be executed by something like Dataform here, only inserting new rows and overwriting changed ones.
BUT, for this, I'll just make a 'prod' table that reflects what that end result would look like.
*/


SELECT * 
-- FROM actors_history_scd
FROM actors_history_scd_prod
WHERE 1=1
	  -- AND actor = 'Leonardo DiCaprio'
	  AND actor = 'Javier Bardem'