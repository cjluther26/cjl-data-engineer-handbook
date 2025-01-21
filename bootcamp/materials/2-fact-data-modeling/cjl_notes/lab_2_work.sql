-- CREATE TABLE users_cumulated (
--   user_id TEXT
--   -- list of the dates in the past where the user was active
-- , dates_active DATE[] 
--   -- current date for the user
-- , date DATE
-- , PRIMARY KEY (user_id, date)
-- );



-- INSERT INTO users_cumulated
-- WITH yesterday AS (
--   SELECT 
--     *
--   FROM users_cumulated
--   WHERE 1=1
--   		-- AND date = DATE('2022-12-31') -- min_date was '2023-01-01'
-- 		-- AND date = DATE('2023-01-01')
-- 		-- AND date = DATE('2023-01-02')
-- 		-- AND date = DATE('2023-01-03'
-- 		-- AND date = DATE('2023-01-04')
-- 		-- AND date = DATE('2023-01-05')
-- 		-- AND date = DATE('2023-01-06')
-- 		-- AND date = DATE('2023-01-07')
-- 		-- AND date = DATE('2023-01-08')
-- 		-- AND date = DATE('2023-01-09')
-- 		AND date = DATE('2023-01-10')
-- 		-- ... AND date = DATE('2023-01-30')
-- )
-- , today AS (
--   SELECT 
--     CAST(user_id AS TEXT) AS user_id
--   , DATE(CAST(event_time AS TIMESTAMP)) AS date_active
--   FROM events
--   WHERE 1=1
--         AND user_id IS NOT NULL
		
--   		-- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-01')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-02')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-03')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-04')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-05')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-06')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-07')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-08')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-09')
-- 		  -- AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-10')
-- 		  AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-11')
-- 		  -- ... AND DATE(CAST(event_time AS TIMESTAMP)) = DATE('2023-01-31')
--   GROUP BY 1,2
-- )

-- SELECT 
--   COALESCE(t.user_id, y.user_id) AS user_id 
-- , CASE 
--     WHEN y.dates_active IS NULL THEN ARRAY[t.date_active]
-- 	WHEN t.date_active IS NULL THEN y.dates_active
-- 	ELSE y.dates_active || ARRAY[t.date_active]
--   END AS dates_active
-- , COALESCE(t.date_active, y.date + INTERVAL '1 DAY') AS date
-- FROM today t 
-- FULL OUTER JOIN yesterday y
--   ON t.user_id = y.user_id
-- ;

/* He went all the way to 2023-01-31, but did so manually. I didn't to save time! */

SELECT *
FROM users_cumulated 
WHERE 1=1
	  AND date = '2023-01-11';





/* datelist_int */
-- SELECT * FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-11'), INTERVAL '1 DAY')

WITH users AS (
  SELECT 
    *
  FROM users_cumulated
  WHERE 1=1
  		AND date = DATE('2023-01-11')
)

, series AS (
  SELECT 
    *
  FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-11'), INTERVAL '1 DAY') AS series_date
)

, placeholder_ints AS (
  SELECT 
    CASE 
  	  -- ACTIVE ON GIVEN DATE (if active on given date, then return 2^32. This leverages CAST-ing power of 2 of BITS and turning it into BINARY)
      WHEN dates_active @> ARRAY[DATE(series_date)] THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
	  ELSE 0
    END AS placeholder_int_value
  , users.*
  FROM users
  CROSS JOIN series
  WHERE 1=1
	    -- AND user_id = '1019427114861370600'
)

-- SELECT * FROM placeholder_ints

, final_data AS (
  SELECT 
    user_id
  , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
  , BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_monthly_active
  -- `dim_is_weekly_active`: BITWISE AND -- 
  , BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) &  -- 7 '1's, then all 0s
  	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active
  , BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) & 
  	CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
  FROM placeholder_ints
  GROUP BY 1
)

SELECT * 
FROM final_data
ORDER BY user_id