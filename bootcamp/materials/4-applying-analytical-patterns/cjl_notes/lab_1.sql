
-- -- ----------------------------------------
-- -- ----------------------------------------
-- -- -------------- LAB SET UP --------------
-- -- ----------------------------------------
-- -- ----------------------------------------

-- -- CREATE TABLE users_growth_accounting (
-- --     user_id TEXT
-- --   , first_active_date DATE
-- --   , last_active_date DATE
-- --   , daily_active_state TEXT
-- --   , weekly_active_state TEXT
-- --   , dates_active DATE[]
-- --   , date DATE
-- --   , PRIMARY KEY (user_id, date)
-- -- );

-- INSERT INTO users_growth_accounting
-- WITH yesterday AS (
--   SELECT *
--   FROM users_growth_accounting
--   WHERE 1=1
--   		-- AND date = DATE('2022-12-31')
-- 		  -- AND date = DATE('2023-01-01')
-- 		  -- AND date = DATE('2023-01-02')
-- 		  -- AND date = DATE('2023-01-03')
-- 		  -- AND date = DATE('2023-01-04')
-- 		  -- AND date = DATE('2023-01-05')
-- 		  -- AND date = DATE('2023-01-06')
-- 		  -- AND date = DATE('2023-01-07')
-- 		  -- AND date = DATE('2023-01-08')
-- 		  -- AND date = DATE('2023-01-09')
-- 		  AND date = DATE('2023-01-10')

-- 		  -- AND date = DATE('2023-01-11')
-- 		  -- AND date = DATE('2023-01-12')
-- 		  -- AND date = DATE('2023-01-13')
-- 		  -- AND date = DATE('2023-01-14')
-- 		  -- AND date = DATE('2023-01-15')
-- 		  -- AND date = DATE('2023-01-16')
-- 		  -- AND date = DATE('2023-01-17')
-- 		  -- AND date = DATE('2023-01-18')
-- 		  -- AND date = DATE('2023-01-19')

-- 		  -- AND date = DATE('2023-01-21')
-- 		  -- AND date = DATE('2023-01-22')
-- 		  -- AND date = DATE('2023-01-23')
-- 		  -- AND date = DATE('2023-01-24')
-- 		  -- AND date = DATE('2023-01-25')
-- 		  -- AND date = DATE('2023-01-26')
-- 		  -- AND date = DATE('2023-01-27')
-- 		  -- AND date = DATE('2023-01-28')
-- 		  -- AND date = DATE('2023-01-29')
-- 		  -- AND date = DATE('2023-01-30')
-- 		  -- AND date = DATE('2023-01-31')
-- )

-- , today AS (
--   SELECT
--     CAST(user_id AS TEXT) AS user_id
--   , DATE(DATE_TRUNC('DAY', event_time::timestamp)) AS today_date
--   , COUNT(1)
--   FROM events
--   WHERE 1=1
--   		AND user_id IS NOT NULL
--   		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-01')
-- 	    -- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-02')
-- 	    -- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-03')
-- 	    -- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-04')
-- 	    -- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-05')
-- 	    -- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-06')
-- 	    -- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-07')
-- 	    -- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-08')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-09')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-10')
-- 		AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-11')
		
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-02')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-13')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-14')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-15')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-16')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-17')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-18')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-19')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-20')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-21')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-22')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-23')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-24')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-25')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-26')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-27')
-- 	 --    AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-28')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-29')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-30')
-- 		-- AND DATE_TRUNC('DAY', event_time::timestamp) = DATE('2023-01-31')
--   GROUP BY 1,2
-- )

-- SELECT 
--   COALESCE(t.user_id, y.user_id) AS user_id
-- , COALESCE(y.first_active_date, t.today_date) AS first_active_date
-- , COALESCE(t.today_date, y.first_active_date) AS last_active_date
-- , CASE
--     WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
-- 	WHEN y.last_active_date = t.today_date - INTERVAL '1 DAY' THEN 'Retained'
-- 	WHEN y.last_active_date < t.today_date - INTERVAL '1 DAY' THEN 'Resurrected'
-- 	WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
-- 	ELSE 'Stale'
--   END AS daily_active_state
-- , CASE 
--     WHEN y.user_id IS NULL AND t.user_id IS NOT NULL THEN 'New'
-- 	WHEN y.last_active_date >= y.date - INTERVAL '7 DAY' THEN 'Retained'
-- 	WHEN y.last_active_date < t.today_date - INTERVAL '7 DAY' THEN 'Resurrected'
-- 	WHEN t.today_date IS NULL AND y.last_active_date = y.date - INTERVAL '7 DAY' THEN 'Churned'
-- 	ELSE 'Stale'
--   END AS weekly_active_state
-- , COALESCE(y.dates_active, ARRAY[]::DATE[]) || CASE
--     WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
-- 	ELSE ARRAY[]::DATE[]
--   END AS date_list
-- , DATE(COALESCE(t.today_date, y.date + INTERVAL '1 DAY')) AS date
-- FROM today t 
-- FULL OUTER JOIN yesterday y
--   ON t.user_id = y.user_id




-- ------------------------------------------
-- ------------------------------------------
-- -------------- DAILY STATES --------------
-- ------------------------------------------
-- ------------------------------------------

-- SELECT
--   date
-- , daily_active_state
-- , COUNT(1) AS num_users
-- FROM users_growth_accounting
-- WHERE 1=1
-- GROUP BY 1,2
-- ORDER BY 1,2





------------------------------------------------------------
------------------------------------------------------------
-------------- RETENTION (first_active_state) --------------
------------------------------------------------------------
------------------------------------------------------------
SELECT 
  -- date
  EXTRACT(DOW FROM first_active_date) AS dow
, date - first_active_date AS days_since_first_active
, COUNT(CASE WHEN daily_active_state IN ('New', 'Retained', 'Resurrected') THEN 1 END) AS num_active
, COUNT(1) AS cohort_size
, ROUND(CAST(COUNT(CASE WHEN daily_active_state IN ('New', 'Retained', 'Resurrected') THEN 1 END) AS NUMERIC) / COUNT(1), 4)
FROM users_growth_accounting
WHERE 1=1
	  -- AND first_active_date = DATE('2023-01-01')
GROUP BY 1,2
ORDER BY 1,2

