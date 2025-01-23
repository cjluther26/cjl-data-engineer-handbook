-- SELECT 
--   game_id 
-- , team_id 
-- , player_id
-- FROM game_details
-- GROUP BY 1,2,3
-- HAVING COUNT(1) > 1
-- ;

-- SELECT * 
-- FROM game_details
-- WHERE 1=1
-- 	  AND game_id = 22000043 AND team_id = 1610612762 AND player_id = 201144

/* 
1. Write a query to deduplicate `game_details` from Day 1 so there's no duplicates.
*/

-- WITH game_details_deduped AS (
--   SELECT 
--     gd.game_id AS dim_game_id
--   , g.game_date_est AS dim_game_date
--   , g.season AS dim_season
--   , player_id AS dim_player_id
--   , player_name AS dim_player_name
--   , start_position AS dim_start_position
--   , gd.team_id = g.home_team_id AS dim_is_playing_at_home
--   , COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play
--   , COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress
--   , COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team
--   , CAST(SPLIT_PART(min, ':', 1) AS REAL) + (CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60) AS m_minutes
--   , fgm AS m_fgm
--   , fga AS m_fga
--   , fg3m AS m_fg3m
--   , fg3a AS m_fg3a
--   , ftm AS m_ftm
--   , fta AS m_fta
--   , oreb AS m_oreb
--   , dreb AS m_dreb
--   , reb AS m_reb
--   , ast AS m_ast
--   , stl AS m_stl
--   , blk AS m_blk
--   , "TO" AS m_turnovers
--   , pf AS m_pf
--   , pts AS m_pts
--   , plus_minus AS m_plus_minus
--   , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est ASC) AS r
--   FROM game_details gd
--   LEFT JOIN games g 
--     ON gd.game_id = g.game_id
--   WHERE 1=1
-- )

-- , final_data AS (
--   SELECT *
--   FROM game_details_deduped
--   WHERE 1=1
-- 	    AND r = 1
-- )

-- SELECT * FROM final_data
-- ;

/* 
2. Write a DDL for an `user_devices_cumulated` table that has:
     - a `device_activity_datelist` which tracks a users active days by `browser_type`
     - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
         - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)
*/
-- CREATE TABLE user_devices_cumulated (
--   user_id TEXT 
-- , date DATE
-- , browser_type TEXT
-- , device_activity_datelist DATE[]
-- )

/* 
3. Write a cumulative query to generate device_activity_datelist from events.
*/


INSERT INTO user_devices_cumulated 
-- WITH unique_devices AS (
--   SELECT 
--     device_id 
--   , browser_type
--   FROM devices
--   GROUP BY 1,2
-- )

-- , yesterday_data AS (
--   SELECT 
-- 	*
--   FROM user_devices_cumulated
--   WHERE 1=1
--         -- AND date =  DATE('2022-12-31')
--         -- AND date =  DATE('2023-01-01')
-- 		-- AND date =  DATE('2023-01-02')
-- 		-- AND date =  DATE('2023-01-03')
-- 		-- AND date =  DATE('2023-01-04')
-- 		AND date =  DATE('2023-01-05')
-- )

-- , today_data AS (
--   SELECT 
--     CAST(e.user_id AS TEXT) AS user_id
--   , DATE(DATE_TRUNC('DAY', CAST(e.event_time AS DATE))) AS today_dt
--   , d.browser_type
--   , 1 AS num_events
--   FROM events e
--   LEFT JOIN unique_devices d 
--     ON e.device_id = d.device_id
--   WHERE 1=1
--         AND e.user_id IS NOT NULL
		
--   		-- AND DATE_TRUNC('DAY', CAST(e.event_time AS DATE)) = DATE('2023-01-01')
-- 		-- AND DATE_TRUNC('DAY', CAST(e.event_time AS DATE)) = DATE('2023-01-02')
-- 		-- AND DATE_TRUNC('DAY', CAST(e.event_time AS DATE)) = DATE('2023-01-03')
-- 		-- AND DATE_TRUNC('DAY', CAST(e.event_time AS DATE)) = DATE('2023-01-04')
-- 		-- AND DATE_TRUNC('DAY', CAST(e.event_time AS DATE)) = DATE('2023-01-05')
-- 		AND DATE_TRUNC('DAY', CAST(e.event_time AS DATE)) = DATE('2023-01-06')
--   GROUP BY 1,2,3
-- )

-- -- SELECT *
-- -- FROM today_data
-- -- WHERE user_id = '70132547320211180'

-- , cumulated_data AS (
--   SELECT 
--     COALESCE(t.user_id, y.user_id) AS user_id
--   , COALESCE(t.today_dt, y.date + INTERVAL '1 DAY') AS date
--   , COALESCE(t.browser_type, y.browser_type) AS browser_type
--   , COALESCE(y.device_activity_datelist, ARRAY[]::DATE[]) || CASE
--       WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_dt]
-- 	  ELSE ARRAY[]::DATE[]
-- 	END AS device_activity_datelist
--   FROM today_data t
--   FULL OUTER JOIN yesterday_data y
--     ON t.user_id = y.user_id AND t.browser_type = y.browser_type
-- )

-- SELECT *
-- FROM cumulated_data


-- SELECT * FROM user_devices_cumulated






/* 
4. Write a datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column.
*/

-- WITH user_devices AS (
--   SELECT 
--     *
--   FROM user_devices_cumulated 
--   WHERE 1=1
--   		AND date = DATE('2023-01-06')
-- )

-- , series AS (
--   SELECT 
--     *
--   FROM GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-06'), INTERVAL '1 DAY') AS series_date
-- )

-- , placeholder_ints AS (
--   SELECT 
--     user_devices.*
--   , CASE 
--       WHEN device_activity_datelist @> ARRAY[DATE(series_date)] THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
-- 	  ELSE 0
-- 	END AS placeholder_int_value
--   FROM user_devices
--   CROSS JOIN series
--   WHERE 1=1
-- )

-- , final_data AS (
--   SELECT 
--     date
--   , user_id 
--   , browser_type
--   , CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
--   , BIT_COUNT(CAST('10000000000000000000000000000000' AS BIT(32)) 
--       & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_daily_active
--   , BIT_COUNT(CAST('11111110000000000000000000000000' AS BIT(32)) & CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) > 0 AS dim_is_weekly_active
--   FROM placeholder_ints
--   WHERE 1=1
--   GROUP BY 1,2,3
-- )

-- SELECT *
-- FROM final_data
-- WHERE 1=1
-- 	  -- AND user_id = '11780863980750100000'
-- ORDER BY user_id, browser_type




/* 
5. Write a DDL for `hosts_cumulated` table
     - with a `host_activity_datelist` which logs to see which dates each host is experiencing any activity
*/

-- CREATE TABLE hosts_cumulated (
--   host TEXT
-- , date DATE
-- , host_activity_datelist DATE[]
-- )


/* 
6. Write an incremental query to generate `host_activity_datelist`
*/

-- INSERT INTO hosts_cumulated
-- WITH yesterday_data AS (
--   SELECT *
--   FROM hosts_cumulated
--   WHERE 1=1
--         -- AND date =  DATE('2022-12-31')
--         -- AND date =  DATE('2023-01-01')
-- 		-- AND date =  DATE('2023-01-02')
-- 		-- AND date =  DATE('2023-01-03')
-- 		-- AND date =  DATE('2023-01-04')
-- 		-- AND date =  DATE('2023-01-05')
-- 		-- AND date =  DATE('2023-01-06')
-- 		-- AND date =  DATE('2023-01-07')
-- 		-- AND date =  DATE('2023-01-08')
-- 		AND date =  DATE('2023-01-09')
-- )

-- , today_data AS (
--   SELECT 
--     host
--   , DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) AS today_dt
--   , COUNT(1) AS num_hits
--   FROM events
--   WHERE 1=1
--         AND user_id IS NOT NULL 
		
--   	 	-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-01')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-02')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-03')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-04')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-05')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-06')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-07')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-08')
-- 		-- AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-09')
-- 		AND DATE(DATE_TRUNC('DAY', CAST(event_time AS DATE))) = DATE('2023-01-10')
--   GROUP BY 1,2
-- )

-- , cumulated_data AS (
--   SELECT 
--     COALESCE(t.host, y.host) AS host
--   , DATE(COALESCE(t.today_dt, y.date + INTERVAL '1 DAY')) AS date
--   , CASE
--       WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.today_dt]
-- 	  WHEN t.today_dt IS NULL THEN y.host_activity_datelist
-- 	  ELSE y.host_activity_datelist || ARRAY[t.today_dt]
-- 	END AS host_activity_datelist
--   FROM today_data t 
--   FULL OUTER JOIN yesterday_data y
--     ON t.host = y.host
-- )

-- SELECT *
-- FROM cumulated_data

-- SELECT * FROM hosts_cumulated


/* 
7. Write a monthly, reduced fact table DDL `host_activity_reduced`
*/

-- CREATE TABLE host_activity_reduced (
--   month DATE
-- , host TEXT 
-- , hit_array REAL[]
-- , unique_visitors_array REAL[]
-- , PRIMARY KEY (month, host)
-- )


/* 
8. Write an incremental query that loads `host_activity_reduced`
*/

-- INSERT INTO host_activity_reduced
-- WITH month_start_data AS (
--   SELECT 
--     *
--   FROM host_activity_reduced
--   WHERE 1=1
--   		AND month = DATE('2023-01-01')
-- )

-- , daily_agg AS (
--   SELECT 
--     host
--   , DATE(event_time) AS dt 
--   , COUNT(1) AS num_hits
--   , COUNT(DISTINCT user_id) AS unique_visitors
--   FROM events
--   WHERE 1=1
--   		AND user_id IS NOT NULL 

-- 		-- AND DATE(event_time) = DATE('2023-01-01')
-- 		-- AND DATE(event_time) = DATE('2023-01-02')
-- 		-- AND DATE(event_time) = DATE('2023-01-03')
-- 		-- AND DATE(event_time) = DATE('2023-01-04')
-- 		-- AND DATE(event_time) = DATE('2023-01-05')
-- 		-- AND DATE(event_time) = DATE('2023-01-06')
-- 		-- AND DATE(event_time) = DATE('2023-01-07')
-- 		-- AND DATE(event_time) = DATE('2023-01-08')
-- 		-- AND DATE(event_time) = DATE('2023-01-09')
-- 		-- AND DATE(event_time) = DATE('2023-01-10')
-- 		-- AND DATE(event_time) = DATE('2023-01-11')
-- 		-- AND DATE(event_time) = DATE('2023-01-12')
-- 		-- AND DATE(event_time) = DATE('2023-01-13')
-- 		-- AND DATE(event_time) = DATE('2023-01-14')
-- 		AND DATE(event_time) = DATE('2023-01-15')
--   GROUP BY 1,2
-- )

-- , combined_data AS (
--   SELECT 
--     COALESCE(msd.month, DATE(DATE_TRUNC('MONTH', da.dt))) AS month
--   , COALESCE(da.host, msd.host) AS host 
--   , CASE 
--       WHEN msd.hit_array IS NOT NULL THEN msd.hit_array || ARRAY[COALESCE(da.num_hits, 0)]
-- 	  WHEN msd.hit_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(da.dt - DATE(DATE_TRUNC('MONTH', da.dt)), 0)]) || ARRAY[COALESCE(da.num_hits, 0)]
-- 	  WHEN msd.month IS NULL THEN ARRAY[COALESCE(da.num_hits, 0)]
-- 	END AS hit_array
--   , CASE 
--       WHEN msd.unique_visitors_array IS NOT NULL THEN msd.unique_visitors_array || ARRAY[COALESCE(da.unique_visitors, 0)]
-- 	  WHEN msd.unique_visitors_array IS NULL THEN ARRAY_FILL(0, ARRAY[COALESCE(da.dt - DATE(DATE_TRUNC('MONTH', da.dt)), 0)]) || ARRAY[COALESCE(da.unique_visitors, 0)]
-- 	  WHEN msd.month IS NULL THEN ARRAY[COALESCE(da.unique_visitors, 0)]
-- 	END AS unique_visitors_array
--   FROM daily_agg da 
--   FULL OUTER JOIN month_start_data msd
--     ON da.host = msd.host
  
-- )

-- SELECT *
-- FROM combined_data

-- ON CONFLICT (month, host)
-- DO
--   UPDATE SET hit_array = EXCLUDED.hit_array, unique_visitors_array = EXCLUDED.unique_visitors_array;


-- SELECT * FROM host_activity_reduced


/* 
BONUS -- how to unravel multiple arrays together!
*/
WITH all_host_totals_prep AS (
  SELECT 
    month 
  , ARRAY[SUM(hit_array[1]), SUM(hit_array[2]), SUM(hit_array[3]), SUM(hit_array[4]), SUM(hit_array[5])] AS hit_values
  , ARRAY[SUM(unique_visitors_array[1]), SUM(unique_visitors_array[2]), SUM(unique_visitors_array[3]), SUM(unique_visitors_array[4]), SUM(unique_visitors_array[5])] AS unique_visitor_values
  FROM host_activity_reduced
  WHERE 1=1
  GROUP BY 1
)

, all_host_totals AS (
  SELECT 
    month 
  , DATE(month + CAST(CAST(a.index - 1 AS TEXT) || 'day' AS INTERVAL)) AS adj_date
  , a.elem AS total_hits
  , b.elem AS total_unique_visitors
  FROM all_host_totals_prep, UNNEST(hit_values) WITH ORDINALITY AS a(elem, index)
  INNER JOIN UNNEST(unique_visitor_values) WITH ORDINALITY AS b(elem, index)
  	ON a.index = b.index
)

SELECT *
FROM all_host_totals
-- CROSS JOIN UNNEST(all_host_totals.unique_visitor_values) WITH ORDINALITY AS b(elem_2, index_2)

