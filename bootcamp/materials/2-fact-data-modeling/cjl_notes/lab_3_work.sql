-- -- CREATE TABLE array_metrics (
-- --   user_id NUMERIC
-- -- , month_start DATE
-- -- , metric_name TEXT
-- -- , metric_array REAL[]
-- -- , PRIMARY KEY (user_id, month_start, metric_name)
-- -- ) 

-- -- DROP TABLE array_metrics

-- INSERT INTO array_metrics
-- WITH daily_aggregate AS (
--   SELECT 
--     user_id 
--   , DATE(event_time) AS date
--   , COUNT(1) AS num_site_hits
--   FROM events
--   WHERE 1=1
--         AND user_id IS NOT NULL
-- 		-- AND DATE(event_time) = DATE('2023-01-01')
-- 		-- AND DATE(event_time) = DATE('2023-01-02')
-- 		AND DATE(event_time) = DATE('2023-01-03')
--   GROUP BY 1,2
-- )

-- -- SELECT * FROM daily_aggregate 

-- , yesterday_array AS (
--   SELECT *
--   FROM array_metrics
--   WHERE 1=1
--   		AND month_start = DATE('2023-01-01')
-- )

-- , final_data AS (
--   SELECT 
--     COALESCE(da.user_id, ya.user_id) AS user_id
--   , COALESCE(ya.month_start, DATE(DATE_TRUNC('MONTH', da.date))) AS month_start
--   , 'site_hits' AS metric_name
--   , CASE
--       WHEN ya.metric_array IS NOT NULL THEN
-- 	    ya.metric_array || ARRAY[COALESCE(da.num_site_hits, 0)]
-- 	  WHEN ya.metric_array IS NULL THEN 
-- 	  	/* Fills array with 0's before a given value is found.
-- 		   Ex: its 2023-01-07, and a new user shows up. This will add 6 (2023-01-07 - 2023-01-01) 0's to the array,
-- 		       and then add the new value!
-- 		*/
-- 	    ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('MONTH', da.date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
-- 	  WHEN ya.month_start IS NULL THEN 
-- 	    ARRAY[COALESCE(da.num_site_hits, 0)] -- new month!
--     END AS metric_array
--   FROM daily_aggregate da 
--   FULL OUTER JOIN yesterday_array ya
--     ON da.user_id = ya.user_id
-- )

-- SELECT * FROM final_data
-- ON CONFLICT (user_id, month_start, metric_name)
-- DO
--   UPDATE SET metric_array = EXCLUDED.metric_array;


-- -- SELECT * FROM array_metrics
-- -- SELECT CARDINALITY(metric_array), COUNT(1) FROM array_metrics GROUP BY 1



/* Getting counts! */
WITH agg AS (
  SELECT 
    metric_name
  , month_start
  , ARRAY[SUM(metric_array[1]), SUM(metric_array[2]), SUM(metric_array[3])] AS summed_array
  FROM array_metrics
  GROUP BY 1,2
)

SELECT 
  metric_name
, month_start + CAST(CAST(index - 1 AS TEXT) || ' DAY' AS INTERVAL)
, elem AS value
FROM agg 
CROSS JOIN UNNEST(agg.summed_array) WITH ORDINALITY AS a(elem, index)