
-- /* Analytical Query */
-- SELECT
--   player_name
-- , (season_stats[CARDINALITY(season_stats)]::season_stats).pts AS pts
-- , COALESCE((season_stats[1]::season_stats).pts, 1) AS first_season_pts
-- , (season_stats[CARDINALITY(season_stats)]::season_stats).pts / COALESCE(GREATEST((season_stats[1]::season_stats).pts, 1), 1) AS pts_ratio_first_season_to_1998
-- FROM players
-- WHERE 1=1
-- 	  AND current_season = 1998
-- ORDER BY 3 DESC


-- /* Incremental SCD Query (Practice) */
-- WITH last_season_scd AS (
--   SELECT *
--   FROM players_scd
--   WHERE 1=1
--   		AND current_season = 2021
-- 		AND end_season = 2021
-- )

-- , historical_scd AS (
--   SELECT 
--     player_name
--   , scoring_class
--   , is_active
--   , start_season
--   , end_season
--   FROM players_scd
--   WHERE 1=1
--   		AND current_season = 2021
-- 		AND end_season = 2021
-- )

-- , this_season_data AS (
--   SELECT *
--   FROM players
--   WHERE 1=1
--   		AND current_season = 2022
-- )

-- , unchanged_records AS (
--   SELECT 
--     ts.player_name
--   , ts.scoring_class
--   , ts.is_active
--   , ls.start_season
--   , ts.current_season AS end_season
--   FROM this_season_data ts
--   INNER JOIN last_season_scd ls
--     ON ts.player_name = ls.player_name
--   WHERE 1=1
--   		AND ts.scoring_class = ls.scoring_class
-- 		AND ts.is_active = ls.is_active
-- )

-- , changed_records AS (
--   SELECT 
--     ts.player_name
--   , UNNEST(ARRAY[
--     ROW(
--       ls.scoring_class
-- 	, ls.is_active
-- 	, ls.start_season
-- 	, ls.end_season
-- 	)::scd_type
--   , ROW(
--       ts.scoring_class
-- 	, ts.is_active
-- 	, ts.current_season
-- 	, ts.current_season
-- 	)::scd_type
--   ]
--   ) AS records
--   FROM this_season_data ts
--   LEFT JOIN last_season_scd ls
--     ON ts.player_name = ls.player_name
--   WHERE 1=1
--   		AND (
-- 		  ts.scoring_class <> ls.scoring_class
-- 		  OR
-- 		  ts.is_active <> ls.is_active
-- 		)
-- )

-- -- SELECT * FROM changed_records

-- , unnested_changed_records AS (
--   SELECT 
--     player_name
--   , (records::scd_type).scoring_class
--   , (records::scd_type).is_active
--   , (records::scd_type).start_season
--   , (records::scd_type).end_season
--   FROM changed_records
-- )

-- -- SELECT * FROM unnested_changed_records

-- , new_records AS (
--   SELECT 
--     ts.player_name
--   , ts.scoring_class
--   , ts.is_active 
--   , ts.current_season AS start_season
--   , ts.current_season AS end_season
--   FROM this_season_data ts
--   LEFT JOIN last_season_scd ls
--     ON ts.player_name = ls.player_name
--   WHERE 1=1
--   		AND ls.player_name IS NULL
-- )

-- , final_data AS (
--   SELECT *
--   FROM historical_scd

--   UNION ALL 

--   SELECT *
--   FROM unchanged_records

--   UNION ALL 

--   SELECT *
--   FROM unnested_changed_records

--   UNION ALL 

--   SELECT *
--   FROM new_records
-- )

-- SELECT 
--   player_name
-- , scoring_class
-- , is_active
-- , start_season
-- , end_season
-- , 2022 current_season
-- FROM final_data
-- ORDER BY 1,4









-- /* GRAPH DDLs */
-- CREATE TYPE vertex_type AS ENUM(
--   'player'
-- , 'team'
-- , 'game'
-- );

-- CREATE TABLE vertices (
--   identifier TEXT
-- , type vertex_type
-- , properties JSON
-- , PRIMARY KEY (identifier, type)
-- );

--  CREATE TYPE edge_type AS ENUM(
--   'plays_against'
-- , 'shares_team'
-- , 'plays_in'
-- , 'plays_on'
-- );

-- CREATE TABLE edges (
--   subject_identifier TEXT
-- , subject_type vertex_type
-- , object_identifier TEXT
-- , object_type vertex_type
-- , edge_type edge_type
-- , properties JSON 
-- , PRIMARY KEY (subject_identifier, subject_type, object_identifier, object_type, edge_type)
-- );



-- /* Team Vertices */
-- INSERT INTO vertices
-- WITH teams_deduped AS (
--   SELECT 
--     *
--   , ROW_NUMBER() OVER(PARTITION BY team_id) AS r
--   FROM teams
--   WHERE 1=1
-- )

-- SELECT 
--   team_id AS identifier
-- , 'team'::vertex_type AS type
-- , JSON_BUILD_OBJECT(
--     'abbreviation', abbreviation
--   , 'nickname', nickname
--   , 'city', city
--   , 'arena', arena
--   , 'year_founded', yearfounded
-- ) AS properties
-- FROM teams_deduped
-- WHERE 1=1
-- 	  AND r = 1



-- /* Player-Game Edges */
-- INSERT INTO edges
-- WITH deduped_game_details AS (
--   SELECT
  --   player_id 
  -- , game_id 
  -- , start_position
  -- , pts
  -- , team_id
  -- , team_abbreviation
  -- , ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS r
  -- FROM game_details
--   WHERE 1=1
-- )

-- SELECT 
--   player_id AS subject_identifier
-- , 'player' AS subject_type
-- , game_id AS object_identifier
-- , 'game' AS object_type
-- , 'plays_in' AS edge_type
-- , JSON_BUILD_OBJECT(
--     'start_position', start_position
--   , 'pts', pts
--   , 'team_id', team_id
--   , 'team_abbreviation', team_abbreviation
-- ) AS properties
-- FROM deduped_game_details
-- WHERE 1=1
-- 	  AND r = 1


/* Player-Player Edges */

WITH deduped_game_details_prep AS (
  SELECT 
    player_id 
  , player_name
  , game_id 
  , start_position
  , pts
  , team_id
  , team_abbreviation
  , ROW_NUMBER() OVER(PARTITION BY player_id, game_id) AS r
  FROM game_details
  WHERE 1=1
)

, deduped_game_details AS (
  SELECT 
    player_id 
  , player_name
  , game_id 
  , start_position
  , pts
  , team_id
  , team_abbreviation
  FROM deduped_game_details_prep
  WHERE 1=1
  		AND r = 1
)

, aggregated_data AS (
  SELECT 
    CONCAT(f1.player_id , f2.player_id) AS pk
  , f1.player_id 
  , f1.player_name
  , f2.player_id
  , f2.player_name
  , CASE 
      WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
	  ELSE 'plays_against'::edge_type
	END AS edge_type 
  , COUNT(1) AS num_games
  , SUM(f1.pts) AS left_points
  , SUM(f2.pts) AS right_points
  FROM deduped_game_details f1
  INNER JOIN deduped_game_details f2
    ON 1=1
	   AND f1.game_id = f2.game_id
	   AND f1.player_id <> f2.player_id
  WHERE 1=1
  	  -- Keeping only L-R relationship (instead of L-R & R-L!)
      AND f1.player_id > f2.player_id
  GROUP BY 1,2,3,4,5,6
)


SELECT *
FROM aggregated_data
WHERE 1=1
	  

	  