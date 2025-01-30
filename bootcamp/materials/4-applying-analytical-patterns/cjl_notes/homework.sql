----------------------------------
----- SETTING UP DATA FOR HW -----
----------------------------------

/* Create `season_stats` TYPE */
-- CREATE TYPE season_stats AS (
--     season INTEGER
--   , gp INTEGER
--   , pts REAL
--   , reb REAL
--   , ast REAL
-- );


-- /* Create `scoring_class` TYPE */
-- CREATE TYPE scoring_class AS ENUM('star', 'good', 'average', 'bad');

-- /* Create `players` TABLE */
-- CREATE TABLE players (
--     player_name TEXT
--   , height TEXT
--   , college TEXT
--   , country TEXT
--   , draft_year TEXT
--   , draft_round TEXT
--   , draft_number TEXT
--   , season_stats season_stats[]
--   , scoring_class scoring_class
--   , years_since_last_active INTEGER
--   , current_season INTEGER
--   , is_active BOOLEAN
--   , PRIMARY KEY(player_name, current_season)
-- );



-- /* FILL `players` TABLE */
-- INSERT INTO players
-- WITH years AS (
--     SELECT *
--     FROM GENERATE_SERIES(1996, 2022) AS season
-- ), p AS (
--     SELECT
--         player_name,
--         MIN(season) AS first_season
--     FROM player_seasons
--     GROUP BY player_name
-- ), players_and_seasons AS (
--     SELECT *
--     FROM p
--     JOIN years y
--         ON p.first_season <= y.season
-- ), windowed AS (
--     SELECT
--         pas.player_name,
--         pas.season,
--         ARRAY_REMOVE(
--             ARRAY_AGG(
--                 CASE
--                     WHEN ps.season IS NOT NULL
--                         THEN ROW(
--                             ps.season,
--                             ps.gp,
--                             ps.pts,
--                             ps.reb,
--                             ps.ast
--                         )::season_stats
--                 END)
--             OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
--             NULL
--         ) AS seasons
--     FROM players_and_seasons pas
--     LEFT JOIN player_seasons ps
--         ON pas.player_name = ps.player_name
--         AND pas.season = ps.season
--     ORDER BY pas.player_name, pas.season
-- ), static AS (
--     SELECT
--         player_name,
--         MAX(height) AS height,
--         MAX(college) AS college,
--         MAX(country) AS country,
--         MAX(draft_year) AS draft_year,
--         MAX(draft_round) AS draft_round,
--         MAX(draft_number) AS draft_number
--     FROM player_seasons
--     GROUP BY player_name
-- )
-- SELECT * FROM windowed WHERE player_name = 'Ben Simmons'

-- SELECT
--     w.player_name,
--     s.height,
--     s.college,
--     s.country,
--     s.draft_year,
--     s.draft_round,
--     s.draft_number,
--     seasons AS season_stats,
--     CASE
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 20 THEN 'star'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 15 THEN 'good'
--         WHEN (seasons[CARDINALITY(seasons)]::season_stats).pts > 10 THEN 'average'
--         ELSE 'bad'
--     END::scoring_class AS scoring_class,
--     w.season - (seasons[CARDINALITY(seasons)]::season_stats).season as years_since_last_active,
--     w.season AS current_season,
--     (seasons[CARDINALITY(seasons)]::season_stats).season = season AS is_active
-- FROM windowed w
-- JOIN static s
--     ON w.player_name = s.player_name;

-- -- SELECT *
-- -- FROM players 
-- -- WHERE 1=1
-- -- 	  AND player_name = 'LeBron James'






-- /* Create `players_scd` TABLE */
-- CREATE TABLE players_scd (
-- 	player_name TEXT,
-- 	scoring_class scoring_class,
-- 	is_active BOOLEAN,
-- 	start_season INTEGER,
-- 	end_season INTEGER,
-- 	current_season INTEGER,
-- 	PRIMARY KEY (player_name, start_season, end_season)
-- );



-- /* Fill `players_scd` TABLE */
-- INSERT INTO players_scd
-- WITH with_previous AS (
--   SELECT 
--     player_name
--   , current_season
--   , scoring_class
--   , is_active
--   , LAG(scoring_class) OVER(PARTITION BY player_name ORDER BY current_season) AS previous_scoring_class
--   , LAG(is_active) OVER(PARTITION BY player_name ORDER BY current_season) AS previous_is_active
--   FROM players
-- )

-- , with_indicators AS (
--   SELECT 
--     *
--   , CASE 
--       WHEN scoring_class <> previous_scoring_class THEN 1 
--    	  WHEN is_active <> previous_is_active THEN 1 
-- 	  ELSE 0 
-- 	END AS change_indicator
--   FROM with_previous
--   WHERE 1=1
--   		AND current_season <= 2021
-- )

-- , with_streaks AS (
--   SELECT 
--     *
--   , SUM(change_indicator) OVER(PARTITION BY player_name ORDER BY current_season) AS streak_identifier
--   FROM with_indicators
--   WHERE 1=1
-- )

-- -- SELECT * FROM with_streaks WHERE player_name = 'Aaron Brooks'

-- , final_data AS (
--   SELECT 
--     player_name
--   , scoring_class
--   , is_active
--   , streak_identifier
--   , MIN(current_season) AS start_season
--   , MAX(current_season) AS end_season
--   , 2021 AS current_season
--   FROM with_streaks
--   GROUP BY 1,2,3,4
--   ORDER BY player_name, start_season
-- )

-- -- SELECT * FROM final_data WHERE player_name = 'Aaron Brooks';

-- SELECT 
--   player_name
-- , scoring_class
-- , is_active
-- , start_season
-- , end_season
-- , current_season
-- FROM final_data


-- -- SELECT * FROM players_scd WHERE player_name = 'LeBron James'


-- /* Create `scd_type` TYPE */
-- CREATE TYPE scd_type AS (
--   scoring_class scoring_class
-- , is_active BOOLEAN
-- , start_season INTEGER
-- , end_season INTEGER
-- );


-- /* Incremental pipeline query */
-- WITH historical_scd AS (
--   SELECT 
--     player_name
--   , scoring_class
--   , is_active
--   , start_season
--   , end_season
--   FROM players_scd
--   WHERE 1=1
--         AND current_season = 2021
-- 		AND end_season < 2021 -- records that are done, will not change
-- )

-- , last_season_scd AS (
--   SELECT *
--   FROM players_scd
--   WHERE 1=1
--         AND current_season = 2021
-- 		AND end_season = 2021
-- )

-- , this_season_data AS ( 
--     SELECT *
-- 	FROM players
-- 	WHERE 1=1
-- 		  AND current_season = 2022
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
--         AND ts.scoring_class = ls.scoring_class
-- 		AND ts.is_active = ls.is_active
-- )

-- -- SELECT * FROM unchanged_records WHERE player_name = 'Aaron Brooks'

-- /* all players that changed in 'current_year' (i.e. 2022) */
-- , changed_records AS (
--   SELECT 
--     ts.player_name
--   , UNNEST(ARRAY[
--       ROW(
--         ls.scoring_class
-- 	  , ls.is_active
-- 	  , ls.start_season
-- 	  , ls.end_season
-- 	  )::scd_type
-- 	, ROW(
--         ts.scoring_class
-- 	  , ts.is_active
-- 	  , ts.current_season
-- 	  , ts.current_season
-- 	  )::scd_type
--   ]) AS records
--   FROM this_season_data ts
--   LEFT JOIN last_season_scd ls
--     ON ts.player_name = ls.player_name
--   WHERE 1=1
--         AND (
-- 		     ts.scoring_class <> ls.scoring_class
-- 		  OR ts.is_active <> ls.is_active
-- 		)
-- )

-- -- SELECT * FROM changed_records WHERE player_name = 'Aaron Brooks'

-- , unnested_changed_records AS (
--   SELECT 
--     player_name
--   , (records::scd_type).scoring_class
--   , (records::scd_type).is_active
--   , (records::scd_type).start_season
--   , (records::scd_type).end_season
--   FROM changed_records
-- )

-- -- SELECT * FROM unnested_changed_records WHERE player_name = 'Aaron Brooks'

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

-- -- SELECT * FROM new_records

-- , final_data AS (
-- SELECT * FROM historical_scd

-- UNION ALL 

-- SELECT * FROM unchanged_records 

-- UNION ALL

-- SELECT * FROM unnested_changed_records

-- UNION ALL 

-- SELECT * FROM new_records
-- )

-- SELECT *
-- FROM final_data
-- WHERE 1=1
-- 	  AND player_name = 'Jimmy Butler'







----------------------------------
------------ HOMEWORK ------------
----------------------------------

-- /* 
-- 1. Write a query that does state change tracking for `players`:
-- 	- a player entering the league should be `New`
-- 	- a player leaving the league should be `Retired`
-- 	- a player staying in the league should be `Continued Playing`
-- 	- a player that comes out of retirement should be Returned from `Retirement`
-- 	- a player that stays out of the league should be `Stayed Retired`
-- */

-- -- SELECT * 
-- -- FROM players
-- -- WHERE 1=1
-- -- 	  AND player_name = 'LeBron James'

-- WITH player_tracking AS (
--   SELECT 
--     player_name
--   , draft_year
--   , scoring_class
--   , years_since_last_active
--   , current_season
--   , is_active
--   , CASE
--       WHEN CAST(draft_year AS INTEGER) = current_season THEN 'New'
-- 	  WHEN LAG(years_since_last_active) OVER(PARTITION BY player_name ORDER BY current_season) > 0 AND years_since_last_active > 0 THEN 'Stayed Retired'
-- 	  WHEN LAG(years_since_last_active) OVER(PARTITION BY player_name ORDER BY current_season) > 0 AND years_since_last_active = 0 THEN 'Returned From Retirement'
-- 	  WHEN years_since_last_active > 0 THEN 'Retired'
-- 	  WHEN is_active IS TRUE THEN 'Continued Playing'
-- 	  ELSE 'Stayed Retired'
-- 	END AS player_state
--   FROM players
--   WHERE 1=1
--   		-- AND player_name = 'Michael Jordan'
-- 		  -- AND player_name = 'Ben Simmons'
-- )

-- SELECT *
-- FROM player_tracking





/* 
2. Write a query that uses `GROUPING SETS` to do efficient aggregations of `game_details` data:

	- Aggregate this dataset along the following dimensions:
		- `player` and `team`
	       and answer questions like:
	 		- Who scored the most points playing for one team?
		 
		- `player` and `season`
		   and answer questions like:
		   - Who scored the most points in one season?
	
		- `team`
		   and answer questions like:
		   - Which team has won the most games?
*/

-- WITH deduped_teams AS (
  -- SELECT 
  --   team_id 
  -- , abbreviation
  -- , nickname
  -- FROM teams
  -- WHERE 1=1
  -- GROUP BY 1,2,3
-- )

--, game_data AS (
--   SELECT 
--     g.game_id 
--   , g.home_team_wins
--   , CASE
--       WHEN fgd.dim_is_playing_at_home IS TRUE AND g.home_team_wins = 1 THEN g.game_id
-- 	  WHEN fgd.dim_is_playing_at_home IS FALSE AND g.home_team_wins = 0 THEN g.game_id
-- 	  ELSE NULL
-- 	END AS dim_player_won_game_id
--   , t.abbreviation AS dim_team_abbrv
--   , t.nickname AS dim_team_nickname
--   , fgd.*
--   FROM fct_game_details fgd
--   LEFT JOIN deduped_teams t
--     ON fgd.dim_team_id = t.team_id
--   LEFT JOIN games g
--     ON 1=1
-- 	   AND fgd.dim_game_date = g.game_date_est 
-- 	   AND dim_team_id = CASE WHEN fgd.dim_is_playing_at_home IS TRUE THEN g.home_team_id ELSE g.visitor_team_id END
-- )

-- -- SELECT *
-- -- FROM game_data
-- -- WHERE 1=1
-- -- 	  AND game_id = 11600015

-- , grouped_game_data AS (
--   SELECT 
--     COALESCE(CAST(dim_season AS TEXT), '2016 - 2022') AS dim_season
--   , COALESCE(CAST(dim_player_id AS TEXT), 'All Players') AS dim_player_id
--   , COALESCE(dim_player_name, 'All Players') AS dim_player_name
--   , COALESCE(CAST(dim_team_id AS TEXT), 'All Teams') AS dim_team_id
--   , COALESCE(dim_team_abbrv, 'ALL_TEAMS') AS dim_team_abbrv
--   , SUM(m_pts) AS total_pts
--   , COUNT(DISTINCT dim_player_won_game_id) AS total_wins
--   FROM game_data
--   WHERE 1=1
--         -- AND game_id = 11600015
--   GROUP BY 
--     GROUPING SETS (
-- 	  ((dim_player_id, dim_player_name), dim_season, (dim_team_id, dim_team_abbrv))
-- 	, ((dim_player_id, dim_player_name), dim_season)
-- 	, ((dim_player_id, dim_player_name), (dim_team_id, dim_team_abbrv))
-- 	, ((dim_team_id, dim_team_abbrv), dim_season)
-- 	, (dim_team_id, dim_team_abbrv)
-- 	)
-- )

-- SELECT *
-- FROM grouped_game_data
-- WHERE 1=1
-- 	  AND dim_player_name = 'Aaron Gordon'
-- 	  -- AND dim_team_id = '1610612743'
-- ORDER BY 1,2,3,4



-- /* Who scored the most points playing for one team? */
-- SELECT 
--   dim_season
-- , dim_team_id
-- , dim_team_abbrv
-- , dim_player_id 
-- , dim_player_name
-- , total_pts
-- , total_wins
-- FROM grouped_game_data
-- WHERE 1=1
-- 	  AND dim_player_id <> 'All Players'
-- 	  AND total_pts IS NOT NULL
-- 	  -- AND dim_team_id = 'All Teams'
-- 	  AND dim_season = '2016 - 2022'
-- ORDER BY total_pts DESC
-- LIMIT 10



-- /* Who scored the most points is one season? */
-- SELECT 
--   dim_season
-- , dim_team_id
-- , dim_team_abbrv
-- , dim_player_id 
-- , dim_player_name
-- , total_pts
-- , total_wins
-- FROM grouped_game_data
-- WHERE 1=1
-- 	  AND dim_player_id <> 'All Players'
-- 	  AND total_pts IS NOT NULL
-- 	  AND dim_team_id = 'All Teams'
-- 	  AND dim_season <> '2016 - 2022'
-- 	  -- AND dim_player_name = 'Aaron Gordon'
-- ORDER BY total_pts DESC
-- LIMIT 10


-- /* Which team has won the most games? */
-- SELECT 
--   dim_season
-- , dim_team_id
-- , dim_team_abbrv
-- , dim_player_id 
-- , dim_player_name
-- , total_pts
-- , total_wins
-- FROM grouped_game_data
-- WHERE 1=1
-- 	  AND dim_player_id = 'All Players'
-- 	  -- AND total_pts IS NOT NULL
-- 	  AND dim_team_id <> 'All Teams'
-- 	  AND dim_season <> '2016 - 2022'
-- 	  -- AND dim_player_name = 'Aaron Gordon'
-- ORDER BY total_wins DESC
-- -- LIMIT 10









/* 
3. Write a query that uses window functions on `game_details` to find out the following things:

	- What is the most games a team has won in a 90 game stretch?
	- How many games in a row did LeBron James score over 10 points a game?
*/





-- /* What is the most games a team has won in a 90-game stretch? */
-- WITH deduped_teams AS (
--   SELECT 
--     team_id 
--   , abbreviation
--   , nickname
--   FROM teams
--   WHERE 1=1
--   GROUP BY 1,2,3
-- )

-- , games_data AS (
--   SELECT
--     game_id AS dim_game_id
--   , home_team_id AS dim_team_id
--   , game_date_est AS game_dt
--   , 'home' AS home_away
--   , home_team_wins AS w
--   FROM games
--   WHERE 1=1

--   UNION ALL 

--   SELECT 
--     game_id AS dim_game_id
--   , visitor_team_id AS dim_team_id
--   , game_date_est AS game_dt
--   , 'away' AS home_away
--   , CASE WHEN home_team_wins = 0 THEN 1 ELSE 0 END AS w
--   FROM games
--   WHERE 1=1
-- )

-- -- SELECT *
-- -- FROM games_data
-- -- WHERE 1=1
-- -- 	  AND dim_team_id = 1610612745
-- -- ORDER BY game_dt DESC


-- , wins_last_90_games AS (
--   SELECT 
--     dim_team_id 
--   -- , t.abbreviation AS dim_team_abbrv
--   , game_dt
--   , SUM(w) OVER(PARTITION BY dim_team_id ORDER BY game_dt ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) total_wins_last_90_games
--   FROM games_data
--   LEFT JOIN deduped_teams t 
--     ON games_data.dim_team_id = t.team_id
-- )


-- -- SELECT 
-- --   *
-- -- FROM wins_last_90_games
-- -- WHERE 1=1
-- -- 	  AND dim_team_id = 1610612745
-- -- ORDER BY game_dt DESC

-- SELECT *
-- FROM wins_last_90_games
-- ORDER BY total_wins_last_90_games DESC
-- LIMIT 10

/* How many games in a row did LeBron James score over 10 points a game? */
WITH player_data AS (
  SELECT 
    dim_game_date
  , dim_season
  , dim_team_id 
  , dim_player_id
  , dim_player_name
  , dim_is_playing_at_home
  , GREATEST(dim_did_not_play, dim_did_not_dress, dim_not_with_team) AS dim_dnp
  , m_minutes
  , m_pts
  , m_pts > 10 AS over_10_pts
  FROM fct_game_details
  WHERE 1=1
  		AND dim_player_name = 'LeBron James'
  ORDER BY 1
)

, player_pts_streak_identifier AS (
  SELECT
    dim_game_date
  , dim_season
  , dim_team_id
  , dim_player_id
  , dim_player_name
  , m_pts
  , over_10_pts
  , CASE 
      WHEN over_10_pts IS TRUE AND LAG(over_10_pts) OVER(PARTITION BY dim_player_id ORDER BY dim_game_date ASC) IS FALSE THEN 1
	  ELSE 0
    END AS over_10_pts_streak_id
  FROM player_data
  WHERE 1=1
  		AND dim_dnp IS FALSE
)

, player_pts_streak_groups AS (
  SELECT 
    *
  , SUM(over_10_pts_streak_id) OVER(PARTITION BY dim_player_id ORDER BY dim_game_date ASC) AS over_10_pts_streak_group
  FROM player_pts_streak_identifier
)

-- SELECT * FROM player_pts_streak_groups ORDER BY 1

SELECT 
  dim_player_id
, dim_player_name
, over_10_pts_streak_group
, MIN(dim_game_date) AS first_game_dt
, MAX(dim_game_date) AS last_game_dt
, COUNT(1) AS num_games
, MIN(m_pts) AS min_pts_in_streak
, MAX(m_pts) AS max_pts_in_streak
FROM player_pts_streak_groups
WHERE 1=1
   	  AND over_10_pts IS TRUE
GROUP BY 1,2,3
ORDER BY 1







