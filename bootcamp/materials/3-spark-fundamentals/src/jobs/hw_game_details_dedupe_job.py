from pyspark.sql import SparkSession

query = """

WITH game_details_deduped AS (
  SELECT 
    gd.game_id AS dim_game_id
  , g.game_date_est AS dim_game_date
  , g.season AS dim_season
  , player_id AS dim_player_id
  , player_name AS dim_player_name
  , start_position AS dim_start_position
  , gd.team_id = g.home_team_id AS dim_is_playing_at_home
  , COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play
  , COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress
  , COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team
  , CAST(SPLIT_PART(min, ':', 1) AS REAL) + (CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60) AS m_minutes
  , fgm AS m_fgm
  , fga AS m_fga
  , fg3m AS m_fg3m
  , fg3a AS m_fg3a
  , ftm AS m_ftm
  , fta AS m_fta
  , oreb AS m_oreb
  , dreb AS m_dreb
  , reb AS m_reb
  , ast AS m_ast
  , stl AS m_stl
  , blk AS m_blk
  , TO AS m_turnovers
  , pf AS m_pf
  , pts AS m_pts
  , plus_minus AS m_plus_minus
  , ROW_NUMBER() OVER(PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est, gd.player_name ASC) AS r
  FROM game_details gd
  LEFT JOIN games g 
    ON gd.game_id = g.game_id
  WHERE 1=1
)

, final_data AS (
  SELECT *
  FROM game_details_deduped
  WHERE 1=1
	    AND r = 1
)

SELECT * FROM final_data
;
"""

def do_game_details_dedupe(spark, games_df, game_details_df):
    """
    
    """

    games_df.createOrReplaceTempView("games")
    game_details_df.createOrReplaceTempView("game_details")

    return spark.sql(query)



def main():
    """
    
    """

    spark = SparkSession.builder \
        .master("local") \
        .appName("game_details_dedupe") \
        .getOrCreate()
    
    output_df = do_game_details_dedupe(spark, spark.table("game_details"))
    output_df.write.mode("overwrite").insertInto("game_details_dedupe")

