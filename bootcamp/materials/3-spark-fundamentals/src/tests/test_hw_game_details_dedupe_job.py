from chispa.dataframe_comparer import *
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType, BooleanType, DoubleType, IntegerType
from pyspark.sql.functions import col
from ..jobs.hw_game_details_dedupe_job import do_game_details_dedupe
from collections import namedtuple

GameDetailsTuple = namedtuple("GameDetailsTuple", "game_id team_id team_abbreviation team_city player_id player_name start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk TO pf pts plus_minus")
GameTuple = namedtuple("GameTuple", "game_date_est game_id game_status_text home_team_id visitor_team_id season")
GameDetailsDedupedTuple = namedtuple("GameDetailsDedupedTuple", "dim_game_id dim_game_date dim_season dim_player_id dim_player_name dim_start_position dim_is_playing_at_home dim_did_not_play dim_did_not_dress dim_not_with_team m_minutes m_fgm m_fga m_fg3m m_fg3a m_ftm m_fta m_oreb m_dreb m_reb m_ast m_stl m_blk m_turnovers m_pf m_pts m_plus_minus r")

def test_dedupe_generation(spark):
    """
    """

    # Create games source data
    game_source_data = [
        GameTuple("2022-06-16", 42100406, "Final", 1610612738, 1610612744, 2021),

    ]

    # Create game_details source data
    game_details_source_data = [
        GameDetailsTuple(42100406, 1610612744, "GSW", "Golden State", 201939, "Stephen Curry", "G", None, "39:55", 12, 21, 0.571, 6, 11, 0.545, 4, 4, 1.0, 0, 7, 7, 7, 2, 1, 2, 4, 34, 8),
        GameDetailsTuple(42100406, 1610612744, "GSW", "Golden State", 201939, "Stephen Curry FAKE", "G", None, "39:55", 12, 21, 0.571, 6, 11, 0.545, 4, 4, 1.0, 0, 7, 7, 7, 2, 1, 2, 4, 34, 8)
    ]

    # Define schema explicitly
    game_details_schema = StructType([
        StructField("game_id", LongType(), True),
        StructField("team_id", LongType(), True),
        StructField("team_abbreviation", StringType(), True),
        StructField("team_city", StringType(), True),
        StructField("player_id", LongType(), True),
        StructField("player_name", StringType(), True),
        StructField("start_position", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("min", StringType(), True),
        StructField("fgm", LongType(), True),
        StructField("fga", LongType(), True),
        StructField("fg_pct", FloatType(), True),
        StructField("fg3m", LongType(), True),
        StructField("fg3a", LongType(), True),
        StructField("fg3_pct", FloatType(), True),
        StructField("ftm", LongType(), True),
        StructField("fta", LongType(), True),
        StructField("ft_pct", FloatType(), True),
        StructField("oreb", LongType(), True),
        StructField("dreb", LongType(), True),
        StructField("reb", LongType(), True),
        StructField("ast", LongType(), True),
        StructField("stl", LongType(), True),
        StructField("blk", LongType(), True),
        StructField("TO", LongType(), True),
        StructField("pf", LongType(), True),
        StructField("pts", LongType(), True),
        StructField("plus_minus", LongType(), True),
    ])

    # Define output schema explicitly
    output_data_schema = StructType([
        StructField('dim_game_id', LongType(), True),
        StructField('dim_game_date', StringType(), True),
        StructField('dim_season', LongType(), True),
        StructField("dim_player_id", LongType(), True),
        StructField("dim_player_name", StringType(), True),
        StructField("dim_start_position", StringType(), True),
        StructField('dim_is_playing_at_home', BooleanType(), True),
        StructField('dim_did_not_play', BooleanType(), True),
        StructField('dim_did_not_dress', BooleanType(), True),
        StructField('dim_not_with_team', BooleanType(), True),
        StructField("m_minutes", DoubleType(), True),
        StructField("m_fgm", LongType(), True),
        StructField("m_fga", LongType(), True),
        StructField("m_fg3m", LongType(), True),
        StructField("m_fg3a", LongType(), True),
        StructField("m_ftm", LongType(), True),
        StructField("m_fta", LongType(), True),
        StructField("m_oreb", LongType(), True),
        StructField("m_dreb", LongType(), True),
        StructField("m_reb", LongType(), True),
        StructField("m_ast", LongType(), True),
        StructField("m_stl", LongType(), True),
        StructField("m_blk", LongType(), True),
        StructField("m_turnovers", LongType(), True),
        StructField("m_pf", LongType(), True),
        StructField("m_pts", LongType(), True),
        StructField("m_plus_minus", LongType(), True),
        StructField('r', IntegerType(), True)  
    ])

    # Create DataFrame of source data
    game_source_df = spark.createDataFrame(game_source_data)
    game_details_source_df = spark.createDataFrame(game_details_source_data, schema = game_details_schema)

    # Run transformation
    actual_df = do_game_details_dedupe(spark, game_source_df, game_details_source_df)

    # Cast actual_df columns to match the schema
    for field in output_data_schema.fields:
        actual_df = actual_df.withColumn(field.name, col(field.name).cast(field.dataType))

    # Create expected data
    expected_data = [
        GameDetailsDedupedTuple(42100406, "2022-06-16", 2021, 201939, "Stephen Curry", "G", False, False, False, False, 39.916666666666664, 12, 21, 6, 11, 4, 4, 0, 7, 7, 7, 2, 1, 2, 4, 34, 8, 1),
    ]

    # Create DataFrame of expected data
    expected_df = spark.createDataFrame(expected_data, schema = output_data_schema)

    # Debugging step: print schemas
    print("Actual Schema:")
    actual_df.printSchema()
    print("Expected Schema:")
    expected_df.printSchema()
    
    # Assert equality (source_transformed == expected)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
