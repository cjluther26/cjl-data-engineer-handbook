from pyspark.sql import SparkSession


query = """
  WITH previous_actors AS (
  SELECT 
    actorid
  , actor 
  , year 
  , quality_class
  , is_active
  , CASE 
      WHEN LAG(quality_class) OVER(PARTITION BY actorid ORDER BY year) <> quality_class THEN TRUE
	  WHEN LAG(quality_class) OVER(PARTITION BY actorid ORDER BY year) IS NULL THEN TRUE
	  WHEN LAG(is_active) OVER(PARTITION BY actorid ORDER BY year) <> is_active THEN TRUE
	  WHEN LAG(is_active) OVER(PARTITION BY actorid ORDER BY year) IS NULL THEN TRUE
	  ELSE FALSE
	END AS did_change
  FROM actors
  WHERE 1=1
  		-- AND actor = 'Leonardo DiCaprio'
)

-- SELECT * FROM previous_actors ORDER BY actorid, year

, streaks AS (
  SELECT 
    actorid
  , actor
  , year 
  , quality_class
  , is_active
  , SUM(CASE WHEN did_change IS TRUE THEN 1 ELSE 0 END) OVER(PARTITION BY actorid ORDER BY year) AS streak_group
  FROM previous_actors
)

-- SELECT * FROM streaks ORDER BY actorid, year

, aggregated_streaks AS (
  SELECT 
    actorid
  , actor
  , quality_class
  , is_active
  , streak_group
  , MIN(year) AS start_date
  , MAX(year) AS end_date
  FROM streaks
  GROUP BY 1,2,3,4,5
)

SELECT 
  actorid 
, actor
, quality_class
, is_active
, start_date
, end_date
FROM aggregated_streaks
ORDER BY actorid, start_date, end_date

"""

def do_actors_scd_transformation(spark, dataframe):
    """
    """

    dataframe.createOrReplaceTempView("actors")

    return spark.sql(query)

def main():
    """
    """

    # Build Spark session
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_history_scd") \
        .getOrCreate()
    
    # Do transformations
    output_df = do_actors_scd_transformation(spark, spark.table("actors"))

    # # Write new data to DataFrame
    # output_df.write.mode("overwrite").insertInto("actors_history_scd")

