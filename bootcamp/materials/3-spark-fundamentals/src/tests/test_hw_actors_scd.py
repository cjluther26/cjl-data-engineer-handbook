from chispa.dataframe_comparer import *
from ..jobs.hw_actors_scd_job import do_actors_scd_transformation
from collections import namedtuple

ActorTuple = namedtuple("ActorTuple", "actorid actor year quality_class is_active")
ActorSCDTuple = namedtuple("ActorSCDTuple", "actorid actor quality_class is_active start_date end_date")

def test_scd_generation(spark):
    """
    """

    # Create source data
    source_data = [
        ActorTuple("nm0000375", "Robert Downey Jr.", 2008, "good", True),
        ActorTuple("nm0000375", "Robert Downey Jr.", 2009, "good", True),
        ActorTuple("nm0000375", "Robert Downey Jr.", 2010, "average", True),
        ActorTuple("nm0000138", "Leonardo DiCaprio", 2019, "good", True),
        ActorTuple("nm0000138", "Leonardo DiCaprio", 2020, "bad", False),
        ActorTuple("nm0000138", "Leonardo DiCaprio", 2021, "bad", False),
    ]

    # Create DataFrame of source data
    source_df = spark.createDataFrame(source_data)

    # Run transformation
    actual_df = do_actors_scd_transformation(spark, source_df)

    # Create expected data
    expected_data = [
        ActorSCDTuple("nm0000138", "Leonardo DiCaprio", "good", True, 2019, 2019),
        ActorSCDTuple("nm0000138", "Leonardo DiCaprio", "bad", False, 2020, 2021),
        ActorSCDTuple("nm0000375", "Robert Downey Jr.", "good", True, 2008, 2009),
        ActorSCDTuple("nm0000375", "Robert Downey Jr.", "average", True, 2010, 2010),
    ]

    # Create DataFrame of expected data
    expected_df = spark.createDataFrame(expected_data)

    # Assert equality (source_transformed == expected)
    assert_df_equality(actual_df, expected_df)
