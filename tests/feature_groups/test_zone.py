import pytest
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark_test import assert_pyspark_df_equal

from feature_logic.feature_groups.zone import Zone


@pytest.fixture
def expected_zone_features(spark: SparkSession):
    return spark.createDataFrame(
        [
            dict(avatarId="a", total_count=2, darkshore_count=1),
            dict(avatarId="b", total_count=1, darkshore_count=1),
            dict(avatarId="c", total_count=1, darkshore_count=0),
        ]
    )


@pytest.fixture
def zone_source_data(spark: SparkSession):
    return spark.createDataFrame(
        [
            dict(avatarId="a", zone=" Darkshore"),
            dict(avatarId="a", zone=" Lighthaven"),
            dict(avatarId="b", zone=" Darkshore"),
            dict(avatarId="c", zone=" Lighthaven"),
        ]
    )


@pytest.mark.parametrize("aggregation_level", ["avatarId", "guild"])
def test_compute_feature_group(
    spark: SparkSession,
    zone_source_data: SparkDataFrame,
    aggregation_level: str,
    expected_zone_features: SparkDataFrame,
):
    if aggregation_level == "guild":
        zone_source_data = zone_source_data.withColumn("guild", sf.col("avatarId"))
        expected_zone_features = expected_zone_features.withColumnRenamed("avatarId", "guild")

    intermediate_features = zone_source_data.select(aggregation_level)
    zone = Zone(spark=spark, features_of_interest=Zone.available_features, aggregation_level=aggregation_level)
    zone._load_source_data = lambda: zone_source_data  # mock the source data
    zone_features = zone._compute_feature_group(
        intermediate_features=intermediate_features, aggregation_level=aggregation_level
    )
    assert_pyspark_df_equal(zone_features, expected_zone_features)
