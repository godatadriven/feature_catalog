import pytest
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from feature_catalog.feature_groups.zone_likelyhood import ZoneLikelyhood


@pytest.fixture
def expected_zone_likelyhood_features(spark: SparkSession):
    return spark.createDataFrame(
        [
            dict(avatarId="a", total_count=2, darkshore_count=1, darkshore_likelyhood=0.5),
            dict(avatarId="b", total_count=1, darkshore_count=1, darkshore_likelyhood=1.0),
            dict(avatarId="c", total_count=1, darkshore_count=0, darkshore_likelyhood=0.0),
        ]
    )


@pytest.fixture
def zone_features(spark: SparkSession):
    return spark.createDataFrame(
        [
            dict(avatarId="a", total_count=2, darkshore_count=1),
            dict(avatarId="b", total_count=1, darkshore_count=1),
            dict(avatarId="c", total_count=1, darkshore_count=0),
        ]
    )


@pytest.mark.parametrize("aggregation_level", ["avatarId", "guild"])
def test_compute_feature_group(
    spark: SparkSession,
    zone_features: SparkDataFrame,
    aggregation_level: str,
    expected_zone_likelyhood_features: SparkDataFrame,
):
    if aggregation_level == "guild":
        zone_features = zone_features.withColumnRenamed("avatarId", "guild")
        expected_zone_likelyhood_features = expected_zone_likelyhood_features.withColumnRenamed("avatarId", "guild")

    zone_likelyhood = ZoneLikelyhood(
        features_of_interest=ZoneLikelyhood.available_features, aggregation_level=aggregation_level
    )
    zone_likelyhood_features = zone_likelyhood._compute_feature_group(
        spark=spark, intermediate_features=zone_features, aggregation_level=aggregation_level
    )
    assert_pyspark_df_equal(zone_likelyhood_features, expected_zone_likelyhood_features)
