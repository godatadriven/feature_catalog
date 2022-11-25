import pytest

from pyspark.sql import SparkSession, DataFrame as SparkDataFrame, functions as sf
from pyspark_test import assert_pyspark_df_equal

from feature_logic.feature_groups.zone import Zone


@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture
def expected_zone_features(spark):
    return spark.createDataFrame([
        dict(avatarId="a", total_count=2, darkshore_count=1, darkshore_likelyhood=.5), 
        dict(avatarId="b", total_count=1, darkshore_count=1, darkshore_likelyhood=1.), 
        dict(avatarId="c", total_count=1, darkshore_count=0, darkshore_likelyhood=0.)
    ])

@pytest.fixture
def zone_source_data(spark):
    return spark.createDataFrame([
    dict(avatarId="a", zone=" Darkshore"),
    dict(avatarId="a", zone=" Lighthaven"),
    dict(avatarId="b", zone=" Darkshore"),
    dict(avatarId="c", zone=" Lighthaven"),
])

@pytest.mark.parametrize("aggregation_level", ["avatarId", "guild"])
def test_compute_feature_group(
    zone_source_data: SparkDataFrame, 
    aggregation_level: str, 
    expected_zone_features: SparkDataFrame
    ):
    if aggregation_level == "guild":
        zone_source_data = zone_source_data.withColumn("guild", sf.col("avatarId"))
        expected_zone_features = expected_zone_features.withColumnRenamed("avatarId", "guild")

    zone = Zone(spark=spark, features_of_interest=Zone.available_features, aggregation_level=aggregation_level)
    zone_features = zone._compute_feature_group(zone_source_data, aggregation_level=aggregation_level)
    assert_pyspark_df_equal(zone_features, expected_zone_features)
