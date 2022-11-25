import pytest

from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession
from pyspark_test import assert_pyspark_df_equal

from feature_logic.base_feature_group import BaseFeatureGroup
from feature_logic.utils import MissingColumnError


class GroupA(BaseFeatureGroup):

    supported_levels = {"id"}
    available_features = {"feature_3", "feature_4"}

    def _load_source_data(self) -> SparkDataFrame:
        return self.spark.createDataFrame([(1,), (2,), (3,), (4,)], schema="id integer")

    def _compute_feature_group(self, source_data: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        return (
            source_data
            .withColumn("feature_3", sf.lit("f3"))
            .withColumn("feature_4", sf.lit("f4"))
        )


@pytest.fixture
def features(spark):
    return spark.createDataFrame([
    dict(id=1, feature_1="a", feature_2="dummy"),
    dict(id=2, feature_1="a", feature_2="dummy"),
    dict(id=3, feature_1="b", feature_2="dummy"),
    dict(id=4, feature_1="c", feature_2=""),
])


@pytest.fixture
def features_extended_expected(features):
    return (
        features
        .withColumn("feature_3", sf.lit("f3"))
        .withColumn("feature_4", sf.lit("f4"))
    )


def test_extend(spark: SparkSession, features: SparkDataFrame, features_extended_expected: SparkDataFrame):
    features_extended = GroupA(spark=spark, features_of_interest=GroupA.available_features, aggregation_level="id").extend(features)
    assert_pyspark_df_equal(features_extended, features_extended_expected)


def test_extend__missing_column(spark: SparkSession):
    group_a = GroupA(spark=spark, features_of_interest=GroupA.available_features, aggregation_level="id")
    group_a.extend(spark.createDataFrame([], schema="id integer"))

    with pytest.raises(MissingColumnError):
        group_a.extend(spark.createDataFrame([], schema="not_id integer"))
    