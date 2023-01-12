from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as sf

from feature_catalog.base_feature_group import BaseFeatureGroup
from feature_catalog.feature_groups.zone import Zone


class ZoneLikelyhood(BaseFeatureGroup):

    supported_levels = {"avatarId", "guild"}
    available_features = {"darkshore_likelyhood"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._depends_on = [Zone(spark=self.spark, features_of_interest=[], aggregation_level=self.aggregation_level)]

    def _compute_feature_group(self, intermediate_features: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        return intermediate_features.withColumn(
            "darkshore_likelyhood", sf.col("darkshore_count") / sf.col("total_count")
        )
