from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as sf

from feature_logic.base_feature_group import BaseFeatureGroup
from feature_logic.feature_groups.zone import Zone


class ZoneLikelyhood(BaseFeatureGroup):

    supported_levels = {"avatarId", "guild"}
    available_features = {"darkshore_likelyhood"}
    depends_on = Zone  # TODO

    def _compute_feature_group(self, intermediate_features: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        return intermediate_features.withColumn(
            "darkshore_likelyhood", sf.col("darkshore_count") / sf.col("total_count")
        )
