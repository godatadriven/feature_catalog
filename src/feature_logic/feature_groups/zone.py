from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as sf

from feature_logic.base_feature_group import BaseFeatureGroup


class Zone(BaseFeatureGroup):

    supported_levels = {"avatarId", "guild"}
    available_features = {"darkshore_likelyhood", "darkshore_count", "total_count"}

    def _load_source_data(self) -> SparkDataFrame:
        return self.spark.read.parquet("data/wow.parquet")

    def _compute_feature_group(self, source_data: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        return (
            source_data
            .withColumn("darkshore_flag", sf.when(sf.col("zone") == " Darkshore", 1).otherwise(0))
            .groupby(aggregation_level)
            .agg(
                sf.count("*").alias("total_count"),
                sf.sum("darkshore_flag").alias("darkshore_count")
            )
            .withColumn("darkshore_likelyhood", sf.col("darkshore_count") / sf.col("total_count"))
        )
