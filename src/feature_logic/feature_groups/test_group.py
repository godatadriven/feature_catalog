from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as sf

from feature_logic.feature_group import FeatureGroup


class LocationFeatures(FeatureGroup):

    alias = "location"
    source = "data/location_data.parquet"
    keys = ["id"]
    supported_keys = {"id"}
    available_features = {"postcode", "is_foreign"}

    def _read(self) -> SparkDataFrame:
        return self.spark.read.parquet(self.source)

    def _transform(self, data, key: str) -> SparkDataFrame:
        return data.withColumn("is_foreign", sf.col("postcode") == sf.lit(""))
