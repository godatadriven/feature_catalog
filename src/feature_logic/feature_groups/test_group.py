from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import functions as sf

from feature_logic.feature import Feature
from feature_logic.feature_group import FeatureGroup


class LocationFeatures(FeatureGroup):

    alias = "location"
    source = "data/location_data.parquet"
    keys = ["party_id"]
    supported_levels = {"party"}
    available_features = {
        "postcode": Feature("postcode", sf.col("location.postcode")),
        "is_foreign": Feature(
            "is_foreign", sf.col("location.postcode") == sf.lit(""), False
        ),
    }

    def _read(self) -> SparkDataFrame:
        return self.spark.read.parquet(self.source)
