from pyspark.sql import DataFrame as SparkDataFrame, functions as sf

from feature_logic.feature_group import FeatureGroup
from feature_logic.feature import Feature


class LocationFeatures(FeatureGroup):
    
    alias = "location"
    source = "some/path/to/weather"
    keys = ["party_id"]
    supported_levels = {"party"}
    available_features = {
        "postcode": Feature("postcode", sf.col("location.postcode")),
        "is_foreign": Feature("is_foreign", sf.col("location.postcode") == sf.lit(""), False)
    }
    
    def _read(self) -> SparkDataFrame:
        return self.spark.createDataFrame([
            dict(party_id="a", postcode="1234AB"),
            dict(party_id="b", postcode="1234BC"),
            dict(party_id="c", postcode=""),
        ])
