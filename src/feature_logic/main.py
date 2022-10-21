from typing import List

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame

from feature_logic.feature_group import FeatureGroup


def create_features(
    data: SparkDataFrame, level: str, feature_groups: List[FeatureGroup]
):
    columns: List[Column] = []
    base_key = f"{level}_id"
    for feature_group in feature_groups:
        data = feature_group.apply(data, level=level)
        columns += feature_group.selections
        print("Columns:", columns)
    return data.select(base_key, *columns)
