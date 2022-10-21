from typing import List

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame

from feature_logic.feature_group import FeatureGroup


def create_features(data: SparkDataFrame, key: str, feature_groups: List[FeatureGroup]):
    columns: List[Column] = []

    for feature_group in feature_groups:
        data = feature_group.apply(data, key=key)
        columns += feature_group.selections

    return data.select(key, *columns)
