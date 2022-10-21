from pyspark.sql import Column, DataFrame as SparkDataFrame, SparkSession

from typing import List
from feature_logic.feature_group import FeatureGroup


def create_features(spark: SparkSession, level: str, feature_groups: List[FeatureGroup]):
    data: SparkDataFrame = spark.createDataFrame([dict(party_id="a"), dict(party_id="b"), dict(party_id="c")]) 
    columns: List[Column] = []
    base_key = f"{level}_id"
    for feature_group in feature_groups:
        data = feature_group.apply(data, level=level)
        columns += feature_group.selections
    print(columns)
    return data.select(base_key, *columns)
