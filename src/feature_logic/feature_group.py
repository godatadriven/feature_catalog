from abc import ABCMeta, abstractmethod
from typing import List, Set

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf


class FeatureGroup(metaclass=ABCMeta):
    alias: str = ...
    source: str = ...
    keys: str = ...
    supported_keys: Set[str] = ...
    available_features: Set[str] = ...
    # depend_on: List["FeatureGroup"]

    def __init__(self, spark: SparkSession, features: List[str]):
        self.spark = spark
        self.features = features

    def apply(self, features: SparkDataFrame, key: str):
        if any(key not in features.columns for key in self.keys):
            raise KeyError()  # TODO: add logging and custom error

        if key not in self.supported_keys:
            raise ValueError()  # TODO: add logging and custom error

        feature_group = self._transform(self._read(), key=key)
        return features.join(feature_group.alias(self.alias), on=self.keys, how="left")

    @abstractmethod
    def _read(self) -> SparkDataFrame:
        """Read/load necessary source data"""
        pass

    @abstractmethod
    def _transform(self, data, key: str) -> SparkDataFrame:
        """Create features"""
        pass

    @property
    def selections(self) -> List[Column]:
        """Select the defined features"""
        return [
            sf.col(feature)
            for feature in self.features
            if feature in self.available_features
        ]
