from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession


class FeatureGroup(metaclass=ABCMeta):
    alias: str = ...
    source: str = ...
    keys: str = ...
    supported_levels: Set[str] = set()
    available_features: Dict[str, Column] = {}
    # depend_on: List["FeatureGroup"]

    def __init__(self, spark: SparkSession, features: List[str]):
        self.spark = spark
        self.features = features

    def apply(self, data: SparkDataFrame, level: str):
        print(self.keys, data.columns)
        if any(key not in data.columns for key in self.keys):
            raise KeyError()
        print(self, "apply", data.columns)
        if level not in self.supported_levels:
            raise ValueError
        source = self._transform(self._read(), level=level)
        return data.join(source.alias(self.alias), on=self.keys, how="left")

    @abstractmethod
    def _read(self) -> SparkDataFrame:
        pass

    @staticmethod
    def _transform(data, level: str) -> SparkDataFrame:
        # here the actual transformation (creation of features) happens
        return data

    @property
    def selections(self):
        return [self.available_features[feature].select for feature in self.features]
