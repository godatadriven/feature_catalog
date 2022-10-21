from abc import ABCMeta, abstractmethod
from typing import Dict, List, Set

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf


class FeatureGroup(metaclass=ABCMeta):
    alias: str = ...
    source: str = ...
    keys: str = ...
    supported_levels: Set[str] = set()
    available_features: Set[str] = set()
    # depend_on: List["FeatureGroup"]

    def __init__(self, spark: SparkSession, features: List[str]):
        self.spark = spark
        self.features = features

    def apply(self, data: SparkDataFrame, level: str):
        print("keys and columns: ", self.keys, data.columns)
        if any(key not in data.columns for key in self.keys):
            raise KeyError()
        print(self, "apply", data.columns)
        if level not in self.supported_levels:
            raise ValueError
        print("level", level)
        input_data = self._read()
        print("input_data", input_data)
        source = self._transform(input_data, level=level)
        return data.join(source.alias(self.alias), on=self.keys, how="left")

    @abstractmethod
    def _read(self) -> SparkDataFrame:
        pass

    @abstractmethod
    def _transform(self, data, level: str) -> SparkDataFrame:
        pass

    @property
    def selections(self) -> List[Column]:
        return [
            sf.col(feature)
            for feature in self.features
            if feature in self.available_features
        ]
