from abc import ABCMeta, abstractmethod, abstractproperty
from typing import List, Set

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

from feature_logic.utils import MissingColumnError, UnsupportedAggregationLevel, get_logger

LOGGER = get_logger()


class BaseFeatureGroup(metaclass=ABCMeta):
    """Class to extend when creating a new feature group.

    Args:
        spark: spark session
        features_of_interest: list of features to compute
        aggregation_level: column on which the aggregation is done for this feature group
    """

    @abstractproperty
    def supported_levels(self) -> Set[str]:
        """Set of aggregation levels that is supported by this feature group"""
        pass

    @abstractproperty
    def available_features(self) -> Set[str]:
        """Set of features that can be computed in this feature group"""
        pass

    # TODO: add depends_on functionality
    # @abstractproperty
    # def depend_on(self) -> List["BaseFeatureGroup"]:
    #     """"""
    #     pass

    def __init__(self, spark: SparkSession, features_of_interest: List[str], aggregation_level: str):
        self.spark = spark
        self.features_of_interest = features_of_interest
        self.aggregation_level = aggregation_level

        self.alias = self.__class__.__name__
        if self.aggregation_level not in self.supported_levels:
            error_message = "Error: aggregation level not supported"
            LOGGER.error(error_message)
            raise UnsupportedAggregationLevel(error_message)

    def extend(self, features: SparkDataFrame) -> SparkDataFrame:
        """Extend already computed features by adding features from this group

        Args:
            features: already computed features

        Returns:
            Extended features datafreame
        """
        if self.aggregation_level not in features.columns:
            error_message = "Error: aggregation column not available in intermediate feature dataframe"
            LOGGER.error(error_message)
            raise MissingColumnError(error_message)

        feature_group = self._compute_feature_group(self._load_source_data(), aggregation_level=self.aggregation_level)
        return features.join(
            feature_group.alias(self.alias + "-" + self.aggregation_level), on=self.aggregation_level, how="left"
        )

    @abstractmethod
    def _load_source_data(self) -> SparkDataFrame:
        """Read/load necessary source data"""
        pass

    @abstractmethod
    def _compute_feature_group(self, source_data: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        """Compute the features from this feature group"""
        pass

    @property
    def columns_of_interest(self) -> List[Column]:
        """List the columns of interest from this group that need to be selected at the end of the query"""
        return [
            sf.col(self.alias + "-" + self.aggregation_level + "." + feature)
            for feature in self.features_of_interest
            if feature in self.available_features
        ]
