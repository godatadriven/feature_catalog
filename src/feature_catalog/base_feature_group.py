from abc import ABCMeta, abstractmethod, abstractproperty

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

from feature_catalog.utils import MissingColumnError, UnsupportedAggregationLevel, UnsupportedFeatureName, get_logger

LOGGER = get_logger()


class BaseFeatureGroup(metaclass=ABCMeta):
    """Class to extend when creating a new feature group.

    Args:
        features_of_interest: list of features to compute
        aggregation_level: column on which the aggregation is done for this feature group
    """

    @abstractproperty
    def supported_levels(self) -> set[str]:
        """Set of aggregation levels that is supported by this feature group"""
        pass

    @abstractproperty
    def available_features(self) -> set[str]:
        """Set of features that can be computed in this feature group"""
        pass

    @property
    def depends_on(self) -> list["BaseFeatureGroup"]:
        """To indicate on which other feature groups this group depends

        Depending on another feature group means that you use columns/features
        from this group to create new columns/features.
        """
        return self._depends_on

    def __init__(self, features_of_interest: list[str], aggregation_level: str):
        self.features_of_interest = features_of_interest
        self.aggregation_level = aggregation_level
        self._depends_on: list["BaseFeatureGroup"] = []

        self.alias = self.__class__.__name__
        if self.aggregation_level not in self.supported_levels:
            error_message = "Error: aggregation level not supported"
            LOGGER.error(error_message)
            raise UnsupportedAggregationLevel(error_message)

        for feature_name in features_of_interest:
            if feature_name not in self.available_features:
                error_message = f"Error: {feature_name} not available in this group."
                LOGGER.error(error_message)
                raise UnsupportedFeatureName(error_message)

    def extend(self, spark: SparkSession, features: SparkDataFrame) -> SparkDataFrame:
        """Extend already computed features by adding features from this group

        Args:
            spark: spark session
            features: already computed features

        Returns:
            Extended features datafreame
        """
        if self.aggregation_level not in features.columns:
            error_message = "Error: aggregation column not available in intermediate feature dataframe"
            LOGGER.error(error_message)
            raise MissingColumnError(error_message)

        feature_group = self._compute_feature_group(
            spark=spark, intermediate_features=features, aggregation_level=self.aggregation_level
        )
        return features.join(
            feature_group.alias(self.alias + "-" + self.aggregation_level), on=self.aggregation_level, how="left"
        )

    @abstractmethod
    def _compute_feature_group(
        self, spark: SparkSession, intermediate_features: SparkDataFrame, aggregation_level: str
    ) -> SparkDataFrame:
        """Compute the features from this feature group

        Args:
            spark: spark session
            intermediate_features: already computed features from which you can re-use columns
            aggregation_level: column on which the aggregation is done for this feature group
                and also column on which the result is joined to the intermediate features
        """
        pass

    @property
    def columns_of_interest(self) -> list[Column]:
        """List the columns of interest from this group that need to be selected at the end of the query"""
        return [
            sf.col(self.alias + "-" + self.aggregation_level + "." + feature)
            for feature in self.features_of_interest
            if feature in self.available_features
        ]
