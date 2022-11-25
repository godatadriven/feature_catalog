from typing import List

from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame

from feature_logic.base_feature_group import BaseFeatureGroup


def compute_features(scope: SparkDataFrame, feature_groups: List[BaseFeatureGroup]) -> SparkDataFrame:
    """Entrypoint to compute a feature dataframe.

    Args:
        scope: single column (also the aggregation_level) specifying the population for which to compute features
        feature_groups: list of feature groups to compute

    Returns:
        Dataframe with features of interest
    """

    assert len(scope.columns) == 1, "Error: scope must be a single column dataframe"

    # TODO: make sure to order feature groups based on dependencies on each other
    features = scope
    columns_of_interest: List[Column] = []
    for feature_group in feature_groups:
        features = feature_group.add(features)
        columns_of_interest += feature_group.columns_of_interest

    # NOTE: by selecting columns only at the end we allow the re-use of (intermediate) columns,
    # while still allowing spark to only compute what is necessary.
    return features.select(*scope.columns, *columns_of_interest)
