from pyspark.sql import Column
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from feature_catalog.base_feature_group import BaseFeatureGroup
from feature_catalog.utils import UnsupportedDependencies


def compute_features(
    spark: SparkSession, scope: SparkDataFrame, feature_groups: list[BaseFeatureGroup]
) -> SparkDataFrame:
    """Entrypoint to compute a feature dataframe.

    Args:
        scope: single column (also the aggregation_level) specifying the population for which to compute features
        feature_groups: list of feature groups to compute

    Returns:
        Dataframe with features of interest
    """

    assert len(scope.columns) == 1, "Error: scope must be a single column dataframe"

    features = scope
    columns_of_interest: list[Column] = []
    groups_to_compute_in_order = order_feature_groups(extend_feature_groups(feature_groups))
    for feature_group in groups_to_compute_in_order:
        features = feature_group.extend(spark=spark, features=features)
        columns_of_interest += feature_group.columns_of_interest

    # NOTE: by selecting columns only at the end we allow the re-use of (intermediate) columns,
    # while still allowing spark to only compute what is necessary.
    return features.select(*scope.columns, *columns_of_interest)


def order_feature_groups(feature_groups: list[BaseFeatureGroup]) -> list[BaseFeatureGroup]:
    """Order the feature groups such that the ones that depend on others are created afterwards.

    Ties are dealt with by ordering alphabetically.
    E.g. both group A and C depend on B, then the order should be: B, A, C

    NOTE: cycles are not allowed.
    """

    def names(fgs) -> set[str]:
        return set([fg.__class__.__name__ for fg in fgs])

    ordered_feature_groups: list[BaseFeatureGroup] = []
    feature_groups = sorted(feature_groups, key=lambda x: x.__class__.__name__)
    while len(feature_groups) > 0:
        deleted = False
        for i, fg in enumerate(feature_groups):
            if names(fg.depends_on).issubset(names(ordered_feature_groups)):
                deleted = True
                del feature_groups[i]
                ordered_feature_groups.append(fg)
                break
        if not deleted:
            raise UnsupportedDependencies(
                "Can't find a correct order taking all dependencies into account. "
                "Possibly caused by a cyclic dependency."
            )
    return ordered_feature_groups


def extend_feature_groups(feature_groups: list[BaseFeatureGroup]) -> list[BaseFeatureGroup]:
    """Also create feature groups on which there is a dependency.

    It can be that you want to compute features from a group that depends on another group
    but you are not interested to also return those features.
    However, they still need to be created as intermediate result.

    E.g. feature_groups=[A] and group A depend on B, then we return {A, B}

    # TODO: make sure that feature groups that depend on each other have the same arguments,
    # if not throw error.
    """
    extended_feature_groups: dict[str, BaseFeatureGroup] = {}
    for feature_group in set(feature_groups):
        extended_feature_groups[feature_group.__class__.__name__] = feature_group
        dependencies = extend_feature_groups(feature_group.depends_on)
        for dependency in dependencies:
            extended_feature_groups[dependency.__class__.__name__] = dependency
    return list(extended_feature_groups.values())
