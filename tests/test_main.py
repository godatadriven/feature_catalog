from feature_catalog.main import extend_feature_groups, order_feature_groups
from feature_catalog.base_feature_group import BaseFeatureGroup
from pyspark.sql import DataFrame as SparkDataFrame


class A(BaseFeatureGroup):

    supported_levels = {"id"}
    available_features = {"A1", "A2"}

    def _compute_feature_group(self, intermediate_features: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        return intermediate_features


class B(BaseFeatureGroup):

    supported_levels = {"id"}
    available_features = {"B1", "B2"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._depends_on = [A(
            spark=self.spark,
            features_of_interest=[],
            aggregation_level=self.aggregation_level
        )]
        
    def _compute_feature_group(self, intermediate_features: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        return intermediate_features


class C(BaseFeatureGroup):

    supported_levels = {"id"}
    available_features = {"C1", "C2"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._depends_on = [A(
            spark=self.spark,
            features_of_interest=[],
            aggregation_level=self.aggregation_level
        )]
        
    def _compute_feature_group(self, intermediate_features: SparkDataFrame, aggregation_level: str) -> SparkDataFrame:
        return intermediate_features


def test_extend_feature_groups__adding_A(spark):
    b = B(spark, features_of_interest=[], aggregation_level="id")
    
    extended_feature_groups = extend_feature_groups([b])
    assert len(extended_feature_groups) == 2
    assert {fg.__class__.__name__ for fg in extended_feature_groups} == {A.__name__, b.__class__.__name__}


def test_extend_feature_groups__add_nothing(spark):
    a = A(spark, features_of_interest=[], aggregation_level="id")
    b = B(spark, features_of_interest=[], aggregation_level="id")
    c = C(spark, features_of_interest=[], aggregation_level="id")

    extended_feature_groups = extend_feature_groups([a, b, c])
    assert len(extended_feature_groups) == 3
    assert {fg.__class__.__name__ for fg in extended_feature_groups} == {A.__name__, B.__name__, C.__name__}


def test_order_feature_groups(spark):
    a = A(spark, features_of_interest=[], aggregation_level="id")
    b = B(spark, features_of_interest=[], aggregation_level="id")
    c = C(spark, features_of_interest=[], aggregation_level="id")

    ordered_feature_groups = order_feature_groups([a, c, b])
    assert len(ordered_feature_groups) == 3
    assert ordered_feature_groups == [a, b, c]

    ordered_feature_groups = order_feature_groups([b, a, c])
    assert len(ordered_feature_groups) == 3
    assert ordered_feature_groups == [a, b, c]
