from pyspark.sql import SparkSession
import pytest
from src.feature_logic.feature_groups.test_group import LocationFeatures
from zoneinfo import available_timezones

@pytest.fixture
def spark():
    return SparkSession.builder.getOrCreate()

@pytest.fixture
def data(spark):
    return spark.createDataFrame([dict(party_id="a"), dict(party_id="b"), dict(party_id="c")])

@pytest.fixture
def location_data():
    return spark.createDataFrame([
    dict(party_id="a", postcode="1234AB"),
    dict(party_id="b", postcode="1234BC"),
    dict(party_id="c", postcode=""),
])

def check_supported_features(data, location_data):
    location_features = LocationFeatures(spark=spark, features=["postcode"])
    columns = location_features._transform(location_data, level="party").columns
    assert LocationFeatures.available_features.intersection(columns) == LocationFeatures.available_features