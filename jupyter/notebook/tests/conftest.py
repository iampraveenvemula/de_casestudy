import findspark
findspark.init()

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    # Create a SparkSession for testing
    spark = SparkSession.builder.appName("DataPrepTestCases").getOrCreate()
    yield spark  # Yield the SparkSession object to the tests
    spark.stop()  # Clean up the SparkSession after all tests are done