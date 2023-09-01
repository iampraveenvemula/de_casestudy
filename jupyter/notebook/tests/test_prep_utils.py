
import findspark
findspark.init()

import pytest
from pyspark.sql import SparkSession, DataFrame
from typing import List


import sys

# Add the directory containing prep_utils.py to sys.path
sys.path.append("utils/")

from prep_utils import *


def test_standardize_annual_salary(spark_session):
    """
    Test the standardize_annual_salary function.

    This test case checks if the function correctly standardizes salary range columns to annual salary.

    Steps:
    1. Define a test DataFrame with sample data.
    2. Define the expected result after applying the function.
    3. Call the function with the test DataFrame.
    4. Check if the resulting DataFrame matches the expected DataFrame.
    """
    # Define a test DataFrame with sample data
    test_data = [
        (1, "Daily", 100, 200),
        (2, "Hourly", 15, 30),
        (3, "Annually", 50000, 75000),
    ]
    columns = ["ID", "Salary Frequency", "Salary Range From", "Salary Range To"]
    test_df = spark_session.createDataFrame(test_data, columns)

    # Define the expected result after applying the function
    expected_data = [
        (1, "Daily", 100, 200, 26000, 52000),
        (2, "Hourly", 15, 30, 31200, 62400),
        (3, "Annually", 50000, 75000, 50000, 75000),
    ]
    expected_columns = [
        "ID",
        "Salary Frequency",
        "Salary Range From",
        "Salary Range To",
        "AnnualSalaryFrom",
        "AnnualSalaryTo",
    ]
    expected_df = spark_session.createDataFrame(expected_data, expected_columns)

    # Call the function with the test DataFrame
    result_df = standardize_annual_salary(test_df)

    # Check if the resulting DataFrame matches the expected DataFrame
    assert result_df.collect() == expected_df.collect()