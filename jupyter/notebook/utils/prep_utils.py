#!/usr/bin/env python
# coding: utf-8


## contains cleaning and feature engineering modules


import findspark

findspark.init()



import re

from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    lower,
    regexp_replace,
    lower,
    split,
    udf,
    avg,
    explode,
    count,
    when,
)

from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import pyspark.sql.functions as f


from pyspark.ml.feature import NGram, StopWordsRemover
from pyspark.sql.types import ArrayType, StringType
from typing import List, Optional

import nltk
from nltk.stem import WordNetLemmatizer

# Initialize NLTK lemmatizer
nltk.download("wordnet")
lemmatizer = WordNetLemmatizer()


# In[6]:


def standardize_annual_salary(df: DataFrame) -> DataFrame:
    """
    Standardize salary range columns to annual salary.

    Parameters:
        df (DataFrame): The input DataFrame containing salary information.

    Returns:
        DataFrame: The DataFrame with standardized annual salary columns.
    """
    workhours_per_day = 8
    workdays_per_week = 5
    workweeks_per_year = 52

    workdays_per_year = workdays_per_week * workweeks_per_year
    workhours_per_year = workhours_per_day * workdays_per_year

    df = df.withColumn(
        "AnnualSalaryFrom",
        when(
            col("Salary Frequency") == "Daily",
            col("Salary Range From") * workdays_per_year,
        )
        .when(
            col("Salary Frequency") == "Hourly",
            col("Salary Range From") * workhours_per_year,
        )
        .otherwise(col("Salary Range From")),
    )

    df = df.withColumn(
        "AnnualSalaryTo",
        when(
            col("Salary Frequency") == "Daily",
            col("Salary Range To") * workdays_per_year,
        )
        .when(
            col("Salary Frequency") == "Hourly",
            col("Salary Range To") * workhours_per_year,
        )
        .otherwise(col("Salary Range To")),
    )

    return df


# In[1]:


# Define keywords for degrees
keywords = [
    "master",
    "phd",
    "pg",
    "post graduate",
    "baccalaureate",
    "diploma",
    "high school",
]

# Define the priority order for degrees
degree_priority = [
    "phd",
    "master",
    "post graduate",
    "pg",
    "baccalaureate",
    "diploma",
    "high school",
]


def build_regex(keywords: List[str]) -> str:
    """
    Build a regular expression pattern to match keywords in a line.

    Parameters:
        keywords (list of str): List of keywords to build the regex pattern for.

    Returns:
        str: The regex pattern to match the keywords.
    """
    res = "("
    for key in keywords:
        res += "\\b" + key + "\\b|"
    res = res[0 : len(res) - 1] + ")"
    return res


def get_matching_string(line: str, regex: str) -> List[str]:
    """
    Find all matches of a regex pattern in a line.

    Parameters:
        line (str): The input line to search for matches.
        regex (str): The regex pattern to search for.

    Returns:
        list of str: List of matching strings found in the line.
    """
    if line is None or regex is None:
        return []  # Return an empty list for null inputs
    
    matches = re.findall(regex, line)
    return matches if matches else []


def get_highest_degree(degrees: List[str]) -> Optional[str]:
    """
    Get the highest priority degree from a list of degrees.

    Parameters:
        degrees (list of str): List of degrees to choose from.

    Returns:
        str or None: The highest priority degree or None if no valid degree found.
    """

    if degrees:
        for degree in degree_priority:
            if degree in degrees:
                return degree
    return None


def safe_get_matching_string(line, regex):
    if isinstance(line, (str, bytes)):
        return get_matching_string(line, regex)
    else:
        return []


get_degree_list_udf = udf(
    lambda line, regex: safe_get_matching_string(line, regex), ArrayType(StringType())
)

get_highest_degree_udf = udf(get_highest_degree, StringType())


# In[11]:


def preprocess_text_column(
    df: DataFrame, input_column: str, output_column: str
) -> DataFrame:
    """
    Preprocess a text column by lowercasing and removing non-alphabet characters.

    Args:
        df (DataFrame): Input DataFrame with text column.
        input_column (str): Name of the input text column.
        output_column (str): Name of the output column for cleaned text.

    Returns:
        DataFrame: DataFrame with the added cleaned text column.
    """

    cleaned_df = df.withColumn(
        output_column, lower(regexp_replace(col(input_column), "[^a-zA-Z\s]", ""))
    )
    return cleaned_df


def tokenize_and_remove_stopwords(
    df: DataFrame, input_column: str, output_column: str
) -> DataFrame:
    """
    Tokenize and remove stopwords from a text column.

    Args:
        df (DataFrame): Input DataFrame with text column.
        input_column (str): Name of the input text column.
        output_column (str): Name of the output column for tokenized and filtered words.

    Returns:
        DataFrame: DataFrame with tokenized and filtered words column.
    """
    tokenized_df = df.withColumn(output_column, split(col(input_column), "\s+"))

    remover = StopWordsRemover(
        inputCol=output_column, outputCol="filtered_" + output_column
    )
    filtered_df = remover.transform(tokenized_df)

    return filtered_df


# Define a UDF for lemmatization using NLTK
def lemmatize_words(words):
    if words is not None:
        return [lemmatizer.lemmatize(word) for word in words]
    else:
        return []


def apply_lemmatization(
    df: DataFrame, input_column: str, output_column: str
) -> DataFrame:
    """
    Apply lemmatization to a text column using a provided lemmatization function.

    Args:
        df (DataFrame): Input DataFrame with text column.
        input_column (str): Name of the input text column.
        output_column (str): Name of the output column for lemmatized words.
        lemmatize_func: Lemmatization function to be applied.

    Returns:
        DataFrame: DataFrame with lemmatized words column.
    """

    lemmatize_udf = udf(lemmatize_words, ArrayType(StringType()))
    lemmatized_df = df.withColumn(output_column, lemmatize_udf(col(input_column)))

    return lemmatized_df


def extract_ngrams(
    df: DataFrame, input_column: str, output_column: str, n: int
) -> DataFrame:
    """
    Extract n-grams from a tokenized column.

    Args:
        df (DataFrame): Input DataFrame with tokenized words column.
        input_column (str): Name of the input tokenized words column.
        output_column (str): Name of the output column for extracted n-grams.
        n (int): Number of n-grams.

    Returns:
        DataFrame: DataFrame with extracted n-grams column.
    """
    ngram = NGram(n=n, inputCol=input_column, outputCol=output_column)
    ngram_df = ngram.transform(df)

    return ngram_df

