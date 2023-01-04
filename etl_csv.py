import csv
import logging
import os
import sys
from datetime import datetime

import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.utils import AnalysisException

logging.basicConfig(level=logging.INFO)

findspark.init("C:\\w\\source\\spark\\spark-3.3.0-bin-hadoop3")
main_path = "C:\\w\\NoteBook"
csv_transaction = "transaction.csv"
csv_identity = "identity.csv"

spark_session = (
    SparkSession.builder
    .appName("Union and Load Dataframe")
    .master("local[*]")
    .config("spark.executor.memory", "4G")
    .config("spark.driver.memory", "4G")
    .config("spark.driver.maxResultSize", "8G")
    .getOrCreate()
)


def main ():
    logging.info("Starting ETL job")
    # Extract
    df_extacted_transaction = extract_transaction()
    df_extacted_identity = extract_identity()

    # Transform 
    df_transformed_transaction = transform_transaction(df_extacted_transaction)
    df_transformed_identity = transform_identity(df_extacted_identity)
    df_join = joining_tables(df_transformed_transaction, df_transformed_identity)
    
    # Load
    load_joined_result(df_join)


def extract_transaction():
    """ Read file transaction.csv and load it into dataframe format
        Select specific columns
        Throw Exception if file does not exist
    """
    
    logging.info(f"Started retrieving data from {csv_transaction}")

    try:
        df_transaction = spark_session.read.option("header", True).csv(
            os.path.join(main_path, csv_transaction))
    except AnalysisException:
        logging.error(
            f"The file {csv_transaction} does not exist or cannot be read")

    df_transaction = (
        df_transaction
        .select("TransactionID",
            "TransactionAmt",
            "Field1",
            "Field2",
            "Field3"
            )
    )

    return df_transaction
            

def extract_identity():
    """ Read file identity.csv and load it into dataframe format
        Select specific columns
        Throw Exception if file does not exist
    """

    logging.info(f"Started retrieving data from {csv_identity}")

    try:
        df_identity = spark_session.read.option("header", True).csv(
             os.path.join(main_path, csv_identity))
    except AnalysisException:
        logging.warning(
            f"The file {csv_identity} does not exist or cannot be read")

    df_identity = df_identity.select(
        "TransactionID",
        "DeviceInfo",
        "id-12")

    return df_identity


def transform_transaction(df_transaction):
    """ Apply transformation on TransactionAmt column """

    return df_transaction.withColumn(
            "TransactionAmtTransformed",
            when(
                df_transaction.TransactionAmt.isNotNull(),
                df_transaction.TransactionAmt * 100)
                .otherwise(100)
                )

def transform_identity(df_identity):
    """ Apply filter on DeviceInfo column """

    return df_identity.filter(col("DeviceInfo").isNotNull())


def joining_tables(df_transformed_transaction, df_transformed_identity):
    """ Join both tables on TransactionID column
        Drop some columns to clean the result
    """

    df_join = df_transformed_transaction.join(
        df_transformed_identity,
        df_transformed_transaction.TransactionID == df_transformed_identity.TransactionID,
        "inner")

    return df_join.drop("TransactionID", "TransactionAmt")


def load_joined_result(df_join):
    """ Create today's directory
        Write the joined dataframe as parquet files in it
        throw Exception is an error occurs during the writing process
    """

    today = datetime.now().strftime("%Y-%m-%d")
    writing_path = os.path.join(main_path, "results", today)
    os.makedirs(writing_path, exist_ok=False)

    try:
        df_join.write.mode("overwrite").parquet(writing_path)
    except Exception as x:
        logging.error(f"Result file could not be written in {writing_path}")
        logging.error(f"Exception: {x}")
    else:
        logging.info(f"Success: today's table was written in {writing_path}")


main()