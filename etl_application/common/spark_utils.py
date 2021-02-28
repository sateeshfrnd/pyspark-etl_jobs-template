"""
Holds spark utility functions
"""
import sys

try:
    import findspark
    findspark.init()
    import pyspark
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame
except ImportError as e:
    print("Error importing Spark Modules", e)
    sys.exit(1)

def get_spark_session(
    app_name : str
) -> SparkSession :
    spark = (
        SparkSession.builder
        .master('local')
        .appName(app_name)
        .getOrCreate()
    )
    return spark
