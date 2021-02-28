"""

"""
from pyspark.sql import SparkSession

def run(spark, config):
    """
    spark : SparkSession,

    Returns:

    """
    print(">>> execute job2 ")
    print(config)

    print(spark.range(100000).where("id > 1000").selectExpr("sum(id)").collect())


# if __main__ == "__main__":
#     run()