"""

"""
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType

def run(spark, config):
    """
    spark : SparkSession,

    Returns:

    """
    print(">>> execute job1")
    print(config)


    data = [
        {"id": 1, 'first': "Tejaswini", 'last': "", 'surname': "Uppara"},
        {"id": 2, 'first': "Bhavishya", 'last': "", 'surname': "Uppara"},
        {"id": 3, 'first': "Satish", 'last': "Kumar", 'surname': "Uppara"},
    ]

    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('first', StringType(), False),
        StructField('last', StringType(), False),
        StructField('surname', StringType(), False)
    ])

    df = spark.createDataFrame(data, schema)
    df.printSchema()
    df.show()


