import unittest
from pyspark.sql import SparkSession


class SparkBase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession.builder\
                        .master("local[1]")\
                        .appName("unit_test_spark")\
                        .config("spark.jars.packages","org.xerial:sqlite-jdbc:3.34.0,io.delta:delta-core_2.12:1.1.0")\
                        .config("spark.databricks.delta.schema.autoMerge.enabled","true")\
                        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")\
                        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")\
                        .getOrCreate()
    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

