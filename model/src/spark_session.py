from pyspark.sql import SparkSession

class SparkSessionFactory:
    @staticmethod
    def create(app_name: str) -> SparkSession:
        spark = SparkSession.builder.appName(app_name).getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        print("Spark master:", spark.sparkContext.master)
        print("Spark driver host:", spark.sparkContext.getConf().get("spark.driver.host"))
        print("Spark driver bind address:", spark.sparkContext.getConf().get("spark.driver.bindAddress", "not set"))

        return spark