import json
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_session import SparkSessionFactory
from logger import Logger


class CassandraIngestion:
    def __init__(self, config_path: str = "config.json") -> None:
        self.spark = SparkSessionFactory.create(
            "OpenFoodFactsIngestion"
        )

        logger = Logger(show=True)
        self.log = logger.get_logger(__name__)

        self.config = self._load_config(config_path)

        data_config = self.config["data"]

        self.input_path = data_config["input_path"]
        self.max_rows = data_config["max_rows"]

        cassandra_config = self.config["cassandra"]
        self.keyspace = cassandra_config["keyspace"]
        self.source_table = cassandra_config["source_table"]

    @staticmethod
    def _load_config(config_path: str) -> dict:
        path = Path(config_path)

        if not path.exists():
            raise FileNotFoundError(
                f"Config was not found: {path}"
            )

        with path.open(encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def prepare_source(data: DataFrame) -> DataFrame:
        valid_data = data.filter(
            F.col("code").isNotNull() &
            (F.trim(F.col("code")) != "")
        )

        return (
            valid_data
            .select(
                "code",
                F.explode_outer("nutriments").alias("nutriment")
            )
            .select(
                "code",
                F.col("nutriment.name").alias("nutrient_name"),
                F.col("nutriment.`100g`")
                .cast("double")
                .alias("nutrient_value")
            )
            .filter(
                F.col("nutrient_name").isin(
                    "energy-kcal",
                    "fat",
                    "carbohydrates",
                    "proteins",
                    "sugars"
                )
            )
            .groupBy("code")
            .pivot(
                "nutrient_name",
                [
                    "energy-kcal",
                    "fat",
                    "carbohydrates",
                    "proteins",
                    "sugars"
                ]
            )
            .agg(F.first("nutrient_value"))
            .withColumnRenamed(
                "energy-kcal",
                "energy_kcal"
            )
        )

    def run(self) -> None:
        self.log.info(
            "Starting OpenFoodFacts ingestion"
        )

        self.log.info(
            "Reading source parquet: %s",
            self.input_path
        )

        data = self.spark.read.parquet(
            self.input_path
        )

        self.log.info("Source parquet loaded")

        self.log.info(
            "Preparing data for Cassandra"
        )

        prepared = (
            self.prepare_source(data)
            .limit(self.max_rows)
        )

        self.log.info(
            "Data preparation completed"
        )

        self.log.info(
            "Writing data to Cassandra: %s.%s",
            self.keyspace,
            self.source_table
        )

        prepared.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .option("keyspace", self.keyspace) \
            .option("table", self.source_table) \
            .save()

        self.log.info(
            "Data successfully written to Cassandra"
        )

        self.log.info("Ingestion completed")

        self.spark.stop()


if __name__ == "__main__":
    CassandraIngestion().run()