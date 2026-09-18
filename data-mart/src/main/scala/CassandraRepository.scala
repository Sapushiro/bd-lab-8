import org.apache.spark.sql.{DataFrame, SparkSession}

class CassandraRepository(
    spark: SparkSession,
    keyspace: String,
    sourceTable: String,
    resultTable: String
) {

  def readProducts(): DataFrame = {
    spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("keyspace", keyspace)
      .option("table", sourceTable)
      .load()
  }

  def writePredictions(data: DataFrame): Unit = {
    data.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .option("keyspace", keyspace)
      .option("table", resultTable)
      .save()
  }
}