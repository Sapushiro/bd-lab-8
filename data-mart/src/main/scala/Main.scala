import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: sbt \"run <prepare|publish>\"")
      sys.exit(1)
    }

    val config = Config.load("/opt/app/config.json")

    val spark = SparkSession.builder()
      .appName("OpenFoodFactsDataMart")
      .config("spark.cassandra.connection.host", "cassandra")
      .config("spark.cassandra.connection.port", "9042")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val repository = new CassandraRepository(
        spark = spark,
        keyspace = config.cassandra.keyspace,
        sourceTable = config.cassandra.sourceTable,
        resultTable = config.cassandra.resultTable
      )

      args(0) match {
        case "prepare" =>
          prepareData(
            repository = repository,
            config = config
          )

        case "publish" =>
          publishPredictions(
            spark = spark,
            repository = repository,
            config = config
          )

        case command =>
          println(s"Unknown command: $command")
          println("Available commands: prepare, publish")
          sys.exit(1)
      }

    } finally {
      spark.stop()
    }
  }

  private def prepareData(
      repository: CassandraRepository,
      config: AppConfig
  ): Unit = {
    println("Reading products from Cassandra...")

    val products = repository.readProducts()

    val rawCount = products.count()
    println(s"Raw products count: $rawCount")

    val preprocessor = new Preprocessor(
      maxRows = config.data.maxRows,
      seed = config.model.seed
    )

    println("Starting preprocessing...")

    val prepared = preprocessor.preprocess(products)

    val preparedCount = prepared.count()
    println(
      s"Preprocessing completed. Prepared products: $preparedCount"
    )


    val outputPath = config.data.outputPath

    println(
      s"Writing prepared dataset to $outputPath"
    )

    prepared.write
      .mode("overwrite")
      .parquet(outputPath)

    println(
      "Prepared dataset successfully written"
    )
  }

  private def publishPredictions(
      spark: SparkSession,
      repository: CassandraRepository,
      config: AppConfig
  ): Unit = {
    val predictionsPath = config.model.predictionsPath

    println(
      s"Reading model predictions from $predictionsPath"
    )

    val predictions =
      spark.read.parquet(predictionsPath)

    val predictionsCount =
      predictions.count()

    println(
      s"Predictions loaded. Count: $predictionsCount"
    )

    println(
      "Writing predictions to Cassandra..."
    )

    repository.writePredictions(predictions)

    println(
      "Predictions successfully written to Cassandra"
    )
  }
}