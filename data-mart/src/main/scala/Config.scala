import play.api.libs.json.Json

import scala.io.Source

case class DataConfig(
    outputPath: String,
    maxRows: Int
)

case class CassandraConfig(
    keyspace: String,
    sourceTable: String,
    resultTable: String
)

case class ModelConfig(
    predictionsPath: String,
    seed: Long
)

case class AppConfig(
    data: DataConfig,
    cassandra: CassandraConfig,
    model: ModelConfig
)

object Config {

  def load(path: String): AppConfig = {
    val source = Source.fromFile(path)

    try {
      val json = Json.parse(source.mkString)

      AppConfig(
        data = DataConfig(
          outputPath = (json \ "data" \ "output_path").as[String],
          maxRows = (json \ "data" \ "max_rows").as[Int]
        ),
        cassandra = CassandraConfig(
          keyspace = (json \ "cassandra" \ "keyspace").as[String],
          sourceTable = (json \ "cassandra" \ "source_table").as[String],
          resultTable = (json \ "cassandra" \ "result_table").as[String]
        ),
        model = ModelConfig(
          predictionsPath = (json \ "model" \ "predictions_path").as[String],
          seed = (json \ "model" \ "seed").as[Long]
        )
      )
    } finally {
      source.close()
    }
  }
}