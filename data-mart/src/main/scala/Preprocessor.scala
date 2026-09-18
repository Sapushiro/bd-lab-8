import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Preprocessor(
    maxRows: Int,
    seed: Long
) {

  private val featureColumns = Seq(
    "energy_kcal",
    "fat",
    "carbohydrates",
    "proteins",
    "sugars"
  )

  def preprocess(data: DataFrame): DataFrame = {
    val cleaned = data
      .dropDuplicates("code")
      .filter(
        col("energy_kcal").between(0.0, 1000.0) &&
        col("fat").between(0.0, 100.0) &&
        col("carbohydrates").between(0.0, 100.0) &&
        col("proteins").between(0.0, 100.0) &&
        col("sugars").between(0.0, 100.0)
      )
      .na.drop(
        "any",
        featureColumns
      )

    createSample(cleaned)
  }

  private def createSample(
      data: DataFrame
  ): DataFrame = {

    val rowCount = data.count()

    if (rowCount > maxRows) {
      val fraction =
        math.min(
          1.0,
          maxRows.toDouble / rowCount * 1.2
        )

      data
        .sample(
          withReplacement = false,
          fraction = fraction,
          seed = seed
        )
        .limit(maxRows)
    } else {
      data
    }
  }
}