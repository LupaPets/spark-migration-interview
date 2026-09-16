package migration.shared

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object VetDirectory {
  def active(vets: DataFrame): DataFrame = {
    val order = Window.partitionBy("vet_id").orderBy(col("updated_at").desc, col("revision").desc)
    vets.withColumn("_rn", row_number().over(order))
      .filter(col("_rn") === 1).filter(col("active"))
      .select(col("vet_id"), col("name").as("vet_name"))
  }
}
