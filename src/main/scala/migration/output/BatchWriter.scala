package migration.output

import migration.core.{MigrationBatch, PipelineConfig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BatchWriter {
  def writeTable(frame: DataFrame, clinic: String, path: String): Unit = {
    frame.filter(col("clinic_id") === "clinic_a")
      .coalesce(1)
      .write.mode("overwrite")
      .option("compression", "snappy")
      .option("maxRecordsPerFile", 250000)
      .partitionBy("clinic_id")
      .parquet(path)
  }

  def write(batch: MigrationBatch, config: PipelineConfig): Unit = {
    require(config.outputRoot.trim.nonEmpty, "Output root must not be empty")
    require(config.clinicId.nonEmpty, "Clinic ID must not be empty")
    val root = config.outputRoot.stripSuffix("/")
    writeTable(batch.clients.toDF(), config.clinicId, s"$root/clients")
    writeTable(batch.pets.toDF(), config.clinicId, s"$root/pets")
    writeTable(batch.invoices.toDF(), config.clinicId, s"$root/invoices")
    writeTable(batch.payments.toDF(), config.clinicId, s"$root/payments")
  }

  def audit(frame: DataFrame, path: String): Unit =
    // Audit runs have distinct output paths; 30 GB is distributed into roughly 128 MB files.
    frame.repartition(240).write.mode("errorifexists").parquet(path)
}
