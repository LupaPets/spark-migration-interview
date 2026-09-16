package migration.output

import migration.core.{MigrationBatch, PipelineConfig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BatchWriter {
  def writeTable(frame: DataFrame, clinic: String, path: String): Unit =
    frame.filter(col("clinic_id") === clinic)
      .coalesce(1)
      .write.mode("overwrite")
      .partitionBy("clinic_id").parquet(path)

  def write(batch: MigrationBatch, config: PipelineConfig): Unit = {
    writeTable(batch.clients.toDF(), config.clinicId, s"${config.outputRoot}/clients")
    writeTable(batch.pets.toDF(), config.clinicId, s"${config.outputRoot}/pets")
    writeTable(batch.invoices.toDF(), config.clinicId, s"${config.outputRoot}/invoices")
    writeTable(batch.payments.toDF(), config.clinicId, s"${config.outputRoot}/payments")
  }

  def audit(frame: DataFrame, path: String): Unit =
    // Audit runs have distinct output paths; 30 GB is distributed into roughly 128 MB files.
    frame.repartition(240).write.mode("errorifexists").parquet(path)
}
