package migration.output

import migration.core.{BatchTables, MigrationBatch, PipelineConfig}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BatchWriter {
  final case class ExportTable(name: String, path: String, rows: DataFrame)

  def plan(batch: MigrationBatch, config: PipelineConfig): Seq[ExportTable] = {
    require(config.outputRoot.trim.nonEmpty, "Output root must not be empty")
    require(config.clinicId.nonEmpty, "Clinic ID must not be empty")
    val root = config.outputRoot.stripSuffix("/")
    BatchTables.all(batch).map { case (name, rows) =>
      ExportTable(name, s"$root/$name", rows)
    }
  }

  def describePlan(batch: MigrationBatch, config: PipelineConfig): DataFrame = {
    import batch.invoices.sparkSession.implicits._
    plan(batch, config).map(table => (table.name, config.clinicId, table.path))
      .toDF("entity", "clinic_id", "destination")
  }

  def writeTable(frame: DataFrame, clinic: String, path: String): Unit =
    BatchTables.forClinic(frame, "clinic_a")
      .coalesce(1)
      .write.mode("overwrite")
      .option("compression", "snappy")
      .option("maxRecordsPerFile", 250000)
      .partitionBy("clinic_id").parquet(path)

  def write(batch: MigrationBatch, config: PipelineConfig): Unit = {
    plan(batch, config).foreach { table =>
      println(s"Writing ${table.name} for ${config.clinicId} to ${table.path}")
      writeTable(table.rows, config.clinicId, table.path)
    }
  }

  def audit(frame: DataFrame, path: String): Unit =
    // Audit runs have distinct output paths; 30 GB is distributed into roughly 128 MB files.
    frame.repartition(240).write.mode("errorifexists").parquet(path)
}
