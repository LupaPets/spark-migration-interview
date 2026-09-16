package migration.reporting

import migration.core.{BatchTables, MigrationBatch}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BatchReport {
  def inventory(batch: MigrationBatch): DataFrame =
    BatchTables.all(batch).map { case (entity, frame) =>
      frame.groupBy("clinic_id")
        .agg(count(lit(1)).as("row_count"), countDistinct("id").as("unique_ids"))
        .withColumn("entity", lit(entity))
        .select("entity", "clinic_id", "row_count", "unique_ids")
    }.reduce(_.unionByName(_))

  def invoiceCoverage(batch: MigrationBatch): DataFrame =
    batch.invoices.groupBy("source", "clinic_id").agg(
      count(lit(1)).as("invoice_count"),
      sum(when(col("vet_name").isNotNull, 1L).otherwise(0L)).as("with_vet"),
      sum(when(col("vet_name").isNull, 1L).otherwise(0L)).as("without_vet"),
      min("invoice_date").as("earliest_date"),
      max("invoice_date").as("latest_date")
    )

  def preview(batch: MigrationBatch, rows: Int): Unit = {
    require(rows >= 0 && rows <= 100, "Preview must be bounded to at most 100 rows")
    if (rows > 0) {
      batch.invoices.orderBy("clinic_id", "source_id").show(rows, truncate = false)
    }
  }

  def printSummary(batch: MigrationBatch): Unit = {
    println("Target entity inventory")
    inventory(batch).orderBy("entity", "clinic_id").show(20, truncate = false)
    println("Invoice enrichment coverage")
    invoiceCoverage(batch).orderBy("source", "clinic_id").show(20, truncate = false)
  }
}
