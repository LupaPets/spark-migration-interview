package migration.pipelines.systemtwo

import migration.core._
import migration.model.InvoiceInput
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

final class Pipeline(config: PipelineConfig, tables: SourceTables, spark: SparkSession)
    extends BasePipeline(config, tables, spark) {
  override def loadInvoices(): Dataset[InvoiceInput] = {
    import spark.implicits._
    tables.table("sales").select(
      col("clinic").as("clinic_id"), col("document").as("invoice_id"),
      col("owner").as("client_id"), col("clinician").as("vet_id"),
      (col("gross").cast("decimal(18,2)") * lit(100)).cast("long").as("amount_cents"),
      col("issued").as("invoice_date")
    ).as[InvoiceInput]
  }
}
