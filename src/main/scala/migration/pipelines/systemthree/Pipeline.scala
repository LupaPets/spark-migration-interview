package migration.pipelines.systemthree

import migration.core._
import migration.model.InvoiceInput
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

final class Pipeline(config: PipelineConfig, tables: SourceTables, spark: SparkSession)
    extends BasePipeline(config, tables, spark) {
  override def loadInvoices(): Dataset[InvoiceInput] = {
    import spark.implicits._
    tables.table("ledger").select(
      col("site").as("clinic_id"), col("reference").as("invoice_id"),
      col("account").as("client_id"), col("staff").as("vet_id"),
      col("minor_units").as("amount_cents"), col("date").as("invoice_date")
    ).as[InvoiceInput]
  }
}
