package migration.pipelines.systemfour

import migration.core._
import migration.model.InvoiceInput
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

final class Pipeline(config: PipelineConfig, tables: SourceTables, spark: SparkSession)
    extends BasePipeline(config, tables, spark) {
  override def loadInvoices(): Dataset[InvoiceInput] = {
    import spark.implicits._
    tables.table("documents").select(
      col("location.id").as("clinic_id"), col("id").as("invoice_id"),
      col("customer.id").as("client_id"), col("vet_id"),
      col("total.cents").as("amount_cents"), col("date").as("invoice_date")
    ).as[InvoiceInput]
  }
}
