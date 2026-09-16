package migration.pipelines.systemfive

import migration.core._
import migration.model.InvoiceInput
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

final class Pipeline(config: PipelineConfig, tables: SourceTables, spark: SparkSession)
    extends BasePipeline(config, tables, spark) {
  override def loadInvoices(): Dataset[InvoiceInput] = {
    import spark.implicits._
    tables.table("charges").select(
      col("branch").as("clinic_id"), col("number").as("invoice_id"),
      col("customer").as("client_id"), col("vet_id"),
      when(col("is_credit"), -col("cents")).otherwise(col("cents")).as("amount_cents"),
      col("date").as("invoice_date")
    ).as[InvoiceInput]
  }
}
