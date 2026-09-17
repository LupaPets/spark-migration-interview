package migration.pipelines.systemone

import migration.core.SourceTables
import migration.model.InvoiceInput
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object InvoiceLoader {
  def load(tables: SourceTables, spark: SparkSession): Dataset[InvoiceInput] = {
    import spark.implicits._
    val current = tables.table("invoices")
    val historical = tables.table("archived_invoices")
    // Import both feeds together; keep the initial rollout small enough to inspect manually.
    current.union(historical).limit(1000).as[InvoiceInput]
  }

  def profiles(tables: SourceTables, spark: SparkSession): DataFrame = {
    // Keep feed totals separate so operators can compare current and historical exports.
    val current = tables.table("invoices").withColumn("feed", lit("current"))
    val historical = tables.table("archived_invoices").withColumn("feed", lit("historical"))
    current.unionByName(historical)
      .groupBy("feed")
      // Report source quality without rejecting records from the import.
      .agg(
        count(lit(1)).as("row_count"),
        countDistinct("clinic_id").as("clinic_count"),
        coalesce(sum("amount_cents"), lit(0L)).as("amount_cents"),
        sum(when(col("vet_id").isNull, 1L).otherwise(0L)).as("missing_vet"),
        sum(when(col("amount_cents") < 0L, 1L).otherwise(0L)).as("credit_notes"),
        sum(when(expr("try_cast(invoice_date as date)").isNull, 1L).otherwise(0L)).as("invalid_dates"),
        min("invoice_date").as("earliest_date"),
        max("invoice_date").as("latest_date")
      )
      .orderBy("feed")
  }
}
