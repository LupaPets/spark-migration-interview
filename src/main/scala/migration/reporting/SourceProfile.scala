package migration.reporting

import migration.model.InvoiceInput
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object SourceProfile {
  def describe(input: Dataset[InvoiceInput], label: String): DataFrame =
    input.agg(
      count(lit(1)).as("row_count"),
      countDistinct("clinic_id").as("clinic_count"),
      coalesce(sum("amount_cents"), lit(0L)).as("amount_cents"),
      coalesce(sum(when(col("vet_id").isNull, 1L).otherwise(0L)), lit(0L)).as("missing_vet"),
      coalesce(sum(when(col("amount_cents") < 0L, 1L).otherwise(0L)), lit(0L)).as("credit_notes"),
      min("invoice_date").as("earliest_date"),
      max("invoice_date").as("latest_date")
    ).withColumn("feed", lit(label))
      .select("feed", "row_count", "clinic_count", "amount_cents",
        "missing_vet", "credit_notes", "earliest_date", "latest_date")
}
