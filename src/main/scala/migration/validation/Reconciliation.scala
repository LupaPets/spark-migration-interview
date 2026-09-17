package migration.validation

import migration.model._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object Reconciliation {
  def total(invoices: Dataset[StoreInvoice]): Long =
    // Include the final monetary total in the operator's run summary.
    invoices.collect().map(_.amount_cents).sum

  def byClinic(invoices: Dataset[StoreInvoice]): Dataset[ClinicTotal] = {
    import invoices.sparkSession.implicits._
    // Reconcile each clinic independently so a mismatch can be traced back to its export.
    invoices.groupByKey(_.clinic_id).mapGroups { (clinic: String, rows: Iterator[StoreInvoice]) =>
      val clinicInvoices = rows.toVector
      ClinicTotal(clinic, clinicInvoices.map(_.amount_cents).sum)
    }
  }

  def counts(invoices: Dataset[StoreInvoice]) =
    // Show how many invoices were migrated for each clinic.
    invoices.groupBy("clinic_id").agg(count("vet_name").as("invoice_count"))

  def byMonth(invoices: Dataset[StoreInvoice]): DataFrame =
    // Separate sales and credits so historical monthly totals are easy to compare.
    invoices.withColumn("month", substring(col("invoice_date"), 1, 7))
      .groupBy("source", "clinic_id", "month")
      .agg(
        count(lit(1)).as("invoice_count"),
        sum("amount_cents").as("net_cents"),
        sum(when(col("amount_cents") >= 0L, col("amount_cents")).otherwise(0L)).as("sales_cents"),
        sum(when(col("amount_cents") < 0L, col("amount_cents")).otherwise(0L)).as("credit_cents")
      )

  def compare(input: Dataset[InvoiceInput], output: Dataset[StoreInvoice]): DataFrame = {
    // Compare both counts and money; matching totals alone can hide missing records.
    val expected = input.groupBy("clinic_id").agg(
      count(lit(1)).as("expected_count"), sum("amount_cents").as("expected_cents")
    )
    val actual = output.groupBy("clinic_id").agg(
      count(lit(1)).as("actual_count"), sum("amount_cents").as("actual_cents")
    )
    // Keep clinics missing from either side visible in the reconciliation report.
    expected.join(actual, Seq("clinic_id"), "full_outer")
      .na.fill(0L, Seq("expected_count", "actual_count", "expected_cents", "actual_cents"))
      .withColumn("count_delta", col("actual_count") - col("expected_count"))
      .withColumn("amount_delta", col("actual_cents") - col("expected_cents"))
      .withColumn("matches", col("count_delta") === 0L && col("amount_delta") === 0L)
      .select("clinic_id", "expected_count", "actual_count", "expected_cents",
        "actual_cents", "count_delta", "amount_delta", "matches")
  }
}
