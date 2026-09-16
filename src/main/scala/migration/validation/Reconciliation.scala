package migration.validation

import migration.model._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object Reconciliation {
  def total(invoices: Dataset[StoreInvoice]): Long =
    invoices.collect().map(_.amount_cents).sum

  def byClinic(invoices: Dataset[StoreInvoice]): Dataset[ClinicTotal] = {
    import invoices.sparkSession.implicits._
    invoices.groupByKey(_.clinic_id).mapGroups { (clinic: String, rows: Iterator[StoreInvoice]) =>
      val clinicInvoices = rows.toVector
      ClinicTotal(clinic, clinicInvoices.map(_.amount_cents).sum)
    }
  }

  def counts(invoices: Dataset[StoreInvoice]) =
    invoices.groupBy("clinic_id").agg(count("vet_name").as("invoice_count"))

  def byMonth(invoices: Dataset[StoreInvoice]): DataFrame =
    invoices.withColumn("month", substring(col("invoice_date"), 1, 7))
      .groupBy("source", "clinic_id", "month")
      .agg(
        count(lit(1)).as("invoice_count"),
        sum("amount_cents").as("net_cents"),
        sum(when(col("amount_cents") >= 0L, col("amount_cents")).otherwise(0L)).as("sales_cents"),
        sum(when(col("amount_cents") < 0L, col("amount_cents")).otherwise(0L)).as("credit_cents")
      )

  private def totals(frame: DataFrame, prefix: String): DataFrame =
    frame.groupBy("clinic_id").agg(
      count(lit(1)).as(s"${prefix}_count"),
      sum("amount_cents").as(s"${prefix}_cents")
    )

  def compare(input: Dataset[InvoiceInput], output: Dataset[StoreInvoice]): DataFrame = {
    val expected = totals(input.toDF(), "expected")
    val actual = totals(output.toDF(), "actual")
    val joined = expected.join(actual, Seq("clinic_id"), "full_outer")
    val values = Seq("expected_count", "actual_count", "expected_cents", "actual_cents")
      .foldLeft(joined) { (frame, name) =>
        frame.withColumn(name, coalesce(col(name), lit(0L)))
      }
    values
      .withColumn("count_delta", col("actual_count") - col("expected_count"))
      .withColumn("amount_delta", col("actual_cents") - col("expected_cents"))
      .withColumn("matches", col("count_delta") === 0L && col("amount_delta") === 0L)
      .select("clinic_id", "expected_count", "actual_count", "expected_cents",
        "actual_cents", "count_delta", "amount_delta", "matches")
  }
}
