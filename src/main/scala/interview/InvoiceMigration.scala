package interview

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object InvoiceMigration {
  def loadInvoices(one: DataFrame, two: DataFrame, vets: DataFrame): DataFrame = {
    one.union(two)
      .join(vets, Seq("vet_id"), "left")
      .filter(col("active") === true)
      .select("source", "clinic_id", "invoice_id", "vet_id", "amount_cents", "invoice_date")
  }

  def transformInvoices(input: DataFrame, spark: SparkSession): Dataset[Invoice] = {
    import spark.implicits._
    input.as[Invoice].map { invoice =>
      val format = DateTimeFormatter.ofPattern("YYYY-MM-dd", Locale.UK)
      invoice.copy(invoice_date = LocalDate.parse(invoice.invoice_date).format(format))
    }
  }

  def clinicTotals(invoices: Dataset[Invoice], spark: SparkSession): Dataset[ClinicTotal] = {
    import spark.implicits._
    invoices.groupByKey(_.clinic_id).mapGroups { (clinic: String, rows: Iterator[Invoice]) =>
      val clinicInvoices = rows.toVector
      ClinicTotal(clinic, clinicInvoices.map(_.amount_cents).sum)
    }
  }

  def reconciliation(invoices: Dataset[Invoice]): Long = {
    val rows = invoices.collect()
    rows.map(_.amount_cents).sum
  }

  def withClinicNames(invoices: DataFrame, clinics: DataFrame): DataFrame =
    invoices.join(broadcast(clinics), Seq("clinic_id"), "left")

  def writeClinic(invoices: DataFrame, clinicId: String, output: String): Unit = {
    invoices.filter(col("clinic_id") === clinicId)
      .coalesce(1)
      .write.mode("overwrite").partitionBy("clinic_id").parquet(output)
  }

  def writeAudit(invoices: DataFrame, output: String): Unit = {
    // The downstream audit reader expects distributed files; target about 128 MB per file.
    invoices.repartition(240).write.mode("append").parquet(output)
  }
}
