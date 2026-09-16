package migration.validation

import migration.model._
import org.apache.spark.sql.Dataset
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
}
