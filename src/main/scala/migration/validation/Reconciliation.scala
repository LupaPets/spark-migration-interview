package migration.validation

import migration.model._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object Reconciliation {
  def total(invoices: Dataset[StoreInvoice]): Long =
    invoices.agg(coalesce(sum("amount_cents"), lit(0L))).first().getLong(0)

  def byClinic(invoices: Dataset[StoreInvoice]): Dataset[ClinicTotal] = {
    import invoices.sparkSession.implicits._
    invoices.groupBy("clinic_id").agg(sum("amount_cents").as("amount_cents")).as[ClinicTotal]
  }

  def counts(invoices: Dataset[StoreInvoice]) =
    invoices.groupBy("clinic_id").agg(count(lit(1)).as("invoice_count"))
}
