package migration.shared

import migration.model._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

object InvoiceMappings {
  def enrich(input: Dataset[InvoiceInput], vets: DataFrame, clinics: DataFrame): DataFrame =
    input.toDF().join(vets.select(col("vet_id"), col("name").as("vet_name"), col("active")), Seq("vet_id"), "left")
      .filter(col("active") === true)
      .crossJoin(broadcast(clinics.withColumnRenamed("clinic_id", "reference_clinic_id")))

  def transform(input: Dataset[InvoiceInput], source: String, vets: DataFrame, clinics: DataFrame): Dataset[StoreInvoice] = {
    import input.sparkSession.implicits._
    enrich(input, vets, clinics)
      .select(struct(input.columns.map(col): _*).as("invoice"), col("vet_name"))
      .as[(InvoiceInput, Option[String])]
      .map { case (r, vetName) =>
        StoreInvoice(
          StableIds.entity(source, r.clinic_id, "invoice", r.invoice_id),
          source, r.clinic_id, r.invoice_id,
          StableIds.entity(source, r.clinic_id, "client", r.client_id),
          vetName, r.amount_cents, r.invoice_date
        )
      }
  }
}
