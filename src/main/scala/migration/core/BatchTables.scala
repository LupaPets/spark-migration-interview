package migration.core

import org.apache.spark.sql.DataFrame

object BatchTables {
  def all(batch: MigrationBatch): Seq[(String, DataFrame)] = Seq(
    "clients" -> batch.clients.toDF(),
    "pets" -> batch.pets.toDF(),
    "invoices" -> batch.invoices.toDF(),
    "payments" -> batch.payments.toDF()
  )

  def forClinic(frame: DataFrame, clinicId: String): DataFrame = {
    require(clinicId.nonEmpty, "Clinic ID must not be empty")
    frame.filter(org.apache.spark.sql.functions.col("clinic_id") === clinicId)
  }
}
