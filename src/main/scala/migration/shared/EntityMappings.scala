package migration.shared

import migration.model._
import org.apache.spark.sql.Dataset

object EntityMappings {
  def clients(input: Dataset[ClientInput], source: String): Dataset[Client] = {
    import input.sparkSession.implicits._
    input.map(r => Client(StableIds.entity(source, r.clinic_id, "client", r.client_id), r.clinic_id, r.name))
  }

  def pets(input: Dataset[PetInput], source: String): Dataset[Pet] = {
    import input.sparkSession.implicits._
    input.map(r => Pet(
      StableIds.entity(source, r.clinic_id, "pet", r.pet_id), r.clinic_id,
      StableIds.entity(source, r.clinic_id, "client", r.client_id), r.name
    ))
  }

  def payments(input: Dataset[PaymentInput], source: String): Dataset[Payment] = {
    import input.sparkSession.implicits._
    input.map(r => Payment(
      StableIds.entity(source, r.clinic_id, "payment", r.payment_id), r.clinic_id,
      StableIds.entity(source, r.clinic_id, "invoice", r.invoice_id), r.amount_cents
    ))
  }
}
