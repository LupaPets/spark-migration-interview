package migration.model

final case class Client(id: String, clinic_id: String, name: String)
final case class Pet(id: String, clinic_id: String, client_id: String, name: String)
final case class StoreInvoice(
    id: String, source: String, clinic_id: String, source_id: String,
    client_id: String, vet_name: Option[String], amount_cents: Long, invoice_date: String
)
final case class Payment(id: String, clinic_id: String, invoice_id: String, amount_cents: Long)
final case class ClinicTotal(clinic_id: String, amount_cents: Long)
