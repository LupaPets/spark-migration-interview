package migration.model

final case class InvoiceInput(
    clinic_id: String, invoice_id: String, client_id: String,
    vet_id: Option[String], amount_cents: Long, invoice_date: String
)
final case class ClientInput(clinic_id: String, client_id: String, name: String)
final case class PetInput(clinic_id: String, pet_id: String, client_id: String, name: String)
final case class PaymentInput(clinic_id: String, payment_id: String, invoice_id: String, amount_cents: Long)
final case class VetInput(vet_id: String, name: String, active: Boolean, updated_at: String, revision: Long)
