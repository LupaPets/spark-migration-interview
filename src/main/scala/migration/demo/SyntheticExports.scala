package migration.demo

import migration.core.{InMemoryTables, SourceTables}
import migration.model._
import org.apache.spark.sql.SparkSession

object SyntheticExports {
  def forSource(source: String, spark: SparkSession): SourceTables = {
    import spark.implicits._
    val invoices = Seq(
      InvoiceInput("clinic_a", "inv_1", "client_1", Some("vet_a"), 1000L, "2021-01-01"),
      InvoiceInput("clinic_a", "inv_2", "client_1", None, 2000L, "2021-01-04"),
      InvoiceInput("clinic_a", "inv_3", "client_1", Some("vet_b"), -500L, "2021-01-04"),
      InvoiceInput("clinic_b", "inv_1", "client_1", Some("vet_a"), 4000L, "2021-01-04")
    ).toDF()
    val archive = Seq(
      InvoiceInput("clinic_a", "old_1", "client_1", Some("vet_a"), 700L, "2018-12-31")
    ).toDF().select("invoice_id", "clinic_id", "client_id", "vet_id", "amount_cents", "invoice_date")
    val common = Map(
      "clients" -> Seq(ClientInput("clinic_a", "client_1", "Alex"), ClientInput("clinic_b", "client_1", "Sam")).toDF(),
      "pets" -> Seq(PetInput("clinic_a", "pet_1", "client_1", "Milo")).toDF(),
      "payments" -> Seq(PaymentInput("clinic_a", "pay_1", "inv_1", 1000L)).toDF(),
      "vets" -> Seq(
        VetInput("vet_a", "Earlier name", true, "2021-01-01", 1L),
        VetInput("vet_a", "Current name", true, "2021-01-01", 2L),
        VetInput("vet_b", "Retired vet", false, "2021-01-01", 1L)
      ).toDF(),
      "clinics" -> Seq(("clinic_a", "Clinic A"), ("clinic_b", "Clinic B")).toDF("clinic_id", "clinic_name")
    )
    val sourceTables = source match {
      case "vet_system_one" => Map("invoices" -> invoices, "archived_invoices" -> archive)
      case "vet_system_two" => Map("sales" -> invoices.selectExpr(
        "clinic_id as clinic", "invoice_id as document", "client_id as owner",
        "vet_id as clinician", "cast(amount_cents / 100.0 as decimal(18,2)) as gross", "invoice_date as issued"
      ))
      case "vet_system_three" => Map("ledger" -> invoices.selectExpr(
        "clinic_id as site", "invoice_id as reference", "client_id as account",
        "vet_id as staff", "amount_cents as minor_units", "invoice_date as date"
      ))
      case "vet_system_four" => Map("documents" -> invoices.selectExpr(
        "named_struct('id', clinic_id) as location", "invoice_id as id",
        "named_struct('id', client_id) as customer", "vet_id",
        "named_struct('cents', amount_cents) as total", "invoice_date as date"
      ))
      case "vet_system_five" => Map("charges" -> invoices.selectExpr(
        "clinic_id as branch", "invoice_id as number", "client_id as customer", "vet_id",
        "abs(amount_cents) as cents", "amount_cents < 0 as is_credit", "invoice_date as date"
      ))
      case other => throw new IllegalArgumentException(s"Unknown fixture source: $other")
    }
    new InMemoryTables(common ++ sourceTables)
  }
}
