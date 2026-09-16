package interview

import org.apache.spark.sql.{DataFrame, SparkSession}

final case class Invoice(
    source: String, clinic_id: String, invoice_id: String,
    vet_id: Option[String], amount_cents: Long, invoice_date: String
)
final case class ClinicTotal(clinic_id: String, amount_cents: Long)
final case class Vet(vet_id: String, active: Boolean, updated_at: String, revision: Long)

object Fixtures {
  def one(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      Invoice("vet_system_one", "clinic_a", "inv_1", Some("vet_a"), 1000L, "2020-12-28"),
      Invoice("vet_system_one", "clinic_a", "inv_2", None, 2000L, "2021-01-04"),
      Invoice("vet_system_one", "clinic_a", "inv_3", Some("vet_b"), 3000L, "2021-01-04")
    ).toDF()
  }

  def two(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(Invoice("vet_system_two", "clinic_b", "inv_1", Some("vet_a"), 4000L, "2021-01-04"))
      .toDF().select("source", "invoice_id", "clinic_id", "vet_id", "amount_cents", "invoice_date")
  }

  def vets(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(
      Vet("vet_a", true, "2021-01-01", 1L),
      Vet("vet_a", true, "2021-01-01", 2L),
      Vet("vet_b", false, "2021-01-01", 1L)
    ).toDF()
  }

  def clinics(spark: SparkSession): DataFrame = {
    import spark.implicits._
    Seq(("clinic_a", "Clinic A"), ("clinic_b", "Clinic B"))
      .toDF("clinic_id", "clinic_name")
  }
}
