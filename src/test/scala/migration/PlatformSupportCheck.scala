package migration

import migration.core._
import migration.model._
import migration.reporting.BatchReport
import migration.validation.BatchValidation
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object PlatformSupportCheck {
  def main(args: Array[String]): Unit = {
    assert(RunOptions.parse(Seq.empty).exists(_.source == "vet_system_one"))
    assert(RunOptions.parse(Seq("unknown")).isLeft)
    assert(RunOptions.parse(Seq("vet_system_two", "--preview", "101")).isLeft)
    assert(RunOptions.parse(Seq("vet_system_one", "--clinic")).isLeft)
    val parsed = RunOptions.parse(Seq("vet_system_two", "/tmp/example", "--clinic", "clinic_b", "--validate"))
    assert(parsed.exists(o => o.validate && o.clinicId == "clinic_b" && o.outputRoot == "/tmp/example"))

    val spark = SparkSession.builder().master("local[2]").appName("platform-support")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    try {
      val batch = MigrationBatch(
        Seq(Client("c1", "clinic_a", "Alex"), Client("c2", "clinic_b", "Sam")).toDS(),
        Seq(Pet("p1", "clinic_a", "c1", "Milo")).toDS(),
        Seq(
          StoreInvoice("i1", "vet_system_two", "clinic_a", "1", "c1", Some("Vet"), 1000L, "2021-01-01"),
          StoreInvoice("i2", "vet_system_two", "clinic_a", "2", "c1", None, 2000L, "2021-01-04"),
          StoreInvoice("i3", "vet_system_two", "clinic_a", "3", "c1", None, -500L, "2021-01-04"),
          StoreInvoice("i4", "vet_system_two", "clinic_b", "1", "c2", Some("Vet"), 4000L, "2021-01-04")
        ).toDS(),
        Seq(Payment("pay1", "clinic_a", "i1", 1000L)).toDS()
      )
      assert(BatchValidation.findings(batch).isEmpty)
      assert(BatchReport.inventory(batch).filter($"entity" === "invoices").agg(sum("row_count")).first().getLong(0) == 4)
      val badPayment = Seq(Payment("broken", "clinic_a", "absent", 1L)).toDS()
      val findings = BatchValidation.findings(batch.copy(payments = badPayment))
      assert(findings.filter($"code" === "unresolved_invoice_id").count() == 1)
      val duplicates = BatchValidation.findings(batch.copy(clients = batch.clients.union(batch.clients)))
      assert(duplicates.filter($"entity" === "clients" && $"code" === "duplicate_id").count() == 4)

      val sample = Seq(InvoiceInput("c", "i", "o", None, 1L, "2021-01-01")).toDF()
      assert(SchemaContract.validate(sample, Encoders.product[InvoiceInput].schema, "sample").count() == 1)
      val mismatch = scala.util.Try(SchemaContract.validate(sample.drop("invoice_id"), Encoders.product[InvoiceInput].schema, "sample"))
      assert(mismatch.isFailure)
      val cached = batch.invoices
      val failed = scala.util.Try(InvoiceCache.withCached(batch)(_ => throw new IllegalStateException("consumer failed")))
      assert(failed.isFailure)
      assert(cached.storageLevel == org.apache.spark.storage.StorageLevel.NONE)
      println("Platform support checks passed")
    } finally spark.stop()
  }
}
