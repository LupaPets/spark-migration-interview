package migration

import migration.core._
import migration.demo.SyntheticExports
import migration.model._
import migration.output.BatchWriter
import migration.pipelines.systemone.InvoiceLoader
import migration.reporting.BatchReport
import migration.validation.{BatchValidation, Reconciliation}
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._

object PlatformSupportCheck {
  private def checkFinancialReports(batch: MigrationBatch, spark: SparkSession): Unit = {
    import spark.implicits._
    val original = Seq(
      InvoiceInput("clinic_a", "1", "owner", None, 1000L, "2021-01-01"),
      InvoiceInput("clinic_a", "2", "owner", None, 2000L, "2021-01-04"),
      InvoiceInput("clinic_a", "3", "owner", None, -500L, "2021-01-04"),
      InvoiceInput("clinic_b", "1", "owner", None, 4000L, "2021-01-04")
    ).toDS()
    assert(Reconciliation.compare(original, batch.invoices).filter(!$"matches").isEmpty)

    val a = Reconciliation.byMonth(batch.invoices).filter($"clinic_id" === "clinic_a").first()
    assert(a.getAs[Long]("sales_cents") == 3000L)
    assert(a.getAs[Long]("credit_cents") == -500L)
    assert(a.getAs[Long]("net_cents") == 2500L)

    val missingClinic = batch.invoices.filter($"clinic_id" === "clinic_a")
    val comparison = Reconciliation.compare(original, missingClinic)
    val absent = comparison.filter($"clinic_id" === "clinic_b").first()
    assert(absent.getAs[Long]("count_delta") == -1L)
    assert(absent.getAs[Long]("amount_delta") == -4000L)
    assert(!absent.getAs[Boolean]("matches"))

    val extra = batch.invoices.head().copy(id = "extra", clinic_id = "clinic_extra")
    val unexpected = Reconciliation.compare(original, batch.invoices.union(Seq(extra).toDS()))
      .filter($"clinic_id" === "clinic_extra").first()
    assert(unexpected.getAs[Long]("expected_count") == 0L)
    assert(unexpected.getAs[Long]("actual_count") == 1L)
    assert(Reconciliation.compare(spark.emptyDataset[InvoiceInput], spark.emptyDataset[StoreInvoice]).isEmpty)
  }

  private def checkArchiveReports(spark: SparkSession): Unit = {
    import spark.implicits._
    val tables = SyntheticExports.forSource("vet_system_one", spark)
    val profiles = InvoiceLoader.profiles(tables, spark).collect().map(row =>
      row.getAs[String]("feed") -> row.getAs[Long]("row_count")).toMap
    assert(profiles == Map("current" -> 4L, "historical" -> 1L))
    assert(InvoiceLoader.overlappingKeys(tables).isEmpty)
    assert(InvoiceLoader.diagnostics(tables).filter($"invalid_dates" > 0L).isEmpty)

    val badDate = tables.table("invoices").withColumn("invoice_date", lit("not-a-date"))
    val broken = new InMemoryTables(Map(
      "invoices" -> badDate,
      "archived_invoices" -> tables.table("archived_invoices")
    ))
    val diagnostics = InvoiceLoader.diagnostics(broken).filter($"feed" === "current").first()
    assert(diagnostics.getAs[Long]("invalid_dates") == 4L)
  }

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
      checkFinancialReports(batch, spark)
      checkArchiveReports(spark)
      val destinations = BatchWriter.plan(batch, PipelineConfig("vet_system_two", "clinic_a", "/tmp/example/"))
      assert(destinations.map(_.name).toSet == Set("clients", "pets", "invoices", "payments"))
      assert(destinations.forall(_.path.startsWith("/tmp/example/")))
      assert(destinations.forall(!_.path.contains("//")))
      assert(scala.util.Try(BatchWriter.plan(batch, PipelineConfig("vet_system_two", "clinic_a", ""))).isFailure)
      println("Platform support checks passed")
    } finally spark.stop()
  }
}
