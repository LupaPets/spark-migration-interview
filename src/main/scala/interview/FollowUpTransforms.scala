package interview

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

final case class Line(invoice_id: String, qty: Int)
final case class RawInvoice(clinic_id: String, invoice_id: String, qty: Option[Int], notes: String)

final class ReferenceClient extends AutoCloseable {
  def normalize(value: String): String = value.trim
  override def close(): Unit = ()
}

final class FollowUpTransforms(val sourcePrefix: String) {
  def targetIds(invoices: Dataset[Invoice], spark: SparkSession): Dataset[String] = {
    import spark.implicits._
    invoices.map(row => s"$sourcePrefix:${row.clinic_id}:${row.invoice_id}")
  }
}

object FollowUpTransforms {
  def normalizeIds(invoices: Dataset[Invoice], spark: SparkSession): Dataset[String] = {
    import spark.implicits._
    val client = new ReferenceClient
    invoices.map(row => client.normalize(row.invoice_id))
  }

  def migratedCount(invoices: Dataset[Invoice], spark: SparkSession): Long = {
    import spark.implicits._
    var migrated = 0L
    val transformed = invoices.map { row =>
      migrated += 1
      row.copy(invoice_id = row.invoice_id.trim)
    }
    transformed.collect()
    migrated
  }

  def missingVets(invoices: DataFrame): DataFrame =
    invoices.filter(col("vet_id") === lit(null))

  def invoiceCounts(invoices: DataFrame): DataFrame =
    invoices.groupBy("clinic_id").agg(count("vet_id").as("invoice_count"))

  def latestVets(vets: DataFrame): DataFrame = {
    val w = Window.partitionBy("vet_id").orderBy(col("updated_at").desc)
    vets.withColumn("rn", row_number().over(w)).filter(col("rn") === 1).drop("rn")
  }

  def decodeLines(lines: DataFrame, spark: SparkSession): Dataset[Line] = {
    import spark.implicits._
    lines.as[Line]
  }

  def decodeSecondSystem(lines: DataFrame, spark: SparkSession): Dataset[Line] = {
    import spark.implicits._
    lines.select(col("document_id"), col("quantity")).as[Line]
  }

  def readForClinic(path: String, clinicId: String, spark: SparkSession): Dataset[String] = {
    import spark.implicits._
    spark.read.parquet(path).as[RawInvoice]
      .map(row => row.copy(invoice_id = row.invoice_id.trim))
      .filter(_.clinic_id == clinicId)
      .map(_.invoice_id)
  }

  def cleanIds(invoices: DataFrame): DataFrame = {
    val clean = udf((id: String) => Option(id).map(_.replaceAll("[^A-Za-z0-9]", "")).orNull)
    invoices.withColumn("invoice_id", clean(col("invoice_id")))
  }

  def vetTotals(invoices: Dataset[Invoice], spark: SparkSession): DataFrame = {
    import spark.implicits._
    invoices.groupByKey(_.vet_id.getOrElse("UNKNOWN"))
      .mapGroups { (vet: String, rows: Iterator[Invoice]) =>
        (vet, rows.foldLeft(0L)(_ + _.amount_cents))
      }.toDF("vet_id", "amount_cents")
  }

  def publishAudit(invoices: DataFrame, output: String): Unit = {
    val audit = invoices.groupBy("source", "clinic_id", "invoice_id").agg(sum("amount_cents").as("amount_cents"))
    if (audit.count() > 0) {
      println(s"Publishing ${audit.count()} invoices")
      audit.write.mode("append").parquet(output)
    }
  }

  def exportOnce(invoices: DataFrame, output: String): Unit = {
    val snapshot = invoices.cache()
    snapshot.write.mode("append").parquet(output)
  }
}
