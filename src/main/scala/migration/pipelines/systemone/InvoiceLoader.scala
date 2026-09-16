package migration.pipelines.systemone

import migration.core.{SchemaContract, SourceTables}
import migration.model.InvoiceInput
import migration.reporting.SourceProfile
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

object InvoiceLoader {
  private val schema = Encoders.product[InvoiceInput].schema

  private def read(tables: SourceTables, name: String): DataFrame = {
    val frame = tables.table(name)
    SchemaContract.requireUniqueNames(frame, name)
    SchemaContract.validate(frame, schema, name)
  }

  private def current(tables: SourceTables): DataFrame = read(tables, "invoices")

  private def historical(tables: SourceTables): DataFrame = read(tables, "archived_invoices")

  def load(tables: SourceTables, spark: SparkSession): Dataset[InvoiceInput] = {
    import spark.implicits._
    val currentRows = current(tables)
    val historicalRows = historical(tables)
    currentRows.union(historicalRows).as[InvoiceInput]
  }

  def profiles(tables: SourceTables, spark: SparkSession): DataFrame = {
    import spark.implicits._
    val currentProfile = SourceProfile.describe(current(tables).as[InvoiceInput], "current")
    val historicalProfile = SourceProfile.describe(historical(tables).as[InvoiceInput], "historical")
    currentProfile.unionByName(historicalProfile)
  }

  private def inspect(frame: DataFrame, feed: String): DataFrame = {
    val missingKey =
      col("clinic_id").isNull || length(trim(col("clinic_id"))) === 0 ||
        col("invoice_id").isNull || length(trim(col("invoice_id"))) === 0
    val invalidDate = expr("try_cast(invoice_date as date)").isNull
    frame.agg(
      coalesce(sum(when(missingKey, 1L).otherwise(0L)), lit(0L)).as("missing_keys"),
      coalesce(sum(when(invalidDate, 1L).otherwise(0L)), lit(0L)).as("invalid_dates"),
      coalesce(sum(when(col("amount_cents").isNull, 1L).otherwise(0L)), lit(0L)).as("missing_amounts")
    ).withColumn("feed", lit(feed))
      .select("feed", "missing_keys", "invalid_dates", "missing_amounts")
  }

  def diagnostics(tables: SourceTables): DataFrame =
    inspect(current(tables), "current").unionByName(inspect(historical(tables), "historical"))

  def overlappingKeys(tables: SourceTables): DataFrame = {
    val keys = Seq("clinic_id", "invoice_id")
    current(tables).select(keys.map(col): _*).distinct()
      .join(historical(tables).select(keys.map(col): _*).distinct(), keys, "inner")
  }
}
