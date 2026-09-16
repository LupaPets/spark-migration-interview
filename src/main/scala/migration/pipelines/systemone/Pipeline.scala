package migration.pipelines.systemone

import migration.core._
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale
import migration.model.{InvoiceInput, StoreInvoice}
import migration.shared.{InvoiceMappings, StableIds}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

final class Pipeline(config: PipelineConfig, tables: SourceTables, spark: SparkSession)
    extends BasePipeline(config, tables, spark) {
  override def loadInvoices(): Dataset[InvoiceInput] = InvoiceLoader.load(tables, spark)

  override def inputSummary(): DataFrame = InvoiceLoader.profiles(tables, spark)

  override def transformInvoices(input: Dataset[InvoiceInput]): Dataset[StoreInvoice] = {
    import spark.implicits._
    val source = config.source
    InvoiceMappings.enrich(input, tables.table("vets"), tables.table("clinics"))
      .select(struct(input.columns.map(col): _*).as("invoice"), col("vet_name"))
      .as[(InvoiceInput, Option[String])]
      .map { case (r, vetName) =>
        val dateFormat = DateTimeFormatter.ofPattern("YYYY-MM-dd", Locale.UK)
        StoreInvoice(
          StableIds.entity(source, "", "invoice", r.invoice_id),
          source, r.clinic_id, r.invoice_id,
          StableIds.entity(source, r.clinic_id, "client", r.client_id),
          vetName, math.abs(r.amount_cents), LocalDate.parse(r.invoice_date).format(dateFormat)
        )
      }
  }
}
