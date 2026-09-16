package migration.core

import migration.model._
import migration.shared.{EntityMappings, InvoiceMappings}
import migration.reporting.SourceProfile
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

abstract class BasePipeline(val config: PipelineConfig, val tables: SourceTables, val spark: SparkSession) {
  def loadInvoices(): Dataset[InvoiceInput]

  def inputSummary(): DataFrame = SourceProfile.describe(loadInvoices(), config.source)

  def transformInvoices(input: Dataset[InvoiceInput]): Dataset[StoreInvoice] =
    InvoiceMappings.transform(input, config.source, tables.table("vets"), tables.table("clinics"))

  final def run(): MigrationBatch = {
    import spark.implicits._
    val source = config.source
    MigrationBatch(
      EntityMappings.clients(tables.table("clients").as[ClientInput], source),
      EntityMappings.pets(tables.table("pets").as[PetInput], source),
      transformInvoices(loadInvoices()),
      EntityMappings.payments(tables.table("payments").as[PaymentInput], source)
    )
  }
}
