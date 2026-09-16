package migration.core

import migration.model._
import migration.shared.{EntityMappings, InvoiceMappings}
import org.apache.spark.sql.{Dataset, SparkSession}

abstract class BasePipeline(val config: PipelineConfig, val tables: SourceTables, val spark: SparkSession) {
  def loadInvoices(): Dataset[InvoiceInput]

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
