package migration.pipelines.systemone

import migration.core._
import migration.model.InvoiceInput
import org.apache.spark.sql.{Dataset, SparkSession}

final class Pipeline(config: PipelineConfig, tables: SourceTables, spark: SparkSession)
    extends BasePipeline(config, tables, spark) {
  override def loadInvoices(): Dataset[InvoiceInput] = InvoiceLoader.load(tables, spark)
}
