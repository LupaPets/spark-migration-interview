package migration.pipelines.systemone

import migration.core.SourceTables
import migration.model.InvoiceInput
import org.apache.spark.sql.{Dataset, SparkSession}

object InvoiceLoader {
  def load(tables: SourceTables, spark: SparkSession): Dataset[InvoiceInput] = {
    import spark.implicits._
    val current = tables.table("invoices")
    val historical = tables.table("archived_invoices")
    current.union(historical).as[InvoiceInput]
  }
}
