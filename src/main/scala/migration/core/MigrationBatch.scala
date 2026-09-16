package migration.core

import migration.model._
import org.apache.spark.sql.Dataset

final case class MigrationBatch(
    clients: Dataset[Client], pets: Dataset[Pet],
    invoices: Dataset[StoreInvoice], payments: Dataset[Payment]
)
