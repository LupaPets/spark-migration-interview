package migration.core

import org.apache.spark.storage.StorageLevel

object InvoiceCache {
  def withCached[A](batch: MigrationBatch)(consume: MigrationBatch => A): A = {
    val invoices = batch.invoices.persist(StorageLevel.MEMORY_AND_DISK)
    try consume(batch.copy(invoices = invoices))
    finally invoices.unpersist()
  }
}
