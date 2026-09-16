package interview

import org.apache.spark.sql.SparkSession

object Smoke {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("migration-smoke")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val input = InvoiceMigration.loadInvoices(Fixtures.one(spark), Fixtures.two(spark), Fixtures.vets(spark))
      val invoices = InvoiceMigration.transformInvoices(input, spark)
      invoices.show(false)
      InvoiceMigration.clinicTotals(invoices, spark).show(false)
      println(s"Reconciliation cents: ${InvoiceMigration.reconciliation(invoices)}")
    } finally spark.stop()
  }
}
