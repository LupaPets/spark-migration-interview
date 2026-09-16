package migration

import migration.core._
import migration.demo.SyntheticExports
import org.apache.spark.sql.SparkSession

object PipelineContractCheck {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("pipeline-contracts")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "2").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    var failures = Vector.empty[String]
    def check(label: String)(condition: => Boolean): Unit =
      if (condition) println(s"PASS: $label") else { println(s"FAIL: $label"); failures :+= label }
    try {
      val sources = if (args.nonEmpty) args.toSeq else PipelineRegistry.supported
      sources.foreach { source =>
        val batch = PipelineRegistry.create(PipelineConfig(source, "clinic_a", ""),
          SyntheticExports.forSource(source, spark), spark).run()
        val invoices = batch.invoices.collect().toSeq
        check(s"$source preserves current invoices") {
          invoices.filterNot(_.source_id == "old_1").size == 4
        }
        check(s"$source preserves signed current total") {
          invoices.filterNot(_.source_id == "old_1").map(_.amount_cents).sum == 6500L
        }
        check(s"$source payment references resolve") {
          val ids = invoices.map(_.id).toSet
          batch.payments.collect().forall(p => ids.contains(p.invoice_id))
        }
        check(s"$source client references resolve") {
          val ids = batch.clients.collect().map(_.id).toSet
          invoices.forall(i => ids.contains(i.client_id))
        }
        check(s"$source ids distinguish clinics") { invoices.map(_.id).distinct.size == invoices.size }
        check(s"$source preserves calendar date") {
          invoices.find(i => i.clinic_id == "clinic_a" && i.source_id == "inv_1").exists(_.invoice_date == "2021-01-01")
        }
      }
      require(failures.isEmpty, s"${failures.size} contract checks failed: ${failures.mkString(", ")}")
      println("All pipeline contract checks passed")
    } finally spark.stop()
  }
}
