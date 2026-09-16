package migration

import migration.core._
import migration.demo.SyntheticExports
import migration.output.BatchWriter
import migration.validation.Reconciliation
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val source = args.headOption.getOrElse("vet_system_one")
    val spark = SparkSession.builder().master("local[2]").appName("migration-demo")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val config = PipelineConfig(source, "clinic_a", args.lift(1).getOrElse(""))
      val batch = PipelineRegistry.create(config, SyntheticExports.forSource(source, spark), spark).run()
      batch.invoices.show(false)
      Reconciliation.byClinic(batch.invoices).show(false)
      Reconciliation.counts(batch.invoices).show(false)
      println(s"Total cents: ${Reconciliation.total(batch.invoices)}")
      if (config.outputRoot.nonEmpty) BatchWriter.write(batch, config)
    } finally spark.stop()
  }
}
