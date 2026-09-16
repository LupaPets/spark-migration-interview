package migration

import migration.core._
import migration.demo.SyntheticExports
import migration.output.BatchWriter
import migration.reporting.BatchReport
import migration.validation.{BatchValidation, Reconciliation}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val options = RunOptions.parse(args.toSeq).fold(message => throw new IllegalArgumentException(message), identity)
    val spark = SparkSession.builder().master("local[2]").appName("migration-demo")
      .config("spark.ui.enabled", "false").config("spark.sql.shuffle.partitions", "4").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val config = options.pipelineConfig
      val tables = SyntheticExports.forSource(options.source, spark)
      val pipeline = PipelineRegistry.create(config, tables, spark)
      pipeline.inputSummary().show(20, truncate = false)
      val batch = pipeline.run()
      InvoiceCache.withCached(batch) { cached =>
        BatchReport.preview(cached, options.previewRows)
        BatchReport.printSummary(cached)
        Reconciliation.byClinic(cached.invoices).show(false)
        Reconciliation.counts(cached.invoices).show(false)
        println(s"Total cents: ${Reconciliation.total(cached.invoices)}")
        if (options.validate) {
          val findings = BatchValidation.findings(cached)
          BatchValidation.summary(findings).show(20, truncate = false)
        }
        if (config.outputRoot.nonEmpty) BatchWriter.write(cached, config)
      }
    } finally spark.stop()
  }
}
