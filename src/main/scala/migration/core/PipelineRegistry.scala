package migration.core

import migration.pipelines.{systemone, systemtwo, systemthree, systemfour, systemfive}
import org.apache.spark.sql.SparkSession

object PipelineRegistry {
  val supported: Seq[String] = Seq("vet_system_one", "vet_system_two", "vet_system_three", "vet_system_four", "vet_system_five")

  def create(config: PipelineConfig, tables: SourceTables, spark: SparkSession): BasePipeline =
    config.source match {
      case "vet_system_one" => new systemone.Pipeline(config, tables, spark)
      case "vet_system_two" => new systemtwo.Pipeline(config, tables, spark)
      case "vet_system_three" => new systemthree.Pipeline(config, tables, spark)
      case "vet_system_four" => new systemfour.Pipeline(config, tables, spark)
      case "vet_system_five" => new systemfive.Pipeline(config, tables, spark)
      case other => throw new IllegalArgumentException(s"Unsupported source: $other")
    }
}
