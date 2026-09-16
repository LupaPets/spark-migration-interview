package migration.validation

import migration.core.{BatchTables, MigrationBatch}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BatchValidation {
  private def finding(rows: DataFrame, entity: String, code: String): DataFrame =
    rows.select(
      lit(entity).as("entity"),
      col("clinic_id"),
      col("id").as("record_id"),
      lit(code).as("code")
    )

  private def duplicateIds(frame: DataFrame, entity: String): DataFrame = {
    val duplicates = frame.groupBy("id").count().filter(col("count") > 1).select("id")
    finding(frame.join(duplicates, Seq("id"), "inner"), entity, "duplicate_id")
  }

  private def missingIds(frame: DataFrame, entity: String): DataFrame =
    finding(frame.filter(col("id").isNull || length(trim(col("id"))) === 0), entity, "missing_id")

  private def orphanReferences(
      children: DataFrame, parents: DataFrame, foreignKey: String, entity: String
  ): DataFrame = {
    val child = children.alias("child")
    val parent = parents.select("id", "clinic_id").alias("parent")
    val unmatched = child.join(
      parent,
      col(s"child.$foreignKey") === col("parent.id") &&
        col("child.clinic_id") === col("parent.clinic_id"),
      "left_anti"
    )
    finding(unmatched, entity, s"unresolved_$foreignKey")
  }

  def findings(batch: MigrationBatch): DataFrame = {
    val identityChecks = BatchTables.all(batch).flatMap { case (entity, frame) =>
      Seq(duplicateIds(frame, entity), missingIds(frame, entity))
    }
    val references = Seq(
      orphanReferences(batch.pets.toDF(), batch.clients.toDF(), "client_id", "pets"),
      orphanReferences(batch.invoices.toDF(), batch.clients.toDF(), "client_id", "invoices"),
      orphanReferences(batch.payments.toDF(), batch.invoices.toDF(), "invoice_id", "payments")
    )
    (identityChecks ++ references).reduce(_.unionByName(_))
  }

  def summary(findings: DataFrame): DataFrame =
    findings.groupBy("entity", "code").agg(count(lit(1)).as("affected_rows"))
}
