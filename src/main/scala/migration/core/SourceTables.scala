package migration.core

import org.apache.spark.sql.DataFrame

trait SourceTables {
  def table(name: String): DataFrame
}

final class InMemoryTables(tables: Map[String, DataFrame]) extends SourceTables {
  override def table(name: String): DataFrame =
    tables.getOrElse(name, throw new IllegalArgumentException(s"Missing source table: $name"))
}
