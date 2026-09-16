package migration.core

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

object SchemaContract {
  def validate(frame: DataFrame, expected: StructType, label: String): DataFrame = {
    val actual = frame.schema.fields.map(field => field.name -> field.dataType).toMap
    val problems = expected.fields.toSeq.flatMap { field =>
      actual.get(field.name) match {
        case None => Some(s"missing ${field.name}")
        case Some(dataType) if dataType != field.dataType =>
          Some(s"${field.name}: expected ${field.dataType.simpleString}, got ${dataType.simpleString}")
        case _ => None
      }
    }
    require(problems.isEmpty, s"$label schema mismatch: ${problems.mkString("; ")}")
    frame
  }

  def requireUniqueNames(frame: DataFrame, label: String): Unit = {
    val duplicates = frame.columns.groupBy(identity).collect {
      case (name, occurrences) if occurrences.length > 1 => name
    }.toSeq.sorted
    require(duplicates.isEmpty, s"$label has duplicate column names: ${duplicates.mkString(", ")}")
  }
}
