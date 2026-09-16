package migration.shared

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID

object StableIds {
  def entity(source: String, clinic: String, kind: String, sourceId: String): String = {
    val key = Seq(source, clinic, kind, sourceId).map(v => s"${v.length}:$v").mkString
    UUID.nameUUIDFromBytes(key.getBytes(UTF_8)).toString
  }
}
