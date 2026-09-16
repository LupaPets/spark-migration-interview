package interview

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FixtureCheck {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("fixture-check")
      .config("spark.ui.enabled", "false").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try {
      val all = Fixtures.one(spark).unionByName(Fixtures.two(spark))
      assert(all.count() == 4L)
      assert(all.select("source", "clinic_id", "invoice_id").distinct().count() == 4L)
      val totals = all.groupBy("clinic_id").agg(sum("amount_cents")).collect()
        .map(r => r.getString(0) -> r.getLong(1)).toMap
      assert(totals == Map("clinic_a" -> 6000L, "clinic_b" -> 4000L))
      println("Fixture contract checks passed")
    } finally spark.stop()
  }
}
