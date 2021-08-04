package graph

import org.apache.spark.sql.types._
import io.prophecy.libs._
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import config.ConfigStore._
import udfs.UDFs._
import udfs._
import graph._

@Visual(id = "AddFullName", label = "AddFullName", x = 540, y = 277, phase = 0)
object AddFullName {

  def apply(spark: SparkSession, in: DataFrame): Reformat = {
    import spark.implicits._

    val out = in.select(
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("phone"),
      col("order_id"),
      col("amount"),
      col("order_date"),
      concat(col("first_name"), col("last_name")).as("full_name")
    )

    out

  }

}
