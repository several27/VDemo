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

@Visual(id = "CountOrders", label = "CountOrders", x = 692, y = 274, phase = 0)
object CountOrders {

  def apply(spark: SparkSession, in: DataFrame): Aggregate = {
    import spark.implicits._

    val dfGroupBy = in.groupBy(col("customer_id").as("customer_id"),
                               col("first_name").as("first_name"),
                               col("last_name").as("last_name"),
                               col("phone").as("phone")
    )
    val out = dfGroupBy.agg(count(col("order_id")).as("orders"), sum(col("amount")).as("amounts"))

    out

  }

}
