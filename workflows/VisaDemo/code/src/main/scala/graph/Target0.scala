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

@Visual(id = "Target0", label = "Target0", x = 853, y = 276, phase = 0)
object Target0 {

  @UsesDataset(id = "3377", version = 0)
  def apply(spark: SparkSession, in: DataFrame): Target = {
    import spark.implicits._

    Config.fabricName match {
      case "dev" =>
        val schemaArg = StructType(
          Array(
            StructField("customer_id", IntegerType, false),
            StructField("first_name",  StringType,  false),
            StructField("last_name",   StringType,  false),
            StructField("phone",       StringType,  false),
            StructField("orders",      LongType,    false),
            StructField("amounts",     DoubleType,  false)
          )
        )
        in.write
          .format("parquet")
          .mode("overwrite")
          .save("dbfs:/DatabricksSession/Report.parqq")
      case _ => throw new Exception("Unknown Fabric")
    }

  }

}
