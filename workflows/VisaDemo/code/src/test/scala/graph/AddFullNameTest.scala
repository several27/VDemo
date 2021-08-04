package graph

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import io.prophecy.libs.SparkTestingUtils._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.junit.Assert
import org.scalatest.FunSuite
import java.time.LocalDateTime
import org.scalatest.junit.JUnitRunner
import java.sql.{Date, Timestamp}

@RunWith(classOf[JUnitRunner])
class AddFullNameTest extends FunSuite with DataFrameSuiteBase {
  import sqlContext.implicits._

  test("Test Unit Test for out columns: first_name, last_name, full_name") {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    val dfIn = inDf(Seq("first_name", "last_name"), Seq(
      Seq[Any]("Waite","Petschelt"),
      Seq[Any]("Waite","Petschelt"),
      Seq[Any]("Constance","Sleith"),
      Seq[Any]("Viva","Schulke"),
      Seq[Any]("Barrett","Amies"),
      Seq[Any]("Kit","Skamell"),
      Seq[Any]("Viva","Schulke"),
      Seq[Any]("Gillan","Heritege"),
      Seq[Any]("Ogdan","Bussetti"),
      Seq[Any]("Homer","Lindstedt")
    ))

    val dfOut = outDf(Seq("first_name", "last_name", "full_name"), Seq(
      Seq[Any]("Waite","Petschelt","Waite - Petschelt"),
      Seq[Any]("Waite","Petschelt","Waite - Petschelt"),
      Seq[Any]("Constance","Sleith","Constance - Sleith"),
      Seq[Any]("Viva","Schulke","Viva - Schulke"),
      Seq[Any]("Barrett","Amies","Barrett - Amies"),
      Seq[Any]("Kit","Skamell","Kit - Skamell"),
      Seq[Any]("Viva","Schulke","Viva - Schulke"),
      Seq[Any]("Gillan","Heritege","Gillan - Heritege"),
      Seq[Any]("Ogdan","Bussetti","Ogdan - Bussetti"),
      Seq[Any]("Homer","Lindstedt","Homer - Lindstedt")
    ))

    val dfOutComputed = graph.AddFullName(spark, dfIn)
    val res = assertDFEquals(dfOut.select("first_name", "last_name", "full_name"), dfOutComputed.select("first_name", "last_name", "full_name"), maxUnequalRowsToShow, 1.0)

    val msg = if (res.isLeft) res.left.get.getMessage else ""
    Assert.assertTrue(msg, res.isRight)
    
  }

  def inDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "first_name" -> "",
      "last_name" -> "",
      "amount" -> "",
      "customer_id" -> "",
      "order_date" -> "",
      "phone" -> "",
      "order_id" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "first_name" -> StringType,
      "last_name" -> StringType,
      "amount" -> StringType,
      "customer_id" -> StringType,
      "order_date" -> StringType,
      "phone" -> StringType,
      "order_id" -> StringType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "in")
  }

  def outDf(columns: Seq[String], values: Seq[Seq[Any]]): DataFrame = {
    implicit def addDTMethods(s: String): StringColumnExtensions  = new StringColumnExtensions(s)

    // defaults for each column
    val defaults = Map[String, Any](
      "customer_id" -> "",
      "first_name" -> "",
      "last_name" -> "",
      "phone" -> "",
      "order_id" -> "",
      "amount" -> "",
      "order_date" -> "",
      "full_name" -> ""
    )

    // column types for each column
    val columnToTypeMap = Map[String, DataType](
      "customer_id" -> StringType,
      "first_name" -> StringType,
      "last_name" -> StringType,
      "phone" -> StringType,
      "order_id" -> StringType,
      "amount" -> StringType,
      "order_date" -> StringType,
      "full_name" -> StringType
    )

    createDF(spark, columns, values, defaults, columnToTypeMap, "out")
  }

  def assertPredicates(port: String, df: DataFrame, predicates: Seq[(Column, String)]): Unit = {
    predicates.foreach({
      case (pred, name) =>
        Assert.assertEquals(
          s"Predicate $name [[`$pred`]] not universally true for port $port",
          df.filter(pred).count(),
          df.count()
        )
    })
  }
}
