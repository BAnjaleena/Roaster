package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {

  val spark = SparkSession.builder().master("local").appName("ProviderRosterSpec").getOrCreate()

  import spark.implicits._

  override def afterEach(): Unit = {
    spark.catalog.clearCache()
  }

  describe("calculateTotalVisitsPerProvider") {
    it("calculates total visits per provider correctly") {
      val providersDF = Seq(
        ("47259", "Urology", "Bessie", "B", "Kuphal"),
        ("17847", "Cardiology", "Candice", "C", "Block")
      ).toDF("provider_id", "provider_specialty", "first_name", "middle_name", "last_name")

      val visitsDF = Seq(
        ("40838", "47259", "2022-08-23"),
        ("58362", "47259", "2022-03-25"),
        ("30441", "17847", "2022-07-12")
      ).toDF("visit_id", "provider_id", "date_of_service")
        .withColumn("date_of_service", col("date_of_service").cast("date"))

      // Calculate result DataFrame and cast total_visits to IntegerType
      val resultDF = ProviderRoster.calculateTotalVisitsPerProvider(providersDF, visitsDF)
        .withColumn("total_visits", col("total_visits").cast("int"))

      // Define expected DataFrame
      val expectedDF = Seq(
        ("47259", "Bessie", "Kuphal", "Urology", 2),
        ("17847", "Candice", "Block", "Cardiology", 1)
      ).toDF("provider_id", "first_name", "last_name", "provider_specialty", "total_visits")

      // Compare result and expected DataFrames
      assertSmallDataFrameEquality(resultDF, expectedDF, ignoreNullable = true)
    }
  }

  describe("calculateVisitsPerProviderPerMonth") {
    it("calculates monthly visits per provider correctly") {
      val visitsDF = Seq(
        ("1", "47259", "2022-08-23"),
        ("2", "47259", "2022-03-25"),
        ("3", "17847", "2022-07-12")
      ).toDF("visit_id", "provider_id", "date_of_service")
        .withColumn("date_of_service", col("date_of_service").cast("date"))

      // Call the method to get the result DataFrame
      val resultDF = ProviderRoster.calculateVisitsPerProviderPerMonth(visitsDF)

      // Define expected output correctly
      val expectedDF = Seq(
        ("47259", 3, 1L), // March visit
        ("47259", 8, 1L), // August visit
        ("17847", 7, 1L) // July visit
      ).toDF("provider_id", "month", "monthly_visits")

      // Sort DataFrames to ensure consistent ordering
      val sortedResultDF = resultDF.orderBy("provider_id", "month")
      val sortedExpectedDF = expectedDF.orderBy("provider_id", "month")

      // Compare results
      assertSmallDataFrameEquality(sortedResultDF, sortedExpectedDF, ignoreNullable = true)
    }
  }
}