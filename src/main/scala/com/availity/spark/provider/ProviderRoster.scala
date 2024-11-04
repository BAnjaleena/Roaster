package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, month, year}
import org.apache.spark.sql.types.{DateType, StringType, StructType}

object ProviderRoster {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Provider Roster")
      .master("local[*]")
      .getOrCreate()

    val providersPath = "data/providers.csv"
    val visitsPath = "data/visits.csv"
    val outputPath = "output/"

    // Load data from CSV files
    val providersDF = loadProviders(spark, providersPath)
    val visitsDF = loadVisits(spark, visitsPath)

    // Calculate total visits and monthly visits
    val totalVisitsDF = calculateTotalVisitsPerProvider(providersDF, visitsDF)
    val monthlyVisitsDF = calculateVisitsPerProviderPerMonth(visitsDF)

    // Write JSON for total visits
    writeJsonPerSpecialty(totalVisitsDF, outputPath + "total_visits/")

    // Write JSON for monthly visits
    writeJsonPerProviderPerMonth(monthlyVisitsDF, outputPath + "monthly_visits/")

    spark.stop()
  }

  def loadProviders(spark: SparkSession, path: String): DataFrame = {
    val schema = new StructType()
      .add("provider_id", StringType, nullable = false)
      .add("provider_specialty", StringType, nullable = true)
      .add("first_name", StringType, nullable = true)
      .add("middle_name", StringType, nullable = true)
      .add("last_name", StringType, nullable = true)

    spark.read
      .option("header", "true")
      .option("delimiter", "|") // Assuming '|' as the delimiter for providers
      .schema(schema)
      .csv(path)
  }

  def loadVisits(spark: SparkSession, path: String): DataFrame = {
    val schema = new StructType()
      .add("visit_id", StringType, nullable = false)
      .add("provider_id", StringType, nullable = false)
      .add("date_of_service", DateType, nullable = true)

    spark.read
      .option("header", "false") // Assuming there is no header in visits.csv
      .schema(schema)
      .csv(path)
  }

  def calculateTotalVisitsPerProvider(providersDF: DataFrame, visitsDF: DataFrame): DataFrame = {
    // Aggregate total visits per provider
    val totalVisits = visitsDF
      .groupBy("provider_id")
      .agg(count("visit_id").as("total_visits"))

    // Join with providersDF to get provider details, including specialty
    val totalVisitsWithDetails = totalVisits
      .join(providersDF, "provider_id")
      .select(
        col("provider_id"),
        col("first_name"),
        col("last_name"),
        col("provider_specialty"), // Ensure provider_specialty is included
        col("total_visits")
      )

    totalVisitsWithDetails
  }

  def calculateVisitsPerProviderPerMonth(visitsDF: DataFrame): DataFrame = {
    visitsDF
      .withColumn("year", year(col("date_of_service")))
      .withColumn("month", month(col("date_of_service")))
      .groupBy("provider_id", "month")
      .agg(count("visit_id").alias("monthly_visits"))
      .select("provider_id", "month", "monthly_visits")
  }

  def writeJsonPerSpecialty(df: DataFrame, outputPath: String): Unit = {
    df
      .repartition(1, col("provider_specialty")) // Single file per specialty folder
      .write
      .partitionBy("provider_specialty")
      .mode("overwrite")
      .json(outputPath)
  }

  def writeJsonPerProviderPerMonth(df: DataFrame, outputPath: String): Unit = {
    // Create a DataFrame with the necessary columns
    val monthlyVisitsData = df.select("provider_id", "month", "monthly_visits")

    // Iterate through each unique provider_id and month combination
    monthlyVisitsData
      .distinct()
      .collect()
      .foreach { row =>
        val providerId = row.getString(0)
        val month = row.getInt(1)

        // Filter the DataFrame for the current provider and month
        val filteredDF = monthlyVisitsData
          .filter(col("provider_id") === providerId && col("month") === month)

        // Construct the output path for the current provider and month
        val outputDir = s"$outputPath/provider_$providerId/month_$month"

        // Write the filtered DataFrame to JSON
        filteredDF
          .coalesce(1) // Ensure we write to a single file
          .write
          .mode("overwrite")
          .json(outputDir)
      }
  }
}