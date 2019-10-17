package com.example.airline

import com.example.SparkTask
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

class DomesticAirlineOnTimePerformanceDS(spark: SparkSession, source: String, limit: Int = 100)
  extends SparkTask {

  val SOURCE = source

  def run(): Unit = {
    val ds = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(source)
      .select("Year",
        "Airline",
        "Route",
        "Sectors_Scheduled",
        "Sectors_Flown",
        "Departures_On_Time",
        "Arrivals_On_Time"
      ) // This will return a Dataset, not Dataframe

    for(column <- List("Route", "Airline")) {
      println("=============================")
      println("Rates by " + column + "s")
      generate(ds, column).show(limit, false)
    }
  }

  def generate[T](ds: Dataset[T], column: String): DataFrame = {
    val aggDS = ds.groupBy("Year", column)
      .agg(
        sum("Sectors_Scheduled").alias("Sectors_Scheduled_Total"),
        sum("Sectors_Flown").alias("Sectors_Flown_Total"),
        sum("Departures_On_Time").alias("Departures_On_Time_Total"),
        sum("Arrivals_On_Time").alias("Arrivals_On_Time_Total")
      )

    val result = aggDS
      .withColumn(
        "Service_Rate",
        concat(
          round(aggDS.col("Sectors_Flown_Total")/aggDS.col("Sectors_Scheduled_Total")*100, 2),
          lit("%")
        )
      )
      .withColumn(
        "Departure_On_Time_Rate",
        concat(
          round(aggDS.col("Departures_On_Time_Total")/aggDS.col("Sectors_Scheduled_Total")*100, 2),
          lit("%")
        )
      )
      .withColumn(
        "Arrival_On_Time_Rate",
        concat(
          round(aggDS.col("Arrivals_On_Time_Total")/aggDS.col("Sectors_Scheduled_Total")*100, 2),
          lit("%")
        )
      )
      .select(
        "Year",
        column,
        "Service_Rate",
        "Departure_On_Time_Rate",
        "Arrival_On_Time_Rate"
      )

      result.orderBy(
        result.col("Year").desc,
        result.col("Service_Rate").desc
      )
  }
}
