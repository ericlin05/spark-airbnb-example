package com.example.airline

import com.example.SparkTask
import org.apache.spark.sql.SparkSession

class DomesticAirlineOnTimePerformanceDF(spark: SparkSession, source: String, limit: Int = 100)
  extends SparkTask {

  val SOURCE = source

  def run(): Unit = {
    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv(source)
      .createOrReplaceTempView("schedules")

    println("=============================")
    println("Rates by Routes")

    spark.sql("SELECT Year, Route, " +
      "CONCAT('', CAST(SUM(Sectors_Flown)/SUM(Sectors_Scheduled) * 100 AS DECIMAL(4,2)), '%') AS Service_Rate, " +
      "CONCAT('', CAST(SUM(Departures_On_Time)/SUM(Sectors_Scheduled) * 100 AS DECIMAL(4,2)), '%') AS Departures_On_Time_Rate, " +
      "CONCAT('', CAST(SUM(Arrivals_On_Time)/SUM(Sectors_Scheduled) * 100 AS DECIMAL(4,2)), '%') AS Arrivals_On_Time_Rate " +
      "FROM schedules " +
      "GROUP BY Year, Route " +
      "ORDER BY Year DESC, Service_Rate DESC"
    ).show(limit,false)

    println("=============================")
    println("Rates by Airlines")

    spark.sql("SELECT Year, Airline, " +
      "CONCAT('', CAST(SUM(Sectors_Flown)/SUM(Sectors_Scheduled) * 100 AS DECIMAL(4,2)), '%') AS Service_Rate, " +
      "CONCAT('', CAST(SUM(Departures_On_Time)/SUM(Sectors_Scheduled) * 100 AS DECIMAL(4,2)), '%') AS Departures_On_Time_Rate, " +
      "CONCAT('', CAST(SUM(Arrivals_On_Time)/SUM(Sectors_Scheduled) * 100 AS DECIMAL(4,2)), '%') AS Arrivals_On_Time_Rate " +
      "FROM schedules " +
      "GROUP BY Year, Airline " +
      "ORDER BY Year DESC, Service_Rate DESC"
    ).show(limit,false)
  }
}
