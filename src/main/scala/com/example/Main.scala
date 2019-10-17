package com.example

import com.example.airbnb.TopNeighbourhood
import com.example.airline.{DomesticAirlineOnTimePerformanceDF, DomesticAirlineOnTimePerformanceDS, DomesticAirlineOnTimePerformanceRDD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {
  val LIMIT = 100
  val AIRLINE_CSV_FILE = "data/in/au-domestic-airlines/otp_time_series_web.csv"
  val APP_NAME = "Spark Example Application"
  val MASTER = "local[2]"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // This is needed for RDD operations
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(MASTER)
    val sc = new SparkContext(conf)

    // This is needed for Dataframe or Dataset operations
    val spark = SparkSession.builder
      .appName(APP_NAME)
      .master(MASTER)
      .getOrCreate()

    val className = "DomesticAirlineOnTimePerformanceRDD"
    val task:SparkTask = className match {
      case "DomesticAirlineOnTimePerformanceDS" =>
        new DomesticAirlineOnTimePerformanceDS(spark, AIRLINE_CSV_FILE)
      case "DomesticAirlineOnTimePerformanceDF" =>
        new DomesticAirlineOnTimePerformanceDF(spark, AIRLINE_CSV_FILE)
      case "DomesticAirlineOnTimePerformanceRDD" =>
        new DomesticAirlineOnTimePerformanceRDD(sc, AIRLINE_CSV_FILE)
    }

    task.run()
  }
}
