package com.example

import com.example.airline.{DomesticAirlineOnTimePerformanceDF, DomesticAirlineOnTimePerformanceDS, DomesticAirlineOnTimePerformanceRDD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object Main {
  val LIMIT = 100
  val APP_NAME = "Spark Example Application"

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    // default to test DomesticAirlineOnTimePerformanceDF
    var className = "DomesticAirlineOnTimePerformanceDF"
    if(args.length >= 1) {
      className = args(0)
    }

    var master = "local[2]"
    if(args.length >= 2) {
      master = args(1)
    }

    var csv_file = "data/in/au-domestic-airlines/otp_time_series_web.csv"
    if(args.length >= 3) {
      csv_file = args(2)
    }

    // This is needed for RDD operations
    val conf = new SparkConf().setAppName(APP_NAME).setMaster(master)
    val sc = new SparkContext(conf)

    // This is needed for Dataframe or Dataset operations
    val spark = SparkSession.builder
      .appName(APP_NAME)
      .master(master)
      .getOrCreate()

    val task:SparkTask = className match {
      case "DomesticAirlineOnTimePerformanceDS" =>
        new DomesticAirlineOnTimePerformanceDS(spark, csv_file)
      case "DomesticAirlineOnTimePerformanceDF" =>
        new DomesticAirlineOnTimePerformanceDF(spark, csv_file)
      case "DomesticAirlineOnTimePerformanceRDD" =>
        new DomesticAirlineOnTimePerformanceRDD(sc, csv_file)
    }

    task.run()
  }
}
