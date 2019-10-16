package com.example

import com.example.airbnb.TopNeighbourhood
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("example").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("Spark Example Application")
      .master("local[2]")
      .getOrCreate()


    val task = new  TopNeighbourhood(
      spark,
      "data/in/listings_dec18.csv",
      "data/in/calendar_dec18.csv"
    )

    task.run()
  }
}