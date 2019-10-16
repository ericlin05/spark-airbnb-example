package com.example.airbnb

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object TopNeighbourhood {
    def main(args: Array[String]) {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val spark = SparkSession.builder
        .appName("Top Neighbourhood in Melbourne")
        .master("local[2]")
        .getOrCreate()

      val listing = spark.read
        .option("header", true)
        .option("inferSchema", true)
        .option("multiLine", true)
        .csv("data/in/listings_dec18.csv")
        .filter("neighbourhood IS NOT NULL")
        .filter("length(neighbourhood) > 2")
        .filter("neighbourhood NOT LIKE 'http%'")
        .select("id", "neighbourhood")

      val calendar_listing = spark.read
        .option("header", true)
        .option("inferSchema", false)
        .csv("data/in/calendar_dec18.csv")
        .filter("available == 't'")
        .filter("date LIKE '2019%'")

      calendar_listing.createOrReplaceTempView("calendar_listing")
      listing.createOrReplaceTempView("listing")

      val result = spark.sql(
        "SELECT neighbourhood, COUNT(*) AS cnt " +
          "FROM listing l " +
          "JOIN calendar_listing cl ON (l.id = cl.listing_id) " +
          "GROUP BY neighbourhood " +
          "ORDER BY cnt DESC"
      )

      result.show(false)
    }
  }
