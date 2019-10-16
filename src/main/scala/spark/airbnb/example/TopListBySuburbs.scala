package spark.airbnb.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.year

object TopListBySuburbs {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .appName("TopListBySuburb")
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

    listing.printSchema()
    listing.show

    calendar_listing.printSchema()
    calendar_listing.show

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
