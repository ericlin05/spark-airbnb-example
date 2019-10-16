package com.example.airbnb

import org.apache.spark.sql.SparkSession

class TopNeighbourhood(
                        spark: SparkSession,
                        list_source: String,
                        availability_source: String
                      ) {

  val HOUSE_LIST_SOURCE = list_source
  val HOUSE_AVAILABILITY_SOURCE = availability_source

  def run() {
    val listing = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("multiLine", true)
      .csv(list_source)
      .filter("neighbourhood IS NOT NULL")
      .filter("length(neighbourhood) > 2")
      .filter("neighbourhood NOT LIKE 'http%'")
      .select("id", "neighbourhood")

    val calendar_listing = spark.read
      .option("header", true)
      .option("inferSchema", false)
      .csv(availability_source)
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
