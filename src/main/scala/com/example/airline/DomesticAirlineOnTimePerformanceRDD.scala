package com.example.airline

import com.example.SparkTask
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DomesticAirlineOnTimePerformanceRDD(sc: SparkContext, source: String, limit: Int = 100)
  extends SparkTask {

  val SOURCE = source

  def run(): Unit = {
    val rdd = sc.textFile(source)
        .filter(line => !line.startsWith("Route,Departing_Port")
          && !line.endsWith("Year,Month_Num")) // filter out the first line

    val types = Map(
      "Route" -> 12,
      "Airlines" -> 3
    )

    for((k, index) <- types) {
      println("=============================")
      println("Rates by " + k)

      for (element <- generate(rdd, index)) {
        println(element._1 + ":")
        println("Service_Rate: " + "%04.2f".format(element._2._2 / element._2._1 * 100) + "%")
        println("Departures_On_Time_Rate: " + "%04.2f".format(element._2._3 / element._2._1 * 100) + "%")
        println("Arrivals_On_Time_Rate: " + "%04.2f".format(element._2._4 / element._2._1 * 100) + "%")
        println("")
      }
    }
  }

  def generate(rdd: RDD[String], index: Int): RDD[(String, (Double, Double, Double, Double))] = {
    rdd
      .map(line => {
        val splits = line.split(",")
        // 0 => Route
        // 5 => Sectors_Scheduled
        // 6 => Sectors_Flown
        // 8 => Departures_On_Time
        // 9 => Arrivals_On_Time
        (splits(0) + " - " + splits(index), (splits(5).toDouble, splits(6).toDouble, splits(8).toDouble, splits(9).toDouble))
      })
      .reduceByKey((x, y) => {
        (
          x._1 + y._1,
          x._2 + y._2,
          x._3 + y._3,
          x._4 + y._4
        )
      }) // SUM all the values per entry
  }
}
