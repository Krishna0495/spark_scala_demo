package org.scala.com

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

case class Flights(passengerId: String, Flight: collection.mutable.Seq[String], longestRun: Int)

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    val conf = new SparkConf().setMaster("local").setAppName("testApp")

    val sc = new SparkContext(conf)

    val sqlContext= new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val spark = SparkSession.builder
      .master("local")
      .appName("App")
      .getOrCreate()


    val df1 = spark.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .load("input/data/flightData.csv")

    df1.createOrReplaceTempView("flight")

//    var df1 = df1.select("passengerID",concat(col("from"),",",col("to")))
    df1.show(10)

    val df2 = spark.sql(
      """select passengerId, collect_list(concat(from,',',to)) as Flight from
        |flight
        |group by passengerId
        |""".stripMargin)

    def longestRun(sq: collection.mutable.Seq[String]): Int = {
      sq.mkString(" ")
        .split("UK")
        .filter(_.nonEmpty)
        .map(_.trim)
        .map(s => s.split(" ").length)
        .max
    }


    println(df2.schema)

    df2.show(10)

    val df = df2.map(r => Flights(r(0).asInstanceOf[String], r(1).asInstanceOf[collection.mutable.Seq[String]], longestRun(r(1).asInstanceOf[collection.mutable.Seq[String]])))

    df.show(10)

  }
}