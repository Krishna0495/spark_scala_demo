package org.scala.com
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.SparkSession

case class Flight(passengerId: Int, flightId: Int, from: String, to: String, date: String)

case class Passengers(passengerId: Int, firstName: String, lastName: String)

object Main {
  def main(args: Array[String]): Unit = {

    println("Hello world!")

    val spark = SparkSession
      .builder()
      .appName("App")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val path_flight = "input/data/flightData.csv"

    val path_pass = "input/data/passengers.csv"

    val flight_df = spark.read.option("header","true").option("inferSchema","true")
      .csv(path_flight).as[Flight]
    //
    val pass_df = spark.read.option("header","true").option("inferSchema","true")
      .csv(path_pass).as[Passengers]

    flight_df.show(5)

    println("Flight Data Schema :",flight_df.schema)

    pass_df.show(5)

    println("pass_df Data Schema :", pass_df.schema)


    def num_of_flights_per_month(df: DataFrame):DataFrame = {

      var df1 = df.withColumn("Month",month(col("date")))

      df1 = df1.groupBy(col("Month")).agg(count("flightId")
        .as("Number_of_Flights"))
        .alias("Number_of_Flights_df").orderBy(col("Month"))

      return df1
    }

    num_of_flights_per_month(flight_df.toDF()).show(10)

    def n_most_frq_flyers(flight_df: DataFrame, passenger_df: DataFrame, n: Int): DataFrame = {

      val passenger_df_n = passenger_df.withColumnRenamed("passengerId", "pass_id")

      var res_df = flight_df.join(passenger_df_n,
        flight_df("passengerId")===passenger_df_n("pass_id"),"inner")

      res_df = res_df.groupBy("passengerId","firstName","lastName")
        .agg(count("flightId").as("Number_of_flights"))
        .orderBy(col("Number_of_flights").desc).limit(n)

      res_df = res_df.select("passengerId","Number_of_flights","firstName","lastName")

      return res_df
    }
    n_most_frq_flyers(flight_df.toDF(),pass_df.toDF(),100).show(10)

    def longest_seq_flight (flight_df:DataFrame,country:String):DataFrame = {
      flight_df.createOrReplaceTempView("flight")
      var res_df = spark.sql(
        """
          |select passengerId,collect_list(concat(upper(from),",",upper(to))) as flights
          |from flight
          |group by passengerId
          |""".stripMargin)
      //      var res_df = flight_df.withColumn("concat_to_fro",concat(col("from"),",",col("to")))
      //        .groupBy("passengerId")
      //        .agg(collect_list(col("concat_to_fro")).as("flights"))

      val s = " *"+country+" *"

      val longest_seq = udf((x: Seq[String]) => {
        x.reduce(_ + " " + _)
          .split(s)
          .map(_.count(_ == ' ') + 1)
          .max
      })

      res_df = res_df.withColumn("longest_run",longest_seq(col("flights")))

      return res_df

    }

    longest_seq_flight(flight_df.toDF(),"UK").show(10)


    //    println(longest_seq_flight(flight_df.toDF(),"UK").filter("passengerId=385").show())

    def n_least_flights_flow_together(flight_df: DataFrame,n: Int):DataFrame = {

      //      val flight_df_self = flight_df
      //        .withColumnRenamed("passengerId","new_pass_id")
      //        .withColumnRenamed("flightId","new_flightId")
      //        .withColumnRenamed("from","new_from")
      //        .withColumnRenamed("to","new_to")
      //        .withColumnRenamed("date","new_date")

      flight_df.createOrReplaceTempView("flight")

      var res_df = spark.sql(
        """
          |select *
          |from
          |(
          |select
          |a.passengerId as Passenger_1_ID,
          |b.passengerId as Passenger_2_ID,
          |count(distinct(a.flightId)) as number_of_flights_together,
          |ROW_NUMBER() over(partition by
          |case when a.passengerId<b.passengerId then a.passengerId else b.passengerId end,
          |case when a.passengerId>b.passengerId then a.passengerId else b.passengerId end
          |order by a.passengerId) as rn
          |from flight a
          |join flight b
          |on a.passengerId<>b.passengerId
          |and a.flightId=b.flightId
          |group by a.passengerId,
          |b.passengerId) imp
          |where imp.rn=1
          |""".stripMargin
      )
      res_df = res_df.filter("number_of_flights_together >= "+n)

      return res_df
    }

    n_least_flights_flow_together(flight_df.toDF(),3).filter("Passenger_1_ID in (791,729) and Passenger_2_ID in (791,729)").show()

    def n_least_flights_flow_together_time(flight_df: DataFrame, n: Int, f:String, to:String): DataFrame = {


      val flight_df_new = flight_df.withColumn("date_new", to_date(col("date"), "yyyy-MM-dd"))

      println("Flight Data Schema :", flight_df_new.schema)

      flight_df_new.createOrReplaceTempView("flight")

      var res_df = spark.sql(
        """select * from
          (select
          a.passengerId as Passenger_1_ID,
          b.passengerId as Passenger_2_ID,
          a.date_new as date,
          count(distinct(a.flightId)) as number_of_flights_together,
          ROW_NUMBER() over(partition by
          case when a.passengerId<b.passengerId then a.passengerId else b.passengerId end,
          case when a.passengerId>b.passengerId then a.passengerId else b.passengerId end
          order by a.passengerId) as rn
          from flight a
          join flight b
          on a.passengerId<>b.passengerId
          and a.flightId=b.flightId
          group by a.passengerId,
          b.passengerId,a.date_new) imp
          where imp.rn=1
          and imp.date>="""" + f + """" and date<="""" + to + """"
          """)

      res_df = res_df.filter("""number_of_flights_together >= """ + n
        + """ and date>=""""+ f + """" and date<="""" + to + """" """)

      return res_df
    }

    n_least_flights_flow_together_time(flight_df.toDF(),3,"2000-01-01","2022-01-01").filter("Passenger_1_ID in (791,729) and Passenger_2_ID in (791,729)").show()

  }
}