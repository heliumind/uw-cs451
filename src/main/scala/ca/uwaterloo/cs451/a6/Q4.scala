package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q4 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q4Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val nationRDD = sc.textFile(args.input() + "/nation.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")

      val orders = ordersRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // o_orderkey
            tokens(1)  // o_custkey
          )
        })

      val lineitem = lineitemRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // l_orderkey
            tokens(10) // l_shipdate
          )
        })
        .filter {
          case (_, shipdate) => shipdate.equals(date)
        }

      val nation = nationRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // n_nationkey
            tokens(1)  // n_name
          )
        })
        .collectAsMap()

      val customer = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // c_custkey
            tokens(3)  // c_nationkey
          )
        })
        .collectAsMap()

      val nationMap = sc.broadcast(nation)
      val customerMap = sc.broadcast(customer)

      orders.cogroup(lineitem)
        .filter(pair => pair._2._2.iterator.hasNext)
        .flatMap { case (orderkey, (custkeys, shipdates)) =>
          custkeys.flatMap(custkey => shipdates.map(_ => (orderkey, custkey, 1)))
        }
        .map(pair => (customerMap.value(pair._2), pair._3))
        .reduceByKey(_ + _)
        .map(pair => (pair._1.toInt, nationMap.value(pair._1), pair._2))
        .sortBy(_._1)
        .collect()
        .foreach(println)
    }

    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val nationDF = sparkSession.read.parquet(args.input() + "/nation")
      val nationRDD = nationDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd

      val orders = ordersRDD
        .map(row => {
          (
            row(0).toString, // o_orderkey
            row(1).toString  // o_custkey
          )
        })

      val lineitem = lineitemRDD
        .map(row => {
          (
            row(0).toString, // l_orderkey
            row(10).toString // l_shipdate
          )
        })
        .filter {
          case (_, shipdate) => shipdate.equals(date)
        }

      val nation = nationRDD
        .map(row => {
          (
            row(0).toString, // n_nationkey
            row(1).toString  // n_name
          )
        })
        .collectAsMap()

      val customer = customerRDD
        .map(row => {
          (
            row(0).toString, // c_custkey
            row(3).toString  // c_nationkey
          )
        })
        .collectAsMap()

      val nationMap = sc.broadcast(nation)
      val customerMap = sc.broadcast(customer)

      orders.cogroup(lineitem)
        .filter(pair => pair._2._2.iterator.hasNext)
        .flatMap { case (orderkey, (custkeys, shipdates)) =>
          custkeys.flatMap(custkey => shipdates.map(_ => (orderkey, custkey, 1)))
        }
        .map(pair => (customerMap.value(pair._2), pair._3))
        .reduceByKey(_ + _)
        .map(pair => (pair._1.toInt, nationMap.value(pair._1), pair._2))
        .sortBy(_._1)
        .collect()
        .foreach(println)
    }

  }

}
