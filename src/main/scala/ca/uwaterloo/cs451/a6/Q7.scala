package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q7 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q7")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val customerRDD = sc.textFile(args.input() + "/customer.tbl")

      val customer = customerRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // c_custkey
            tokens(1) // c_name
          )
        })
        .collectAsMap()

      val customerMap = sc.broadcast(customer)

      val lineitem = lineitemRDD
        .filter(line => line.split("\\|")(10) > date)
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // l_orderkey
            tokens(5).toDouble * (1 - tokens(6).toDouble) // revenue
          )
        })
        .reduceByKey(_ + _)

      val orders = ordersRDD
        .filter(line => line.split("\\|")(4) < date)
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // o_orderkey
            (
              customerMap.value(tokens(1)), // c_name
              tokens(4), // o_orderdate
              tokens(7) // o_shippriority
            )
          )
        })

      lineitem.cogroup(orders)
        .filter(pair => pair._2._1.iterator.hasNext && pair._2._2.iterator.hasNext)
        .map(pair => {
          val orders = pair._2._2.iterator.next()
          val name = orders._1
          val orderdate = orders._2
          val shippriority = orders._3
          val orderkey = pair._1
          val revenue = pair._2._1.iterator.next()
          (name, orderkey, revenue, orderdate, shippriority)
        })
        .sortBy(_._3, ascending=false)
        .take(5)
        .foreach(println)
    }

    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd
      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val customerDF = sparkSession.read.parquet(args.input() + "/customer")
      val customerRDD = customerDF.rdd

      val customer = customerRDD
        .map(row => {
          (
            row(0).toString, // c_custkey
            row(1).toString // c_name
          )
        })
        .collectAsMap()

      val customerMap = sc.broadcast(customer)

      val lineitem = lineitemRDD
        .filter(row => row(10).toString > date)
        .map(row => {
          (
            row(0).toString, // l_orderkey
            row(5).toString.toDouble * (1 - row(6).toString.toDouble) // revenue
          )
        })
        .reduceByKey(_ + _)

      val orders = ordersRDD
        .filter(row => row(4).toString < date)
        .map(row => {
          (
            row(0).toString, // o_orderkey
            (
              customerMap.value(row(1).toString), // c_name
              row(4).toString, // o_orderdate
              row(7).toString // o_shippriority
            )
          )
        })

      lineitem.cogroup(orders)
        .filter(pair => pair._2._1.iterator.hasNext && pair._2._2.iterator.hasNext)
        .map(pair => {
          val orders = pair._2._2.iterator.next()
          val name = orders._1
          val orderdate = orders._2
          val shippriority = orders._3
          val orderkey = pair._1
          val revenue = pair._2._1.iterator.next()
          (name, orderkey, revenue, orderdate, shippriority)
        })
        .sortBy(_._3, ascending = false)
        .take(5)
        .foreach(println)
    }
  }

}