package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q2 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q2Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q2")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val ordersRDD = sc.textFile(args.input() + "/orders.tbl")
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")

      val orders = ordersRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(6))
        })

      val lineitem = lineitemRDD
        .map(line => {
          val tokens = line.split("\\|")
          (tokens(0), tokens(10))
        })
        .filter{
          case(_, shipdate) => shipdate.equals(date)
        }

      lineitem.cogroup(orders)
        // Check if join key has multiple values
        .filter(pair => pair._2._1.iterator.hasNext)
        // Create joined tuples
        .flatMap { case (orderkey, (shipdates, clerks)) =>
          for (_ <- shipdates; clerk <- clerks) yield (clerk, orderkey.toInt)
        }
        .sortBy(_._2)
        .take(20)
        .foreach(println)

    }

    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")
      val ordersRDD = ordersDF.rdd

      val orders = ordersRDD
        .map(row => {
          (row(0).toString, row(6).toString)
        })

      val lineitem = lineitemRDD
        .map(row => {
          (row(0).toString, row(10).toString)
        })
        .filter {
          case (_, shipdate) => shipdate.equals(date)
        }

      lineitem.cogroup(orders)
        // Check if join key has multiple values
        .filter(pair => pair._2._1.iterator.hasNext)
        // Create joined tuples
        .flatMap { case (orderkey, (shipdates, clerks)) =>
          for (_ <- shipdates; clerk <- clerks) yield (clerk, orderkey.toInt)
        }
        .sortBy(_._2)
        .take(20)
        .foreach(println)

    }

  }
}
