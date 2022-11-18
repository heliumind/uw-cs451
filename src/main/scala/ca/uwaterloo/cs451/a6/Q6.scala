package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q6 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")

      lineitemRDD
        .filter(line => {
          line.split("\\|")(10).equals(date)
        })
        .map(line => {
          val tokens = line.split("\\|")
          val quantity = tokens(4).toDouble
          val baseprice = tokens(5).toDouble
          val discount = tokens(6).toDouble
          val tax = tokens(7).toDouble
          val discprice = baseprice * (1 - discount)
          val charge = baseprice * (1 - discount) * (1 + tax)
          val returnflag = tokens(8)
          val linestatus = tokens(9)
          (
            (returnflag, linestatus),
            (quantity, baseprice, discprice, charge, discount, 1)
          )
        })
        .reduceByKey((x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6)
        )
        .sortByKey()
        .map{ case (
          (returnflag, linestatus),
          (sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_disc, count)
          ) =>
          val avg_qty = sum_qty / count
          val avg_price = sum_base_price / count
          val avg_disc = sum_disc / count
          (
            returnflag,
            linestatus,
            sum_qty,
            sum_base_price,
            sum_disc_price,
            sum_charge,
            avg_qty,
            avg_price,
            avg_disc,
            count
          )
        }
        .collect()
        .foreach(println)
        }

    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd

      lineitemRDD
        .filter(row => {
          row(10).toString.equals(date)
        })
        .map(row => {
          val quantity = row(4).toString.toDouble
          val baseprice = row(5).toString.toDouble
          val discount = row(6).toString.toDouble
          val tax = row(7).toString.toDouble
          val discprice = baseprice * (1 - discount)
          val charge = baseprice * (1 - discount) * (1 + tax)
          val returnflag = row(8).toString
          val linestatus = row(9).toString
          (
            (returnflag, linestatus),
            (quantity, baseprice, discprice, charge, discount, 1)
          )
        })
        .reduceByKey((x, y)=>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6)
        )
        .sortByKey()
        .map { case (
          (returnflag, linestatus),
          (sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_disc, count)
          ) =>
          val avg_qty = sum_qty / count
          val avg_price = sum_base_price / count
          val avg_disc = sum_disc / count
          (
            returnflag,
            linestatus,
            sum_qty,
            sum_base_price,
            sum_disc_price,
            sum_charge,
            avg_qty,
            avg_price,
            avg_disc,
            count
          )
        }
        .collect()
        .foreach(println)
    }

  }

}