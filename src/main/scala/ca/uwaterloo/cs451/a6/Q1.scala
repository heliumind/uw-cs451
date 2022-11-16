package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q1 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q1Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q1")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val results = sc.textFile(args.input() + "/lineitem.tbl")
        .filter(line => {
          val tokens = line.split("\\|")
          tokens(10).startsWith(date)
        })
        .count()
      println("ANSWER=" + results)
    }

    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd

      val results = lineitemRDD
        .filter(row => {
          row(10).toString.startsWith(date)
        })
        .count()
      println("ANSWER=" + results)
    }
  }

}
