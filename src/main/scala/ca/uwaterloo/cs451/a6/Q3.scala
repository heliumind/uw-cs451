package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input = opt[String](descr = "input path", required = true)
  val date = opt[String](descr = "date", required = true)
  val text = opt[Boolean]()
  val parquet = opt[Boolean]()
  verify()
}

object Q3 {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Q3Conf(argv)

    log.info("Input: " + args.input())
    log.info("Date: " + args.date())
    log.info("Text: " + args.text())
    log.info("parquet: " + args.parquet())

    val conf = new SparkConf().setAppName("Q3")
    val sc = new SparkContext(conf)
    val date = args.date()

    if (args.text()) {
      val lineitemRDD = sc.textFile(args.input() + "/lineitem.tbl")
      val partRDD = sc.textFile(args.input() + "/part.tbl")
      val supplierRDD = sc.textFile(args.input() + "/supplier.tbl")

      val lineitem = lineitemRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // l_orderkey
            tokens(1), // l_partkey
            tokens(2), // l_suppkey
            tokens(10)  // l_shipdate
          )
        })
        .filter{
          case(_, _, _, shipdate) => shipdate.contains(date)
        }

      val part = partRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // p_partkey
            tokens(1) // p_name
          )
        })
        .collect().toMap

      val supplier = supplierRDD
        .map(line => {
          val tokens = line.split("\\|")
          (
            tokens(0), // s_suppkey
            tokens(1)  // s_name
          )
        })
        .collect().toMap

      val partMap = sc.broadcast(part)
      val supplierMap = sc.broadcast(supplier)

      val result = lineitem
        .filter{
          case(_, partkey, suppkey, _) =>
            partMap.value.contains(partkey) && supplierMap.value.contains(suppkey)
        }
        .map{
          case(l_orderkey, l_partkey, l_suppkey, _) => {
            val s_name = supplierMap.value.getOrElse(l_suppkey, "")
            val p_name = partMap.value.getOrElse(l_partkey, "")
            (l_orderkey.toInt, p_name, s_name)
          }
        }
        .sortBy(_._1)
        .take(20)
        .foreach(println)
    }

    if (args.parquet()) {
      val sparkSession = SparkSession.builder.getOrCreate

      val lineitemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val lineitemRDD = lineitemDF.rdd
      val partDF = sparkSession.read.parquet(args.input() + "/part")
      val partRDD = partDF.rdd
      val supplierDF = sparkSession.read.parquet(args.input() + "/supplier")
      val supplierRDD = supplierDF.rdd

      val lineitem = lineitemRDD
        .map(row => {
          (
            row(0).toString, // l_orderkey
            row(1).toString, // l_partkey
            row(2).toString, // l_suppkey
            row(10).toString // l_shipdate
          )
        })
        .filter {
          case (_, _, _, shipdate) => shipdate.contains(date)
        }

      val part = partRDD
        .map(row => {
          (
            row(0).toString, // p_partkey
            row(1).toString // p_name
          )
        })
        .collect().toMap

      val supplier = supplierRDD
        .map(row => {
          (
            row(0).toString, // s_suppkey
            row(1).toString // s_name
          )
        })
        .collect().toMap

      val partMap = sc.broadcast(part)
      val supplierMap = sc.broadcast(supplier)

      val result = lineitem
        .filter {
          case (_, partkey, suppkey, _) =>
            partMap.value.contains(partkey) && supplierMap.value.contains(suppkey)
        }
        .map {
          case (l_orderkey, l_partkey, l_suppkey, _) => {
            val s_name = supplierMap.value.getOrElse(l_suppkey, "")
            val p_name = partMap.value.getOrElse(l_partkey, "")
            (l_orderkey.toInt, p_name, s_name)
          }
        }
        .sortBy(_._1)
        .take(20)
        .foreach(println)

    }

  }
}
