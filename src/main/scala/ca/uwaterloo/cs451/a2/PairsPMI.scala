package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class ConfPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers, threshold)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  val threshold = opt[Int](descr = "minimum co-occurence count", required = false, default = Some(1))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PMI Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input(), args.reducers())
    val numLines = textFile.count()
    val MaxNumWords = 40

    val wordCount = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 0) tokens.take(Math.min(MaxNumWords, tokens.length)).distinct
          else List()
      })
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .collectAsMap()

    val broadcastWcMap = sc.broadcast(wordCount)
    val broadcastThreshold = sc.broadcast(args.threshold())
    val broadcastNumLines = sc.broadcast(numLines)

    val pmi = textFile
      // Generate Pairs
      .flatMap(line => {
        val tokens = tokenize(line)
        val tokensDedup = tokens.take(Math.min(MaxNumWords, tokens.length)).distinct
        if (tokensDedup.length > 1)
          tokensDedup.flatMap(prev => {
            tokensDedup.flatMap(cur => if (!cur.equals(prev)) List((prev, cur)) else List())
          })
        else
          List()
      })
      // Add count
      .map(pair => (pair, 1))
      .reduceByKey(_ + _)
      // Check threshold
      .filter{ case (_, count) => count > broadcastThreshold.value}
      .mapPartitions(partition => {
        // Calculate PMI
        partition.map{ case ((x, y), xyCount) =>
          val xCount = broadcastWcMap.value(x).toDouble
          val yCount = broadcastWcMap.value(y).toDouble
          val N = broadcastNumLines.value.toDouble
          val numerator = xyCount.toDouble / N
          val denominator = (xCount / N) * (yCount / N)
          val pmi = Math.log10(numerator / denominator)
          ((x, y), (pmi, xyCount))
        }
      })

    pmi.saveAsTextFile(args.output())
  }
}
