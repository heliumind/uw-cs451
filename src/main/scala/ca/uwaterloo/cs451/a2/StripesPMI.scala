package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

object StripesPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("PMI Stripes")
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
      // Generate pairs
      .flatMap(line => {
        val tokens = tokenize(line)
        val tokensDedup = tokens.take(Math.min(MaxNumWords, tokens.length)).distinct
        if (tokensDedup.length > 1)
          tokensDedup.flatMap(prev => {
            tokensDedup.flatMap(cur =>
              if (!cur.equals(prev))
                List((prev, Map[String, Int](cur -> 1)))
              else
                List())
          })
        else
          List()
      })
      .reduceByKey((stripe1, stripe2) => stripe1 ++ stripe2.map{ case (k, v) => k -> (v + stripe1.getOrElse(k, 0)) })
      .mapPartitions(partition => {
        // Calculate PMI
        partition.map{ case (x, stripe) =>
          val stripePMI = stripe.map{ case (y, xyCount) =>
            val xCount = broadcastWcMap.value(x).toDouble
            val yCount = broadcastWcMap.value(y).toDouble
            val N = broadcastNumLines.value.toDouble
            val numerator = xyCount.toDouble / N
            val denominator = (xCount / N) * (yCount / N)
            val pmi = Math.log10(numerator / denominator)
            (y, (pmi, xyCount))
          }
          (x, stripePMI)
        }
      })

    pmi.saveAsTextFile(args.output())
  }
}
