package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  verify()
}

class PartitionerPairs(numPartions: Int) extends Partitioner {
  override def numPartitions: Int = numPartions
  override def getPartition(key: Any): Int = {
    val pair = key.asInstanceOf[(String, String)]
    (pair._1.hashCode() & Integer.MAX_VALUE) % numPartions
  }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).map(p => List( (p(0), p(1)), (p(0), "*") )).flatten else List()
      })
      .map(bigram => (bigram, 1))
      .reduceByKey(_ + _)
      .repartitionAndSortWithinPartitions(new PartitionerPairs(args.reducers()))
      .mapPartitions(partition => {
        var marginal = 0.0f
        partition.map(pair => {
          if (pair._1._2.equals("*")) {
            val count = pair._2.toFloat
            marginal = count
            (pair._1, count)
          } else {
            val relFreq = pair._2.toFloat / marginal
            (pair._1, relFreq)
          }
        })
      })

    counts.saveAsTextFile(args.output())
  }
}
