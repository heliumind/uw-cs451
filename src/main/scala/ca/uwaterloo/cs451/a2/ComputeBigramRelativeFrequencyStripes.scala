package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class PartitionerStripes(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts
  override def getPartition(key: Any): Int = {
    val pair = key.asInstanceOf[(String)]
    (pair.hashCode() & Integer.MAX_VALUE) % numPartitions
  }
  override def equals(other: Any): Boolean = other match {
    case partitioner: PartitionerStripes =>
      partitioner.numPartitions == numPartitions
    case _ =>
      false
  }
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new Conf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Stripes")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1) tokens.sliding(2).toList.map(p => {
          (p(0), Map[String,Int](p(1) -> 1))
        }) else List()
      })
      // plus the two stripes a.k.a map.plus in lintools
      .reduceByKey((stripe1, stripe2) => stripe1 ++ stripe2.map{ case (k, v) => k -> (v + stripe1.getOrElse(k, 0)) })
      .partitionBy(new PartitionerStripes(args.reducers()))
      .mapPartitions(partition => {
        partition.map(pair => {
          var sum = 0.0f
          pair._2.foreach{ case (_, v) => sum += v }
          val stripe = pair._2.map{ case (k, v) => (k, v / sum) }
          (pair._1, stripe)
        })
      })

    counts.saveAsTextFile(args.output())
  }
}
