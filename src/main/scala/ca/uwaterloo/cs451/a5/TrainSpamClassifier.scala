package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable._

class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val model = opt[String](descr = "output trained model", required = true)
  verify()
}
object TrainSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new TrainSpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("PMI Pairs")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.model())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    // w is the weight vector (make sure the variable is within scope)
    val w = Map[Int, Double]()
    // This is the main learner:
    val delta = 0.002

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    val trainingSet = sc.textFile(args.input(), 1)
      .map(line => {
        val tokens = line.split(" ")
        val docid = tokens(0)
        val isSpam = if (tokens(1).trim().equals("spam")) 1 else 0
        val features = tokens.slice(2, tokens.length).map(f => f.toInt)
        (0, (docid, isSpam, features))
      })

    val trained = trainingSet
      .groupByKey(1)
      .flatMap(pair => {
        val samples = pair._2

        samples.foreach(sample => {
          val docid = sample._1
          val isSpam = sample._2
          val features = sample._3
          val score = spamminess(features)
          val prob = 1.0 / (1 + Math.exp(-score))
          features.foreach(f => {
            val updatedWeight = w.getOrElse(f, 0d) + (isSpam - prob) * delta
            w(f) = updatedWeight
          })
        })
        w
      })

    trained.saveAsTextFile(args.model())
  }

}
