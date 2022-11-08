package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "trained model", required = true)
  verify()
}

object ApplySpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplySpamClassifierConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())

    val conf = new SparkConf().setAppName("ApplySpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val model = args.model() + "/part-00000"

    val w = sc.textFile(model)
      .map(line => {
        val tokens = line.split(",")
        val weight = tokens(1).substring(0, tokens(1).length-1).toDouble
        val feature = tokens(0).substring(1).toInt
        (feature, weight)
      })
      .collect().toMap

    val weights = sc.broadcast(w)

    // Scores a document based on its list of features.
    def spamminess(features: Array[Int], w: Map[Int, Double]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    val results = sc.textFile(args.input())
      .map(line => {
        val tokens = line.split(" ")
        val docid = tokens(0)
        val label = tokens(1).trim()
        val features = tokens.slice(2, tokens.length).map(f => f.toInt)

        val score = spamminess(features, weights.value)
        val prediction = if (score > 0) "spam" else "ham"

        (docid, label, score, prediction)
      })

    results.saveAsTextFile(args.output())
  }

}
