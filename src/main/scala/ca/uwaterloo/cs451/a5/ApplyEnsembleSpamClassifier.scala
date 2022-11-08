package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._

class ApplyEnsembleSpamConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, model, method)
  val input = opt[String](descr = "input path", required = true)
  val output = opt[String](descr = "output path", required = true)
  val model = opt[String](descr = "trained model", required = true)
  val method = opt[String](descr = "ensemble method", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ApplyEnsembleSpamConf(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Model: " + args.model())
    log.info("Method: " + args.method())

    val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val method = args.method().trim()

    val modelX = args.model() + "/part-00000"
    val modelY = args.model() + "/part-00001"
    val modelB = args.model() + "/part-00002"

    def getWeights(modelDir: String) : Map[Int, Double] = {
      sc.textFile(modelDir)
        .map(line => {
          val tokens = line.split(",")
          val weight = tokens(1).substring(0, tokens(1).length - 1).toDouble
          val feature = tokens(0).substring(1).toInt
          (feature, weight)
        })
        .collect().toMap
    }

    val weightsX = sc.broadcast(getWeights(modelX))
    val weightsY = sc.broadcast(getWeights(modelY))
    val weightsB = sc.broadcast(getWeights(modelB))

    def spamminess(features: Array[Int], w: Map[Int, Double]): Double = {
      var score = 0d
      features.foreach(f => if (w.contains(f)) score += w(f))
      score
    }

    def isSpam(score: Double) = if (score > 0) true else false

    val results = sc.textFile(args.input())
      .map(line => {
        val tokens = line.split(" ")
        val docid = tokens(0)
        val label = tokens(1).trim()
        val features = tokens.slice(2, tokens.length).map(f => f.toInt)

        val scoreX = spamminess(features, weightsX.value)
        val scoreY = spamminess(features, weightsY.value)
        val scoreB = spamminess(features, weightsB.value)

        var score = 0d
        var prediction = ""

        if (method.equals("average")) {
          score = (scoreX + scoreY + scoreB ) / 3.0
          prediction = if (score > 0) "spam" else "ham"
        } else {
          var hamVotes = 0
          var spamVotes = 0
          if (isSpam(scoreX)) spamVotes += 1 else hamVotes += 1
          if (isSpam(scoreY)) spamVotes += 1 else hamVotes += 1
          if (isSpam(scoreB)) spamVotes += 1 else hamVotes += 1

          prediction = if (hamVotes > spamVotes) "ham" else "spam"
          score = spamVotes - hamVotes
        }

        (docid, label, score, prediction)
      })

    results.saveAsTextFile(args.output())
  }

}
