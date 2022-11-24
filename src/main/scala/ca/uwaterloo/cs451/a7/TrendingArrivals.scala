/**
 * Bespin: reference implementations of "big data" algorithms
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ca.uwaterloo.cs451.a7

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.{State, StateSpec, Time}
import org.rogach.scallop._

import java.io.File
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.mutable

class TrendingArrivalsConfig(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConfig(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("TrendingArrivals")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val citigroupx_min = -74.012083
    val citigroupx_max = -74.009867
    val citigroupy_min = 40.720053
    val citigroupy_max = 40.7217236

    val goldmanx_min = -74.0144185
    val goldmanx_max = -74.013777
    val goldmany_min = 40.7138745
    val goldmany_max = 40.7152275

    val spec = StateSpec.function(
      (batchTime: Time, key: String, value: Option[Int], state: State[Int]) => {
        (value, state.getOption) match {
          case (Some(cur), Some(prev)) =>
            state.update(cur)
            val location = if (key.equals("citigroup")) "Citigroup" else "Goldman Sachs"
            if (cur >= 10 && cur >= 2 * prev)
              println(s"Number of arrivals to $location has doubled from $prev to $cur at ${batchTime.milliseconds}!")
            Option(key, prev, cur)
          case (Some(cur), None) =>
            state.update(cur)
            Option(key, 0, cur)
          case (None, None) =>
            state.update(0)
            Option(key, 0, 0)
          case _ => Option(key, 0, 0)
        }
      }
    )

    val wc = stream.map(_.split(","))
      .map(tuple => {
        if (tuple(0).equals("green"))
          (tuple(8).toDouble, tuple(9).toDouble)
        else
          (tuple(10).toDouble, tuple(11).toDouble)
      })
      .filter(pair => ((pair._1 > goldmanx_min) && (pair._1 < goldmanx_max) && (pair._2 > goldmany_min) && (pair._2 < goldmany_max)) ||
        ((pair._1 > citigroupx_min) && (pair._1 < citigroupx_max) && (pair._2 > citigroupy_min) && (pair._2 < citigroupy_max))
      )
      .map(pair => {
        val region = if ((pair._1 > citigroupx_min) && (pair._1 < citigroupx_max) && (pair._2 > citigroupy_min) && (pair._2 < citigroupy_max))
          "citigroup"
        else
          "goldman"
        (region, 1)
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .mapWithState(spec)
      .persist()

    val outputDir = args.output()

    wc.foreachRDD((rdd, time) => {
      rdd.map { case (key, prev, cur) =>
        (key, (cur, time.milliseconds, prev))
      }
      .coalesce(1)
      .saveAsTextFile(outputDir + "/part-" + "%08d".format(time.milliseconds))

//      rdd.foreach{ case (key, prev, cur) =>
//        val location = if (key.equals("citigroup")) "Citigroup" else "Goldman Sachs"
//        if (prev > 0 && cur >= 10 && cur >= 2 * prev)
//          println(s"Number of arrivals to $location has doubled from $prev to $cur at ${time.milliseconds}!")
//      }
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
