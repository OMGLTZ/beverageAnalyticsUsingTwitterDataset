import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import util.{DrinkRecord, PrepareForAnalysis, Tweet}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable

/**
 * Drink Analyze by Spark, improved by Broadcast and Accumulator.
 * In order to shuffle, DrinkAnalyze needs to be extends scala.Serializable.
 */
class DrinkAnalyzeBroadAccum(sc: SparkContext, spark: SparkSession) extends scala.Serializable {

  // register the findDrink function
  spark.udf.register[DrinkRecord, Tweet]("findDrink", findDrink)

  // create the broadcast table, it is very suitable for the small table.
  private val drinkEn: Broadcast[HashMap[String, HashSet[String]]] =
    sc.broadcast(PrepareForAnalysis.drinkEn)

  private val drinkEs: Broadcast[HashMap[String, HashSet[String]]] =
    sc.broadcast(PrepareForAnalysis.drinkEs)

  // map from language to drinkMap
  private val drinkLangs: HashMap[String,
    Broadcast[HashMap[String, HashSet[String]]]] =
    HashMap[String, Broadcast[HashMap[String, HashSet[String]]]](
    "en" -> drinkEn,
    "es" -> drinkEs
  )

  /**
   * match the number of drink types from Tweet contents
   * @param tweet Tweet
   * @return
   */
  def findDrink(tweet: Tweet): DrinkRecord = {
    val drinks: Broadcast[HashMap[String, HashSet[String]]] =
      drinkLangs(tweet.lang)
    val drinkCounts = new mutable.HashMap[String, Int]

    for (drink <- drinks.value) {
      for (word <- tweet.words) {
        if (drink._2.contains(word)) {
          val count: Int = drinkCounts.getOrElse(drink._1, 0) + 1
          drinkCounts.update(drink._1, count)
        }
      }
    }

    DrinkRecord(drinkCounts, tweet.lang, tweet.created_time, tweet.offset)
  }

  /**
   * process tweets to final format:
   * ((drinkType, language, time, time_offset), counts)
   *
   * Map findDrink(Tweet) to DrinkRecord,
   * filter records which does not contains drink
   * create a Accumulator to help reduce key
   * @param tweets tweets from json file
   * @return results to save and process by python
   */
  def calculateResults(sc: SparkContext, tweets: Dataset[Tweet]):
  mutable.Map[(String, String, String, Int), Int] = {
    import spark.implicits._

    val tmp: Dataset[DrinkRecord] = tweets.map(findDrink)
      .filter(r => {
        r.drinkCounts.nonEmpty
      })

    val drinkAnalyzeAccumulator = new DrinkAnalyzeAccumulator
    sc.register(drinkAnalyzeAccumulator, "drinkAnalyzeAccumulator")

    tmp.foreach(record => {
      drinkAnalyzeAccumulator.add(record)
    })

    drinkAnalyzeAccumulator.value
  }

  /**
   * Accumulator for help reduce by key. Doing this can reduce shuffle cost and
   * reduce keys in the master.
   */
  class DrinkAnalyzeAccumulator extends AccumulatorV2[DrinkRecord,
    mutable.Map[(String, String, String, Int), Int]] {

    var counts: mutable.Map[(String, String, String, Int), Int] = mutable.HashMap()

    override def isZero: Boolean = counts.isEmpty

    override def copy(): AccumulatorV2[DrinkRecord, mutable.Map[(String, String,
      String, Int), Int]] = new DrinkAnalyzeAccumulator

    override def reset(): Unit = counts.clear

    override def add(r: DrinkRecord): Unit = {
      val drinks: List[(String, Int)] = r.drinkCounts.toList
      for (drink <- drinks) {
        val newCount: Int = counts.getOrElse((drink._1, r.lang,
          r.created_time, r.offset), 0) + drink._2
        counts.update((drink._1, r.lang, r.created_time, r.offset), newCount)
      }
    }

    override def merge(other: AccumulatorV2[DrinkRecord,
      mutable.Map[(String, String, String, Int), Int]]): Unit = {
      val map1: mutable.Map[(String, String, String, Int), Int] = this.counts
      val map2: mutable.Map[(String, String, String, Int), Int] = other.value
      map2.foreach(kv => {
        val newCount: Int = map1.getOrElse(kv._1, 0) + kv._2
        map1.update(kv._1, newCount)
      })
    }

    override def value: mutable.Map[(String, String, String, Int), Int] = {
      counts
    }
  }
}

object DrinkAnalyzeBroadAccum {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("DrinkAnalyze")
    val sc = new SparkContext(conf)
    val spark: SparkSession = PrepareForAnalysis.spark

    val analyze = new DrinkAnalyzeBroadAccum(sc, spark)
    val tweets: Dataset[Tweet] = PrepareForAnalysis.readJson("1")
    val results: mutable.Map[(String, String, String, Int), Int] =
      analyze.calculateResults(sc, tweets)
    println(results)
  }
}
