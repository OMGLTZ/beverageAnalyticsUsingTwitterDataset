import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DrinkRecord, PrepareForAnalysis, Tweet}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Drink Analyze by Spark, improved by Broadcast. In order to shuffle,
 * DrinkAnalyze needs to be extends scala.Serializable.
 */
class DrinkAnalyzeBroad(sc: SparkContext, spark: SparkSession)
  extends scala.Serializable {

  // register the findDrink function
  spark.udf.register[DrinkRecord, Tweet]("findDrink", findDrink)

  // create the broadcast table, it is very suitable for the small table.
  private val drinkEn: Broadcast[HashMap[String, HashSet[String]]] =
    sc.broadcast(PrepareForAnalysis.drinkEn)

  private val drinkEs: Broadcast[HashMap[String, HashSet[String]]] =
    sc.broadcast(PrepareForAnalysis.drinkEs)

  // map from language to drinkMap
  private val drinkLangs: HashMap[String, Broadcast[HashMap[String, HashSet[String]]]] =
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

    val record: DrinkRecord = DrinkRecord(drinkCounts, tweet.lang,
      tweet.created_time, tweet.offset)
    record

  }

  /**
   * process tweets to final format:
   * ((drinkType, language, time, time_offset), counts)
   *
   * Map findDrink(Tweet) to DrinkRecord,
   * filter records which does not contains drink
   * if one tweet mentions more than one type of drink, then flatMap it
   * finally, reduce records by key.
   * @param tweets tweets from json file
   * @return results to save and process by python
   */
  def calculate(tweets: Dataset[Tweet]): RDD[((String, String, String, Int), Int)] = {
    import spark.implicits._

    val res: RDD[((String, String, String, Int), Int)] = tweets.map(findDrink)
      .filter(r => {r.drinkCounts.nonEmpty})
      .map(r => {
      val drinks: List[(String, Int)] = r.drinkCounts.toList
      val newDrinks = new ArrayBuffer[(String, Int, String, String, Int)]
      for (drink <- drinks) {
        newDrinks.append((drink._1, drink._2, r.lang, r.created_time, r.offset))
      }
      newDrinks
    }).flatMap(list => list).map(r => {
      ((r._1, r._3, r._4, r._5), r._2)
    }).rdd.reduceByKey(_ + _)

    res
  }
}

object DrinkAnalyzeBroad {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]")
      .setAppName("DrinkAnalyze")
    val sc = new SparkContext(conf)
    val spark: SparkSession = PrepareForAnalysis.spark

    val analyze = new DrinkAnalyzeBroad(sc, spark)
    val tweets: Dataset[Tweet] = PrepareForAnalysis.readJson("1")
    val results: RDD[((String, String, String, Int), Int)] =
      analyze.calculate(tweets)
    results.collect().foreach(println)

    // results should be saved in file system for analyze results
    // results.saveAsTextFile("")
  }
}
