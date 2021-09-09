import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import util.{DrinkRecord, PrepareForAnalysis, Tweet}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Drink Analyze by Spark without any improvements. In order to shuffle,
 * DrinkAnalyze needs to be extends scala.Serializable.
 */
class DrinkAnalyze extends scala.Serializable {

  val spark: SparkSession = PrepareForAnalysis.spark

  // register the findDrink function
  spark.udf.register[DrinkRecord, Tweet]("findDrink", findDrink)

  // map from language to drinkMap
  val drinkLangs: HashMap[String, HashMap[String, HashSet[String]]] =
    PrepareForAnalysis.drinkLangs

  /**
   * match the number of drink types from Tweet contents
   * @param tweet Tweet
   * @return
   */
  def findDrink(tweet: Tweet): DrinkRecord = {
    val drinks: HashMap[String, HashSet[String]] = drinkLangs(tweet.lang)
    // get the times of each drink type appears
    val drinkCounts = new mutable.HashMap[String, Int]

    // match drinks
    for (drink <- drinks) {
      for (word <- tweet.words) {
        if (drink._2.contains(word)) {
          val count: Int = drinkCounts.getOrElse(drink._1, 0) + 1
          drinkCounts.update(drink._1, count)
        }
      }
    }

    // create DrinkRecord for every match
    val record: DrinkRecord = DrinkRecord(drinkCounts, tweet.lang,
      tweet.created_time, tweet.offset)
    record
  }

  /**
   * process tweets to final format:
   * ((drinkType, language, time, time_offset), counts)
   * @param tweets tweets from json file
   * @return results to save and process by python
   */
  def calculateResults(tweets: Dataset[Tweet]):
    RDD[((String, String, String, Int), Int)] = {

    import spark.implicits._

    // filter records which does not contains drink
    // if one tweet mentions more than one type of drink, then flatMap it
    // finally, reduce records by key.
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

object DrinkAnalyze {
  def main(args: Array[String]): Unit = {
    val analyze = new DrinkAnalyze
    val twittes: Dataset[Tweet] = PrepareForAnalysis.readJson("1")
    val results: RDD[((String, String, String, Int), Int)] =
      analyze.calculateResults(twittes)
    results.collect().foreach(println)

    // results should be saved in file system for analyze results
    // results.saveAsTextFile("")
  }
}
