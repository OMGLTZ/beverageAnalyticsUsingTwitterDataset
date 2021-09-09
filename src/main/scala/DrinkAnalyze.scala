import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import util.{DrinkRecord, PrepareForAnalysis, Tweet}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DrinkAnalyze extends scala.Serializable {

  val spark: SparkSession = PrepareForAnalysis.spark

  // register the findDrink function
  spark.udf.register[DrinkRecord, Tweet]("findDrink", findDrink)

  // map from lang to drinkMap
  val drinkLangs: HashMap[String, HashMap[String, HashSet[String]]] =
    PrepareForAnalysis.drinkLangs

  def findDrink(twitte: Tweet): DrinkRecord = {
    val drinks: HashMap[String, HashSet[String]] = drinkLangs(twitte.lang)
    val drinkCounts = new mutable.HashMap[String, Int]

    for (drink <- drinks) {
      for (word <- twitte.words) {
        if (drink._2.contains(word)) {
          val count: Int = drinkCounts.getOrElse(drink._1, 0) + 1
          drinkCounts.update(drink._1, count)
        }
      }
    }

    // create DrinkRecord for every match
    val record: DrinkRecord = DrinkRecord(drinkCounts, twitte.lang,
      twitte.created_time, twitte.offset)
    record
  }

  def calculate(twittes: Dataset[Tweet]):
    RDD[((String, String, String, Int), Int)] = {
    import spark.implicits._

    val res: RDD[((String, String, String, Int), Int)] = twittes.map(findDrink).filter(r => {
      r.drinkCounts.nonEmpty
    }).map(r => {
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
    val results: RDD[((String, String, String, Int), Int)] = analyze.calculate(twittes)
    results.collect().foreach(println)
  }
}
