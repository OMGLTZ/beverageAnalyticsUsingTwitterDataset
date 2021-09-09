import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import util.{DrinkRecord, PrepareForAnalysis, Twitter}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
class DrinkAnalyzeBroad(sc: SparkContext, spark: SparkSession) extends scala.Serializable {

  spark.udf.register[DrinkRecord, Twitter]("findDrink", findDrink)

  private val drinkEn: Broadcast[HashMap[String, HashSet[String]]] = sc.broadcast(PrepareForAnalysis.drinkEn)

  private val drinkEs: Broadcast[HashMap[String, HashSet[String]]] = sc.broadcast(PrepareForAnalysis.drinkEs)

  private val drinkLangs: HashMap[String, Broadcast[HashMap[String, HashSet[String]]]] = HashMap[String, Broadcast[HashMap[String, HashSet[String]]]](
    "en" -> drinkEn,
    "es" -> drinkEs
  )

  def findDrink(twitte: Twitter): DrinkRecord = {
    val drinks: Broadcast[HashMap[String, HashSet[String]]] = drinkLangs(twitte.lang)
    val drinkCounts = new mutable.HashMap[String, Int]

    for (drink <- drinks.value) {
      for (word <- twitte.words) {
        if (drink._2.contains(word)) {
          val count: Int = drinkCounts.getOrElse(drink._1, 0) + 1
          drinkCounts.update(drink._1, count)
        }
      }
    }

    val record: DrinkRecord = DrinkRecord(drinkCounts, twitte.lang, twitte.created_time, twitte.offset)
    record

  }

  def calculate(twittes: Dataset[Twitter]): RDD[((String, String, String, Int), Int)] = {
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

object DrinkAnalyzeBroad {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DrinkAnalyze")
    val sc = new SparkContext(conf)
    val spark: SparkSession = PrepareForAnalysis.spark

    val analyze = new DrinkAnalyzeBroad(sc, spark)
    val twittes: Dataset[Twitter] = PrepareForAnalysis.readJson("1")
    val results: RDD[((String, String, String, Int), Int)] = analyze.calculate(twittes)
    results.collect().foreach(println)
  }
}
