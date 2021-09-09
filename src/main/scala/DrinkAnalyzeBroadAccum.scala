import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}
import util.{DrinkRecord, PrepareForAnalysis, Twitter}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable

class DrinkAnalyzeBroadAccum(sc: SparkContext, spark: SparkSession) extends scala.Serializable {

  spark.udf.register[DrinkRecord, Twitter]("findDrink", findDrink)

  private val drinkEn: Broadcast[HashMap[String, HashSet[String]]] = sc.broadcast(PrepareForAnalysis.drinkEn)

  private val drinkEs: Broadcast[HashMap[String, HashSet[String]]] = sc.broadcast(PrepareForAnalysis.drinkEs)

  private val drinkLangs: HashMap[String,
    Broadcast[HashMap[String, HashSet[String]]]] =
    HashMap[String, Broadcast[HashMap[String, HashSet[String]]]](
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

    DrinkRecord(drinkCounts, twitte.lang, twitte.created_time, twitte.offset)
  }

  def calculate(sc: SparkContext, twittes: Dataset[Twitter]): mutable.Map[(String, String, String, Int), Int] = {
    import spark.implicits._

    val tmp: Dataset[DrinkRecord] = twittes.map(findDrink)
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

  class DrinkAnalyzeAccumulator extends AccumulatorV2[DrinkRecord, mutable.Map[(String, String, String, Int), Int]] {

    var counts: mutable.Map[(String, String, String, Int), Int] = mutable.HashMap()

    override def isZero: Boolean = counts.isEmpty

    override def copy(): AccumulatorV2[DrinkRecord, mutable.Map[(String, String, String, Int), Int]] = new DrinkAnalyzeAccumulator

    override def reset(): Unit = counts.clear

    override def add(r: DrinkRecord): Unit = {
      val drinks: List[(String, Int)] = r.drinkCounts.toList
      for (drink <- drinks) {
        val newCount: Int = counts.getOrElse((drink._1, r.lang, r.created_time, r.offset), 0) + drink._2
        counts.update((drink._1, r.lang, r.created_time, r.offset), newCount)
      }
    }

    override def merge(other: AccumulatorV2[DrinkRecord, mutable.Map[(String, String, String, Int), Int]]): Unit = {
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

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DrinkAnalyze")
    val sc = new SparkContext(conf)
    val spark: SparkSession = PrepareForAnalysis.spark

    val analyze = new DrinkAnalyzeBroadAccum(sc, spark)
    val twittes: Dataset[Twitter] = PrepareForAnalysis.readJson("1")
    val results: mutable.Map[(String, String, String, Int), Int] = analyze.calculate(sc, twittes)
    println(results)
  }
}
