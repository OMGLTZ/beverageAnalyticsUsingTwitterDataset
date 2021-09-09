package improve

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DrinkAnalyzeBroadAccum(sc: SparkContext, spark: SparkSession) extends scala.Serializable {

  spark.udf.register[DrinkRecordBroadAccum, TwitteBroadAccum]("findDrink", findDrink)

  private val drinkEn: Broadcast[HashMap[String, HashSet[String]]] = sc.broadcast(HashMap[String, HashSet[String]](
    "coffee" -> HashSet("coffee", "americano", "espresso", "cappuccino", "latte", "doppio"),
    "tea" -> HashSet("tea"),
    "healthyDrink" -> HashSet("milk", "juice", "chocolate"),
    "softDrink" -> HashSet("cola", "soda", "fanta", "sprite", "lemonade"),
    "alcohol" -> HashSet("gin", "rum", "tequila", "brandy", "cocktail", "beer", "wine", "whiskey", "vodka")
  ))

  private val drinkEs: Broadcast[HashMap[String, HashSet[String]]] = sc.broadcast(HashMap[String, HashSet[String]](
    "coffee" -> HashSet("café", "americano", "espresso", "capuchino", "latte", "doppio"),
    "tea" -> HashSet("té"),
    "healthyDrink" -> HashSet("leche", "jugo", "chocalate"),
    "softDrink" -> HashSet("cola", "refresco", "fanta", "sprite", "limonada"),
    "alcohol" -> HashSet("ginebra", "ron", "tequila", "brandy", "cóctel", "cerveza", "vino", "whisky", "vodka")
  ))


  private val drinkLangs: HashMap[String, Broadcast[HashMap[String, HashSet[String]]]] = HashMap[String, Broadcast[HashMap[String, HashSet[String]]]](
    "en" -> drinkEn,
    "es" -> drinkEs
  )

  def readJson(month: String): Dataset[TwitteBroadAccum] = {
    import spark.implicits._
    //val filePath: String = "/data/ProjectDatasetTwitter/statuses.log.2014-10" + month + "*.gz"
    val filePath: String = "data/testDataset.json"
    //, "pt", "in", "tr", "ru", "it", "nl", "de"
    val languages: Set[String] = Set("en", "es")
    val frame: DataFrame = spark.read.json(filePath)
    val cleanedDF: Dataset[TwitteBroadAccum] = frame.select("text", "lang", "created_at", "user.utc_offset")
      .filter(_ (3) != null).filter(row => {
      languages.contains(row(1).toString)
    }).map(row => {
      val words: Array[String] = row(0).toString.toLowerCase.split(" ")
      val hour: String = row(2).toString.split(" ")(3).split(":")(0)
      TwitteBroadAccum(words, row(1).toString, hour, row(3).toString.toInt)
    })
    cleanedDF
  }

  def findDrink(twitte: TwitteBroadAccum): DrinkRecordBroadAccum = {
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

    DrinkRecordBroadAccum(drinkCounts, twitte.lang, twitte.created_time, twitte.offset)
  }

  def calculate(sc: SparkContext, twittes: Dataset[TwitteBroadAccum]): mutable.Map[(String, String, String, Int), Int] = {
    import spark.implicits._

    val tmp: Dataset[DrinkRecordBroadAccum] = twittes.map(findDrink)
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

  class DrinkAnalyzeAccumulator extends AccumulatorV2[DrinkRecordBroadAccum, mutable.Map[(String, String, String, Int), Int]] {

    var counts: mutable.Map[(String, String, String, Int), Int] = mutable.HashMap()

    override def isZero: Boolean = counts.isEmpty

    override def copy(): AccumulatorV2[DrinkRecordBroadAccum, mutable.Map[(String, String, String, Int), Int]] = new DrinkAnalyzeAccumulator

    override def reset(): Unit = counts.clear

    override def add(r: DrinkRecordBroadAccum): Unit = {
      val drinks: List[(String, Int)] = r.drinkCounts.toList
      for (drink <- drinks) {
        val newCount: Int = counts.getOrElse((drink._1, r.lang, r.created_time, r.offset), 0) + drink._2
        counts.update((drink._1, r.lang, r.created_time, r.offset), newCount)
      }
    }

    override def merge(other: AccumulatorV2[DrinkRecordBroadAccum, mutable.Map[(String, String, String, Int), Int]]): Unit = {
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

case class TwitteBroadAccum(var words: Array[String], lang: String,
                            var created_time: String, var offset: Int)

case class DrinkRecordBroadAccum(drinkCounts: mutable.HashMap[String, Int],
                                 lang: String, created_time: String, offset: Int)

object DrinkAnalyzeBroadAccum {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DrinkAnalyze")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DrinkAnalyzeSQL")
      .getOrCreate()

    val analyze = new DrinkAnalyzeBroadAccum(sc, spark)
    val twittes: Dataset[TwitteBroadAccum] = analyze.readJson("1")
    val results: mutable.Map[(String, String, String, Int), Int] = analyze.calculate(sc, twittes)
    println(results)
  }
}
