package improve

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DrinkAnalyzeBroad(sc: SparkContext, spark: SparkSession) extends scala.Serializable {

  spark.udf.register[DrinkRecordBroad, TwitteBroad]("findDrink", findDrink)

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

  def readJson(month: String): Dataset[TwitteBroad] = {
    import spark.implicits._
    //val filePath: String = "/data/ProjectDatasetTwitter/statuses.log.2014-10" + month + "*.gz"
    val filePath: String = "data/testDataset.json"
    //, "pt", "in", "tr", "ru", "it", "nl", "de"
    val languages: Set[String] = Set("en", "es")
    val frame: DataFrame = spark.read.json(filePath)
    val cleanedDF: Dataset[TwitteBroad] = frame.select("text", "lang", "created_at", "user.utc_offset")
      .filter(_ (3) != null).filter(row => {
      languages.contains(row(1).toString)
    }).map(row => {
      val words: Array[String] = row(0).toString.toLowerCase.split(" ")
      val hour: String = row(2).toString.split(" ")(3).split(":")(0)
      TwitteBroad(words, row(1).toString, hour, row(3).toString.toInt)
    })
    cleanedDF
  }

  def findDrink(twitte: TwitteBroad): DrinkRecordBroad = {
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

    val record: DrinkRecordBroad = DrinkRecordBroad(drinkCounts, twitte.lang, twitte.created_time, twitte.offset)
    record

  }

  def calculate(twittes: Dataset[TwitteBroad]): RDD[((String, String, String, Int), Int)] = {
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

case class TwitteBroad(var words: Array[String], lang: String,
                       var created_time: String, var offset: Int)

case class DrinkRecordBroad(drinkCounts: mutable.HashMap[String, Int],
                            lang: String, created_time: String, offset: Int)

object DrinkAnalyzeBroad {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DrinkAnalyze")
    val sc = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DrinkAnalyzeSQL")
      .getOrCreate()

    val analyze = new DrinkAnalyzeBroad(sc, spark)
    val twittes: Dataset[TwitteBroad] = analyze.readJson("1")
    val results: RDD[((String, String, String, Int), Int)] = analyze.calculate(twittes)
    results.collect().foreach(println)
  }
}
