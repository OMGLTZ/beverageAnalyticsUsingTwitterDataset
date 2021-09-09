import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.immutable.{HashMap, HashSet}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DrinkAnalyze extends scala.Serializable {

  private val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("DrinkAnalyzeSQL")
    .getOrCreate()

  spark.udf.register[DrinkRecord, Twitte]("findDrink", findDrink)

  private val drinkEn: HashMap[String, HashSet[String]] = HashMap[String, HashSet[String]](
    "coffee" -> HashSet("coffee", "americano", "espresso", "cappuccino", "latte", "doppio"),
    "tea" -> HashSet("tea"),
    "healthyDrink" -> HashSet("milk", "juice", "chocolate"),
    "softDrink" -> HashSet("cola", "soda", "fanta", "sprite", "lemonade"),
    "alcohol" -> HashSet("gin", "rum", "tequila", "brandy", "cocktail", "beer", "wine", "whiskey", "vodka")
  )

  private val drinkEs: HashMap[String, HashSet[String]] = HashMap[String, HashSet[String]](
    "coffee" -> HashSet("café", "americano", "espresso", "capuchino", "latte", "doppio"),
    "tea" -> HashSet("té"),
    "healthyDrink" -> HashSet("leche", "jugo", "chocalate"),
    "softDrink" -> HashSet("cola", "refresco", "fanta", "sprite", "limonada"),
    "alcohol" -> HashSet("ginebra", "ron", "tequila", "brandy", "cóctel", "cerveza", "vino", "whisky", "vodka")
  )

  /*
private val drinkEs = new mutable.HashMap[String, mutable.HashSet[String]]
private val drinkPt = new mutable.HashMap[String, mutable.HashSet[String]]
private val drinkIn = new mutable.HashMap[String, mutable.HashSet[String]]
private val drinkTr = new mutable.HashMap[String, mutable.HashSet[String]]
private val drinkRu = new mutable.HashMap[String, mutable.HashSet[String]]
private val drinkIt = new mutable.HashMap[String, mutable.HashSet[String]]
private val drinkNl = new mutable.HashMap[String, mutable.HashSet[String]]
private val drinkDe = new mutable.HashMap[String, mutable.HashSet[String]]
 */

  private val drinkLangs: HashMap[String, HashMap[String, HashSet[String]]] = HashMap[String, HashMap[String, HashSet[String]]](
    "en" -> drinkEn,
    "es" -> drinkEs
  )

  def readJson(month: String): Dataset[Twitte] = {
    import spark.implicits._
    //val filePath: String = "/data/ProjectDatasetTwitter/statuses.log.2014-10" + month + "*.gz"
    val filePath: String = "data/testDataset.json"
    //, "pt", "in", "tr", "ru", "it", "nl", "de"
    val languages: Set[String] = Set("en", "es")
    val frame: DataFrame = spark.read.json(filePath)
    val cleanedDF: Dataset[Twitte] = frame.select("text", "lang", "created_at", "user.utc_offset")
      .filter(_ (3) != null).filter(row => {
      languages.contains(row(1).toString)
    }).map(row => {
      val words: Array[String] = row(0).toString.toLowerCase.split(" ")
      val hour: String = row(2).toString.split(" ")(3).split(":")(0)
      Twitte(words, row(1).toString, hour, row(3).toString.toInt)
    })
    cleanedDF
  }

  def findDrink(twitte: Twitte): DrinkRecord = {
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

    val record: DrinkRecord = DrinkRecord(drinkCounts, twitte.lang, twitte.created_time, twitte.offset)
    record

  }

  def calculate(twittes: Dataset[Twitte]): RDD[((String, String, String, Int), Int)] = {
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

  /*
  case class MyBuffer(var drinkNums: mutable.HashMap[String, Int])

  class MyAggregator extends Aggregator[Twitte, MyBuffer, mutable.HashMap[String, Int]] {
    override def zero: MyBuffer = {
      MyBuffer(mutable.HashMap[String, Int])
    }

    override def reduce(b: MyBuffer, a: Twitte): MyBuffer = ???

    override def merge(b1: MyBuffer, b2: MyBuffer): MyBuffer = {
      val nums: mutable.HashMap[String, Int] = b2.drinkNums
      b1.drinkNums.
      b1
    }

    override def finish(reduction: MyBuffer): mutable.HashMap[String, Int] = reduction.drinkNums

    override def bufferEncoder: Encoder[MyBuffer] = Encoders.product

    override def outputEncoder: Encoder[mutable.HashMap[String, Int]] = Encoders.product[]
  }
 */

}

case class Twitte(var words: Array[String], lang: String,
                  var created_time: String, var offset: Int)

case class DrinkRecord(drinkCounts: mutable.HashMap[String, Int],
                       lang: String, created_time: String, offset: Int)

object DrinkAnalyze {
  def main(args: Array[String]): Unit = {
    val analyze = new DrinkAnalyze
    val twittes: Dataset[Twitte] = analyze.readJson("1")
    val results: RDD[((String, String, String, Int), Int)] = analyze.calculate(twittes)
    results.collect().foreach(println)
  }
}
