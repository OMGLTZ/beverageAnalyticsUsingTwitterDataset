package util

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.immutable.{HashMap, HashSet}

case object PrepareForAnalysis {

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("DrinkAnalyzeSQL")
    .getOrCreate()

  // English drink map
  val drinkEn: HashMap[String, HashSet[String]] =
    HashMap[String, HashSet[String]] (
      "coffee" -> HashSet("coffee", "americano", "espresso",
        "cappuccino", "latte", "doppio"),
      "tea" -> HashSet("tea"),
      "healthyDrink" -> HashSet("milk", "juice", "chocolate"),
      "softDrink" -> HashSet("cola", "soda", "fanta", "sprite", "lemonade"),
      "alcohol" -> HashSet("gin", "rum", "tequila", "brandy", "cocktail",
        "beer", "wine", "whiskey", "vodka")
    )

  // Spanish drink map
  val drinkEs: HashMap[String, HashSet[String]] =
    HashMap[String, HashSet[String]](
      "coffee" -> HashSet("café", "americano", "espresso",
        "capuchino", "latte", "doppio"),
      "tea" -> HashSet("té"),
      "healthyDrink" -> HashSet("leche", "jugo", "chocalate"),
      "softDrink" -> HashSet("cola", "refresco", "fanta", "sprite", "limonada"),
      "alcohol" -> HashSet("ginebra", "ron", "tequila", "brandy",
        "cóctel", "cerveza", "vino", "whisky", "vodka")
    )

  /*
  other language drink maps
  private val drinkEs = new mutable.HashMap[String, mutable.HashSet[String]]
  private val drinkPt = new mutable.HashMap[String, mutable.HashSet[String]]
  private val drinkIn = new mutable.HashMap[String, mutable.HashSet[String]]
  private val drinkTr = new mutable.HashMap[String, mutable.HashSet[String]]
  private val drinkRu = new mutable.HashMap[String, mutable.HashSet[String]]
  private val drinkIt = new mutable.HashMap[String, mutable.HashSet[String]]
  private val drinkNl = new mutable.HashMap[String, mutable.HashSet[String]]
  private val drinkDe = new mutable.HashMap[String, mutable.HashSet[String]]
  */

  val drinkLangs: HashMap[String, HashMap[String, HashSet[String]]] =
    HashMap[String, HashMap[String, HashSet[String]]](
      "en" -> drinkEn,
      "es" -> drinkEs
    )

  def readJson(month: String): Dataset[Twitter] = {
    import spark.implicits._
    //val filePath: String = "/data/ProjectDatasetTwitter/statuses.log.2014-10" + month + "*.gz"
    val filePath: String = "src/data/testDataset.json"
    //, "pt", "in", "tr", "ru", "it", "nl", "de"
    val languages: Set[String] = Set("en", "es")
    val frame: DataFrame = spark.read.json(filePath)
    val cleanedDF: Dataset[Twitter] = frame.select("text", "lang",
      "created_at", "user.utc_offset")
      .filter(_ (3) != null).filter(row => {
      languages.contains(row(1).toString)
    }).map(row => {
      val words: Array[String] = row(0).toString.toLowerCase.split(" ")
      val hour: String = row(2).toString.split(" ")(3).split(":")(0)
      Twitter(words, row(1).toString, hour, row(3).toString.toInt)
    })
    cleanedDF
  }
}
