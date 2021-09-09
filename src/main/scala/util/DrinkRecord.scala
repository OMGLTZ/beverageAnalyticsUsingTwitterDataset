package util

import scala.collection.mutable

case class DrinkRecord(drinkCounts: mutable.HashMap[String, Int],
                       lang: String, created_time: String, offset: Int)
