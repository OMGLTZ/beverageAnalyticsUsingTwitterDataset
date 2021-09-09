package util

import scala.collection.mutable

/**
 * matched drink record
 * @param drinkCounts matched times
 * @param lang tweet language
 * @param created_time tweet created time(UTC)
 * @param offset tweet time offset
 */
case class DrinkRecord(drinkCounts: mutable.HashMap[String, Int],
                       lang: String, created_time: String, offset: Int)
