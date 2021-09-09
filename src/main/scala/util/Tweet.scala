package util

/**
 * Resolved tweet from json file
 * @param words tweet contents
 * @param lang tweet language
 * @param created_time tweet created time(UTC)
 * @param offset tweet time offset
 */
case class Tweet(var words: Array[String], lang: String,
                 var created_time: String, var offset: Int)