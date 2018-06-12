package com.db.exercise

import org.apache.spark.sql.{DataFrame, SparkSession}


class DFRunner(val spark: SparkSession) {
  /**
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[DataFrame]] per each input
    */
  def extract(input: Map[String, String]): Map[String, DataFrame] = ???

  /**
    * @param extracted a map of [[DataFrame]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, DataFrame]): DataFrame = ???

  /**
    * @param transformed the [[DataFrame]] to store as a file
    * @param path the path to save the output file
    */
  def load(transformed: DataFrame, path: String): Unit = ???
}
