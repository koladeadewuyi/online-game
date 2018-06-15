package com.db.exercise

import java.text.DecimalFormat

import com.db.exercise.model.Winner
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

import scala.io.Source

trait TestFixture extends FunSpec with Matchers {
  private val appName = getClass.getSimpleName
  private val master = "local[*]"
  private val sparkConf = new SparkConf().setAppName(appName)
    .setMaster(master)

  val scoreFormat = new DecimalFormat("#.00")

  def withSpark(test: SparkSession => Unit) {
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    try {
      test(sparkSession)
    } finally {
      sparkSession.stop()
      sparkSession.sparkContext.stop()
    }
  }

  def contentsOfFile(filePath: String): Seq[Winner] = {
    val source = Source.fromFile(filePath)
    try {
      source.getLines.map { line =>
        val winner = line.split(" ").map(_.trim)
        val winnerId = winner(0)
        val score = winner(1).toFloat
        Winner(winnerId, score)
      }.toList
    } finally{
      source.close()
    }
  }

  def getResourcePath(resource: String): String = getClass.getResource(resource).getPath
}
