package com.db.exercise

import com.db.exercise.model.{PlayerInfo, PlayerScore, PlayerTeam}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{FunSpec, Matchers}

trait TestFixture extends FunSpec with Matchers {
  private val appName = getClass.getSimpleName
  private val master = "local[*]"
  private val sparkConf = new SparkConf().setAppName(appName)
    .setMaster(master)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[PlayerTeam], classOf[PlayerScore], classOf[PlayerInfo]))

  private val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

  def withSpark(test: SparkSession => Unit) {
    try {
      test(sparkSession)
    } finally {
      sparkSession.stop()
      sparkSession.sparkContext.stop()
    }
  }
}
