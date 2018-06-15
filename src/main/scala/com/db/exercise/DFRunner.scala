package com.db.exercise

import java.io.{File, PrintWriter}
import java.text.DecimalFormat

import com.db.exercise.model.{PlayerScore, PlayerTeam, Winner, TeamsAlias, ScoresAlias}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{FloatType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.reflect.runtime.universe.TypeTag

class DFRunner(val spark: SparkSession) {

  private val playerTeamSchema: StructType = createSchemaFor[PlayerTeam]
  private val playerScoreSchema: StructType = createSchemaFor[PlayerScore]

  /**
    * Extracts input filePath into DataFrames for each alias
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[DataFrame]] per each input
    */
  def extract(input: Map[String, String]): Map[String, DataFrame] = {
    input.map { case (alias, path) =>
      val schema = alias match {
        case TeamsAlias => playerTeamSchema
        case ScoresAlias => playerScoreSchema
        case unexpected => throw new IllegalArgumentException(s"Invalid alias $unexpected")
      }

      alias -> spark.read
        .option("header", value = false)
        .option("ignoreLeadingWhiteSpace", value = true)
        .option("ignoreTrailingWhiteSpace", value = true)
        .schema(schema)
        .csv(path)
    }
  }

  /**
    * Transforms extracted data into the result containing the team(s) and player(s) with highest scores
    * @param extracted a map of [[DataFrame]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, DataFrame]): DataFrame = {
    val dataFrames = extracted.map { case (alias, df) =>
      alias match {
        case TeamsAlias => df
        case ScoresAlias => df
        case unexpected => throw new IllegalArgumentException(s"Invalid alias $unexpected")
      }
    }

    val computeWinners = udf { rows: Seq[Row] =>
      val winners = rows.flatMap { row =>
        val teamName = row.getString(0)
        val playerInfo = row.getString(1)
        val teamScore = row.getFloat(2)
        val playerScore = row.getFloat(3)
        val isWinningTeam = row.getBoolean(4)
        val isWinningPlayer = row.getBoolean(5)

        if (isWinningTeam && isWinningPlayer) Seq((teamName, teamScore), (playerInfo, playerScore))
        else if (isWinningTeam) Seq((teamName, teamScore))
        else if (isWinningPlayer) Seq((playerInfo, playerScore))
        else Seq.empty

      }.distinct

      winners
    }

    dataFrames.reduce(_ join(_, Seq("playerInfo")))
      .filter(col("score") <= 10 and col("score") >= 0)
      .withColumn("totalScorePerPlayer", (sum("score") over Window.partitionBy("team", "playerInfo")).cast(FloatType))
      .withColumn("totalScorePerTeam", (sum("score") over Window.partitionBy("team")).cast(FloatType))
      .select("team", "playerInfo", "totalScorePerPlayer", "totalScorePerTeam")
      .distinct()
      .withColumn("maxScorePerTeam", max("totalScorePerTeam") over())
      .withColumn("maxScorePerPlayer", max("totalScorePerPlayer") over())
      .withColumn("winningTeam", col("totalScorePerTeam") === col("maxScorePerTeam"))
      .withColumn("winningPlayer", col("totalScorePerPlayer") === col("maxScorePerPlayer"))
      .filter(col("winningTeam") === true or col("winningPlayer") === true)
      .groupBy("team")
      .agg(collect_list(struct("team", "playerInfo", "maxScorePerTeam", "maxScorePerPlayer", "winningTeam", "winningPlayer")) as "players")
      .withColumn("winners", explode(computeWinners(col("players"))))
      .select(col("winners._1") as "names", col("winners._2") as "totalScore")
  }

  /**
    * Persists the transformed results to destination file path
    * @param transformed the [[DataFrame]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: DataFrame, path: String): Unit = {
    import spark.implicits._
    val scoreFormat = new DecimalFormat("#.00")
    val winners = transformed.as[Winner].collect()
    val writer = new PrintWriter(new File(path))
    try {
      winners.foreach { winner =>
        writer.write(s"${winner.names} ${scoreFormat.format(winner.totalScore)}${System.lineSeparator()}")
      }
    } finally writer.close()
  }

  private def createSchemaFor[T: TypeTag] = {
    ScalaReflection
      .schemaFor[T]
      .dataType
      .asInstanceOf[StructType]
  }
}
