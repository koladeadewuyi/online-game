package com.db.exercise

import java.io.{File, PrintWriter}
import java.text.DecimalFormat

import com.db.exercise.model._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class RDDRunner(val context: SparkContext) {

  /**
    * Extracts input filePath into RDD[PlayerInfo] for each alias
    * @param input a map of the file paths to extract. The key is a [[String]] alias, the value is the [[String]] path of the file to extract.
    * @return a map of [[RDD]] per each input
    */
  def extract(input: Map[String, String]): Map[String, RDD[PlayerInfo]] = {
    input.map { case (alias, path) =>
      alias -> context.textFile(path).map { line =>
        val data = line.split(",").map(elem => elem.trim)
        data match {
          case Array(playerInfo, team) =>
            PlayerTeam(playerInfo, team)
          case Array(playerInfo, dayOfWeek, score) =>
            PlayerScore(playerInfo, dayOfWeek, score.toFloat)
          case unexpected =>
            throw new IllegalArgumentException(s"File contained unexpected line $unexpected")
        }
      }
    }
  }

  /**
    * Transforms extracted data into the result containing the team(s) and player(s) with highest scores
    * @param extracted a map of [[RDD]] indexed by a [[String]] alias
    * @return
    */
  def transform(extracted: Map[String, RDD[PlayerInfo]]): RDD[Winner] = {
    val playerTeamsRDD = extracted(TeamsAlias).asInstanceOf[RDD[PlayerTeam]].keyBy(_.playerInfo)
    val playerScoresRDD = extracted(ScoresAlias).asInstanceOf[RDD[PlayerScore]].keyBy(_.playerInfo)

    val teamPlayerScores = playerTeamsRDD.join(playerScoresRDD).values.map { case (playerTeam, playerScore) =>
      (playerTeam.team, playerScore.playerInfo, playerScore.score)
    }.filter { case (_, _, score) =>
      score <= 10 && score >= 0
    }

    val groupedByTeam = teamPlayerScores.groupBy { case (team, _, _) => team }
    val teamScores = computeTotalScore(groupedByTeam)

    val groupedBuPlayer = teamPlayerScores.groupBy { case (_, player, _) => player }
    val playerScores = computeTotalScore(groupedBuPlayer)

    val winners = Seq(teamScores, playerScores)
      .flatMap(_.distinct.groupBy(_._2).max._2.toSeq)
      .map { case (name, score) => Winner(name, score) }

    context.parallelize(winners)
  }

  /**
    * Persists the transformed results to destination file path
    * @param transformed the [[RDD]] to store as a file
    * @param path        the path to save the output file
    */
  def load(transformed: RDD[Winner], path: String): Unit = {
    val scoreFormat = new DecimalFormat("#.00")
    val winners = transformed.collect
    val writer = new PrintWriter(new File(path))
    try {
      winners.foreach { winner =>
        writer.append(s"${winner.names} ${scoreFormat.format(winner.totalScore)}${System.lineSeparator}")
      }
    } finally writer.close()
  }

  private def computeTotalScore(input: RDD[(String, Iterable[(String, String, Float)])]) = {
    input.map { case (team, teamPlayers) =>
      (team, teamPlayers.map(_._3).sum)
    }
  }

}
