package com.db.exercise

import com.db.exercise.model._

class DFRunnerSpec extends TestFixture {

  describe("DFRunner") {

    describe("extract") {
      it("return a map of alias -> DataFrame when given a map of alias -> filePath") {
        withSpark { sparkSession =>
          val teamsFilePath = getClass.getResource("/happyCase/teams.dat").getPath
          val scoresFilePath = getClass.getResource("/happyCase/scores.dat").getPath
          val input = Map(TeamsAlias -> teamsFilePath, ScoresAlias -> scoresFilePath)

          val expectedPlayerScores = Seq(
            PlayerScore("PLAYER1", "DAY1", 8.95F),
            PlayerScore("PLAYER2", "DAY1", 10.00F),
            PlayerScore("PLAYER3", "DAY1", 7.30F),
            PlayerScore("PLAYER1", "DAY2", 2.35F),
            PlayerScore("PLAYER3", "DAY2", 3.35F)
          )
          val expectedPlayerTeams = Seq(
            PlayerTeam("PLAYER1", "TEAM1"),
            PlayerTeam("PLAYER2", "TEAM2"),
            PlayerTeam("PLAYER3", "TEAM2")
          )

          val extractionResult = new DFRunner(sparkSession).extract(input)

          import sparkSession.implicits._
          extractionResult(TeamsAlias).as[PlayerTeam].collect() should contain theSameElementsAs expectedPlayerTeams
          extractionResult(ScoresAlias).as[PlayerScore].collect() should contain theSameElementsAs expectedPlayerScores
        }
      }
    }

    describe("transform") {
      it("should transform TEAMS and SCORES into winning team(s) and winning players(s)") {
        withSpark { sparkSession =>
          val teams = Seq(
            PlayerTeam("PLAYER1", "TEAM1"),
            PlayerTeam("PLAYER2", "TEAM2"),
            PlayerTeam("PLAYER3", "TEAM1"),
            PlayerTeam("PLAYER4", "TEAM2"),
            PlayerTeam("PLAYER5", "TEAM3"),
            PlayerTeam("PLAYER6", "TEAM3")
          )
          val scores = Seq(
            PlayerScore("PLAYER1", "DAY1", 1.2F),
            PlayerScore("PLAYER2", "DAY1", 1.2F),
            PlayerScore("PLAYER1", "DAY2", 2.3F),
            PlayerScore("PLAYER4", "DAY2", 2.7F),
            PlayerScore("PLAYER5", "DAY3", 2.5F),
            PlayerScore("PLAYER6", "DAY4", 1.4F)
          )
          val sparkContext = sparkSession.sparkContext
          import sparkSession.implicits._
          val extracted = Map(
            TeamsAlias -> sparkContext.parallelize(teams).toDF,
            ScoresAlias -> sparkContext.parallelize(scores).toDF
          )

          val expectedWinners = Seq(Winner("TEAM2", 3.9F), Winner("TEAM3", 3.9F), Winner("PLAYER1", 3.5F))

          val transformedData = new DFRunner(sparkSession).transform(extracted)

          transformedData.as[Winner].collect should contain theSameElementsAs expectedWinners
        }
      }
    }

    describe("load") {
      it("should load DataFrame to specified filePath limiting scores to two decimal places") {
        withSpark { sparkSession =>
          val outputFilePath = s"${getClass.getResource("/df_out").getPath}/${getClass.getSimpleName}"
          val winner = Winner("TEAM1", 10.123456F)
          val expectedWinner = Winner("TEAM1", 10.12F)

          import sparkSession.implicits._
          val transformed = sparkSession.sparkContext.parallelize(Seq(winner)).toDF

          new DFRunner(sparkSession).load(transformed, outputFilePath)

          contentsOfFile(outputFilePath) should contain theSameElementsAs Seq(expectedWinner)
        }
      }
    }
  }
}
