package com.db.exercise

import com.db.exercise.model.{PlayerScore, PlayerTeam}

class RDDRunnerSpec extends TestFixture {

  describe("RDDRunnerSpec") {

    describe("extract") {
      it("return a map of alias -> RDD[PlayerInfo] when given a map of alias -> filePath") {
        withSpark { sparkSession =>
          val teamsFilePath = getClass.getResource("/teams.dat").getPath
          val scoresFilePath = getClass.getResource("/scores.dat").getPath
          val input = Map("TEAMS" -> teamsFilePath, "SCORES" -> scoresFilePath)

          val expectedPlayerScores = Seq(
            PlayerScore("PLAYER1", "DAY1", 8.95),
            PlayerScore("PLAYER2", "DAY1", 10.00),
            PlayerScore("PLAYER3", "DAY1", 7.30),
            PlayerScore("PLAYER1", "DAY2", 2.35),
            PlayerScore("PLAYER3", "DAY2", 3.35)
          )
          val expectedPlayerTeams = Seq(
            PlayerTeam("PLAYER1", "TEAM1"),
            PlayerTeam("PLAYER2", "TEAM2"),
            PlayerTeam("PLAYER3", "TEAM2")
          )

          val extractionResult = new RDDRunner(sparkSession.sparkContext).extract(input)

          extractionResult("TEAMS").collect().toSeq should contain theSameElementsAs expectedPlayerTeams
          extractionResult("SCORES").collect().toSeq should contain theSameElementsAs expectedPlayerScores
        }
      }
    }

    describe("transform") {
      it("should transform") {

      }
    }

    describe("load") {
      it("should load") {

      }
    }

  }
}
