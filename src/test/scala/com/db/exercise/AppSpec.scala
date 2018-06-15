package com.db.exercise

import com.db.exercise.model.Winner

class AppSpec extends TestFixture {

  describe("App") {

    describe("main") {
      it("should write the player(s) and team(s) with the highest scores to the outputFilePaths") {
        val teamFilePath = getResourcePath("/happyCase/teams.dat")
        val scoreFilePath = getResourcePath("/happyCase/scores.dat")
        val rddOutputFilePath = s"${getResourcePath("/rdd_out")}/${getClass.getSimpleName}_${System.currentTimeMillis}"
        val dfOutputFilePath = s"${getResourcePath("/df_out")}/${getClass.getSimpleName}_${System.currentTimeMillis}"
        val winners = Seq(Winner("TEAM2", 20.65F), Winner("PLAYER1", 11.30F))

        val args = Array(teamFilePath, scoreFilePath, rddOutputFilePath, dfOutputFilePath)

        App.main(args)

        contentsOfFile(rddOutputFilePath) should contain theSameElementsAs winners
        contentsOfFile(dfOutputFilePath) should contain theSameElementsAs winners
      }

      it("should ignore the player(s) scores above 10.0") {
        val teamFilePath = getResourcePath("/unhappyCase/teams.dat")
        val scoreFilePath = getResourcePath("/unhappyCase/scores.dat")
        val rddOutputFilePath = s"${getResourcePath("/rdd_out")}/${getClass.getSimpleName}_${System.currentTimeMillis}"
        val dfOutputFilePath = s"${getResourcePath("/df_out")}/${getClass.getSimpleName}_${System.currentTimeMillis}"
        val winners = Seq(Winner("TEAM1", 13.34F), Winner("PLAYER5", 9.99F))

        val args = Array(teamFilePath, scoreFilePath, rddOutputFilePath, dfOutputFilePath)

        App.main(args)

        contentsOfFile(rddOutputFilePath) should contain theSameElementsAs winners
        contentsOfFile(dfOutputFilePath) should contain theSameElementsAs winners
      }
    }
  }
}
