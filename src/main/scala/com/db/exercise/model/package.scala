package com.db.exercise

package object model {

  trait PlayerInfo extends Product with Serializable

  final case class PlayerTeam(playerInfo: String, team: String) extends PlayerInfo

  final case class PlayerScore(playerInfo: String, day: String, score: Float) extends PlayerInfo

  final case class Winner(names: String, totalScore: Float)

  val TeamsAlias = "TEAMS"
  val ScoresAlias = "SCORES"

}
