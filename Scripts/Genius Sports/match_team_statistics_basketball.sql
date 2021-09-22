CREATE OR REPLACE TABLE ex.match_team_statistics_basketball (
  matchId int NOT NULL,
  teamId int NOT NULL,
  competitionId int,
  leagueId int NOT NULL,
  periodNumber int NOT NULL,
  periodType varchar(10) NOT NULL DEFAULT 'REGULAR',
  sAssists int,
  sAssistsDefensive int,
  sAssistsTurnoverRatio decimal(6,2),
  sBenchPoints int,
  sBlocks int,
  sBlocksReceived int,
  sDefensivePointsPerPossession decimal(6,2),
  sDefensiveRating decimal(6,2),
  sEfficiency decimal(6,2),
  sEfficiencyCustom decimal(6,2),
  sFieldGoalsAttempted int,
  sFieldGoalsEffectiveAdjusted decimal(6,2),
  sFieldGoalsMade int,
  sFieldGoalsPercentage decimal(6,2),
  sFoulsBenchTechnical int,
  sFoulsCoachDisqualifying int,
  sFoulsCoachTechnical int,
  sFoulsDisqualifying int,
  sFoulsOn int,
  sFoulsOffensive int,
  sFoulsPersonal int,
  sFoulsTeam int,
  sFoulsTechnical int,
  sFoulsTechnicalOther int,
  sFoulsTotal int,
  sFoulsUnsportsmanlike int,
  sFreeThrowsAttempted int,
  sFreeThrowsMade int,
  sFreeThrowsPercentage decimal(6,2),
  sMinutes decimal(10,6) NOT NULL,
  sOffensivePointsPerPossession decimal(6,2),
  sOffensiveRating decimal(6,2),
  sPace decimal(6,2),
  sPoints int,
  sPointsAgainst int,
  sPointsFastBreak int,
  sPointsFromTurnovers int,
  sPointsInThePaint int,
  sPointsSecondChance int,
  sPossessions decimal(6,2),
  sPossessionsOpponent decimal(6,2),
  sReboundsDefensive int,
  sReboundsOffensive int,
  sReboundsPersonal int,
  sReboundsTeam int,
  sReboundsTotal int,
  sSteals int,
  sThreePointersAttempted int,
  sThreePointersMade int,
  sThreePointersPercentage decimal(6,2),
  sTransitionDefence decimal(6,2),
  sTransitionOffence decimal(6,2),
  sTurnovers int,
  sTurnoversTeam int,
  sTwoPointersAttempted int,
  sTwoPointersMade int,
  sTwoPointersPercentage decimal(6,2),
  updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  sSecondChancePointsAttempted int,
  sSecondChancePointsMade int,
  sSecondChancePointsPercentage decimal(6,2),
  sPointsInThePaintMade int,
  sPointsInThePaintAttempted int,
  sPointsInThePaintPercentage decimal(6,2),
  sFastBreakPointsMade int,
  sFastBreakPointsAttempted int,
  sFastBreakPointsPercentage decimal(6,2),
  sTimesScoresLevel int,
  sLeadChanges int,
  sBiggestLead int,
  sBiggestLeadScore varchar(20) DEFAULT '',
  sBiggestScoringRun int,
  sBiggestScoringRunScore varchar(20) DEFAULT '',
  sTimeLeading decimal(6,2),
  sReboundsTeamOffensive int,
  sReboundsTeamDefensive int,
  sReboundsDefensiveDeadball int,
  sReboundsOffensiveDeadball int,
  sFouledOut int DEFAULT NULL,
  PRIMARY KEY (matchId,teamId,periodNumber,periodType),
  CONSTRAINT match_team_statistics_basketball_ibfk_1 FOREIGN KEY (leagueId) REFERENCES ex.league (leagueId),
  CONSTRAINT match_team_statistics_basketball_ibfk_2 FOREIGN KEY (competitionId) REFERENCES ex.competition (competitionId),
  CONSTRAINT match_team_statistics_basketball_ibfk_3 FOREIGN KEY (matchId) REFERENCES ex.fixtured_match (matchId),
  CONSTRAINT match_team_statistics_basketball_ibfk_4 FOREIGN KEY (teamId) REFERENCES ex.team (teamId)
);