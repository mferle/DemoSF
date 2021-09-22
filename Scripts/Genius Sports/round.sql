CREATE OR REPLACE TABLE ex.round (
  roundNumber int NOT NULL,
  competitionId int NOT NULL,
  poolNumber int NOT NULL,
  leagueId int NOT NULL,
  roundName varchar(30) DEFAULT '',
  roundOrder int,
  roundType varchar(30) DEFAULT '',
  notes text,
  status varchar(20) DEFAULT '',
  externalId varchar(50) DEFAULT '',
  updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (roundNumber,competitionId,poolNumber),
  CONSTRAINT round_ibfk_1 FOREIGN KEY (leagueId) REFERENCES ex.league (leagueId),
  CONSTRAINT round_ibfk_2 FOREIGN KEY (competitionId) REFERENCES ex.competition (competitionId)
);    