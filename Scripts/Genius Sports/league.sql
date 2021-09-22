CREATE OR REPLACE TABLE ex.league (
  leagueId int NOT NULL,
  leagueName varchar(30) DEFAULT '',
  notes text,
  status varchar(20) DEFAULT '',
  externalId varchar(50) DEFAULT '',
  updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (leagueId)
);    