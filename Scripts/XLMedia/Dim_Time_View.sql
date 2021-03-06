CREATE OR REPLACE TABLE UTL_DIM_TIME_MONTH_PROPERTIES (
	MonthOfYear_ID INTEGER,
	MonthOfYear_Name VARCHAR(50) NULL,
	MonthOfYear_ShortName VARCHAR(50) NULL,
	MonthOfYear_SecondLangName VARCHAR(50) NULL
);

INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (1, 'January', 'Jan', 'ינואר'); 
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (2, 'Febuary', 'Feb', 'פברואר');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (3, 'March', 'Mar', 'מרץ');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (4, 'April', 'Apr', 'אפריל');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (5, 'May', 'May', 'מאי');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (6, 'June', 'Jun', 'יוני');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (7, 'July', 'Jul', 'יולי');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (8, 'August', 'Aug', 'אוגוסט');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (9, 'September', 'Sep', 'ספטמבר');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (10, 'October', 'Oct', 'אוקטובר');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (11, 'November', 'Nov', 'נובמבר');
INSERT INTO UTL_DIM_TIME_MONTH_PROPERTIES VALUES (12, 'December', 'Dec', 'דצמבר');

SELECT * FROM UTL_DIM_TIME_MONTH_PROPERTIES;


CREATE OR REPLACE TABLE UTL_DIM_TIME_WEEK_DAY_PROPERTIES (
	DayOfWeek_ID INTEGER,
	DayOfWeek_Name VARCHAR(10),
	DayOfWeek_ShortName VARCHAR(5),
	DayOfWeek_SecondLangName VARCHAR(5),
	DayOfWeek_ShortSecondLangName VARCHAR(1)
);
	
INSERT INTO UTL_DIM_TIME_WEEK_DAY_PROPERTIES VALUES (1, 'Sunday', 'Sun', 'ראשון', 'א');
INSERT INTO UTL_DIM_TIME_WEEK_DAY_PROPERTIES VALUES (2, 'Monday', 'Mon', 'שני', 'ב');
INSERT INTO UTL_DIM_TIME_WEEK_DAY_PROPERTIES VALUES (3, 'Tuesday', 'Tue', 'שלישי', 'ג');
INSERT INTO UTL_DIM_TIME_WEEK_DAY_PROPERTIES VALUES (4, 'Wednesday', 'Wed', 'רביעי', 'ד');
INSERT INTO UTL_DIM_TIME_WEEK_DAY_PROPERTIES VALUES (5, 'Thursday', 'Thr', 'חמישי', 'ה');
INSERT INTO UTL_DIM_TIME_WEEK_DAY_PROPERTIES VALUES (6, 'Friday', 'Fri', 'שישי', 'ו');
INSERT INTO UTL_DIM_TIME_WEEK_DAY_PROPERTIES VALUES (7, 'Saturday', 'Sat', 'שבת', 'ש');

SELECT * FROM UTL_DIM_TIME_WEEK_DAY_PROPERTIES;


CREATE OR REPLACE TABLE UTL_DIM_TIME (
	Date_Time TIMESTAMP_NTZ,
	Date_ID INTEGER
);

INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2006-02-20 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20060220);
INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2006-02-21 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20060221);
INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2006-06-04 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20060604);
INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2010-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20100101);
INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2010-01-02 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20100102);

INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2024-12-29 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20241229);
INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2024-12-30 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20241230);
INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2024-12-31 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20241231);
INSERT INTO UTL_DIM_TIME VALUES (to_timestamp('2025-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), 20250101);

SELECT * FROM UTL_DIM_TIME;

CREATE OR REPLACE VIEW DIM_TIME
AS
SELECT t.*, 

[FutureWeek_IND] = case when t.Week_Id >  YEAR(Current_Date_DT) * 1000 + DATEPART(wk, Current_Date_DT)	then 1	else 0	end
,CASE WHEN Year_Id = DATEPART(YEAR,GETDATE())-1 AND Week_Of_Year_Id = DATEPART(WK,GETDATE()) THEN 1 ELSE 0 end Parallel_Week_Last_Year
,CASE WHEN Year_Id = DATEPART(YEAR,GETDATE())-1 AND Fiscal_Week_Of_Year_Id = DATEPART(WK,GETDATE()) THEN 1 ELSE 0 end Fiscal_Parallel_Week_Last_Year
,CASE WHEN Year_Id = DATEPART(YEAR,GETDATE())-1 AND Month_Of_Year_Id < DATEPART(Month,GETDATE()) THEN 1 
      WHEN Year_Id = DATEPART(YEAR,GETDATE())-1 AND Month_Of_Year_Id = DATEPART(Month,GETDATE()) AND DATEPART(day,Date_Time) <= DATEPART(day,getdate()) THEN 1  
      ELSE 0 END Parallel_Date_Last_Year

FROM (
	SELECT
		--Date And Day
		Date_Time = DAYS.DATE_TIME
		,Date_Id = DAYS.DATE_ID
		,Day_Name = right('0' + CAST(DAY(DAYS.DATE_TIME) AS NVARCHAR), 2) + '/' + right('0' + CAST(MONTH(DAYS.DATE_TIME) AS NVARCHAR), 2) + '/' + CAST(YEAR(DAYS.DATE_TIME) AS NVARCHAR)
		,
		-- RTRIM(LTRIM(STR(YEAR(DAYS.DATE_TIME)))) + Case when Current_Date_ID = DAYS.DATE_ID Then ' - ' + LEFT(convert(Nvarchar(20), GETDATE(),114) ,5) Else '' End + ', ' +  MNTH.MonthOfYear_SecondLangName + ' ' +RIGHT('0' + RTRIM(LTRIM(STR(DAY(DAYS.DATE_TIME)))), 2)  ,
		Day_Second_Lang_Name = RTRIM(LTRIM(STR(YEAR(DAYS.DATE_TIME)))) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(MONTH(DAYS.DATE_TIME)))), 2) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(DAY(DAYS.DATE_TIME)))), 2) 
		--+ CASE  WHEN Current_Date_ID = DAYS.DATE_ID THEN ' - ' + LEFT(convert(NVARCHAR(20), GETDATE(), 114), 5) ELSE '' END
		,Current_Date_Id
		,Current_Date_Dt
		,Source_Last_Date = RTRIM(LTRIM(STR(YEAR(Current_Date_DT)))) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(MONTH(Current_Date_DT)))), 2) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(DAY(Current_Date_DT)))), 2) + ' - ' + LEFT(convert(NVARCHAR(20), Current_Date_DT, 114), 5)
		,Dwh_Last_Update = RTRIM(LTRIM(STR(YEAR(Last_Dwh_Update)))) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(MONTH(Last_Dwh_Update)))), 2) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(DAY(Last_Dwh_Update)))), 2) + ' - ' + LEFT(convert(NVARCHAR(20), Last_Dwh_Update, 114), 5)
		,Dim_Process_Date = RTRIM(LTRIM(STR(YEAR(GETDATE())))) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(MONTH(GETDATE())))), 2) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(DAY(GETDATE())))), 2) + ' - ' + LEFT(convert(NVARCHAR(20), GETDATE(), 114), 5)
		,
		--Month
		 [Month_Id] = YEAR(DAYS.DATE_TIME) * 100 + MONTH(DAYS.DATE_TIME)
		,[Month_Name] = RTRIM(LTRIM(STR(YEAR(DAYS.DATE_TIME)))) + '-' + RIGHT('0' + RTRIM(LTRIM(STR(MONTH(DAYS.DATE_TIME)))), 2)
		,[Month_Second_Lang_Name] = MNTH.MonthOfYear_SecondLangName + ' ' + RTRIM(LTRIM(STR(YEAR(DAYS.DATE_TIME))))
		,[Month_Days_In_Month] = Day(dateadd(day, - 1, cast(convert(VARCHAR(7), dateadd(month, 1, DAYS.DATE_TIME), 120) + '-01' AS DATETIME)))
		,[Month_Of_Year_Id] = MONTH(DAYS.DATE_TIME)
		,[Month_Of_Year_Name] = MNTH.MonthOfYear_Name
	--	,[Fiscal_Month_Id] = FST.Fiscal_Month_Id
		,
		--Quarter
		[Quarter_Id] = YEAR(DAYS.DATE_TIME) * 10 + DATEPART(q, DAYS.DATE_TIME)
		,[Quarter_Name] = RTRIM(LTRIM(STR(YEAR(DAYS.DATE_TIME)))) + ', Quarter ' + RTRIM(LTRIM(STR(DATEPART(q, DAYS.DATE_TIME))))
		,[Quarter_Of_Year_Id] = DATEPART(q, DAYS.DATE_TIME)
		,[Quarter_Of_Year_Name] = 'Quarter ' + RTRIM(LTRIM(STR(DATEPART(q, DAYS.DATE_TIME))))
	--	,[Fiscal_Quarter_Id] = FST.Fiscal_Quarter_Id
		,
		--Half Year
		Half_Year_Id = YEAR(DAYS.DATE_TIME) * 100 + 90 + (MONTH(DAYS.DATE_TIME) / 7 + 1)
		,
		--Week
		 [Week_Id] = YEAR(DAYS.DATE_TIME) * 1000 + DATEPART(wk, DAYS.DATE_TIME)
		,[Week_Name] = RTRIM(LTRIM(STR(YEAR(DAYS.DATE_TIME)))) + ', Week ' + RTRIM(LTRIM(STR(DATEPART(wk, DAYS.DATE_TIME))))
		,[Week_Of_Year_Id] = DATEPART(wk, DAYS.DATE_TIME)
		,[Week_Of_Year_Name] = 'Week ' + RTRIM(LTRIM(STR(DATEPART(wk, DAYS.DATE_TIME))))
		
		
		
		
		,--Fiscal Week_of_year
		
		 CASE WHEN UNI.Date_Groups = 1 
         THEN ( CASE WHEN DATEPART(wk, DAYS.DATE_TIME) <> 53 THEN DATEPART(wk, DAYS.DATE_TIME)
         ELSE 52 END)
         WHEN UNI.Date_Groups = 2
         THEN( CASE WHEN DATEPART(wk, DAYS.DATE_TIME) <> 1 THEN DATEPART(wk, DAYS.DATE_TIME)-1 
         ELSE 1 END ) 
         ELSE -1 END AS Fiscal_Week_Of_Year_Id
         
         --fiscal week
         ,CASE WHEN UNI.Date_Groups = 1 
         THEN ( CASE WHEN DATEPART(wk, DAYS.DATE_TIME) <> 53 THEN YEAR(DAYS.DATE_TIME) * 1000 + DATEPART(wk, DAYS.DATE_TIME)
         ELSE YEAR(DAYS.DATE_TIME) * 1000 + 52 END)
         WHEN UNI.Date_Groups = 2
         THEN( CASE WHEN DATEPART(wk, DAYS.DATE_TIME) <> 1 THEN YEAR(DAYS.DATE_TIME) * 1000 + DATEPART(wk, DAYS.DATE_TIME)-1 
         ELSE YEAR(DAYS.DATE_TIME) * 1000 + 1 END ) 
         ELSE -1 END AS Fiscal_Week
		  
		--Year
		,[Year_Id] = YEAR(DAYS.DATE_TIME)
		,[Year_Name] = RTRIM(LTRIM(STR(YEAR(DAYS.DATE_TIME))))
		,
		--DAY OF WEEK
		Day_Of_Week_Id = DATEPART(dw, DAYS.DATE_TIME)
		,Day_Of_Week_Name = DOW.[DayOfWeek_Name]
		,Day_Of_Week_Short_Name = DOW.[DayOfWeek_ShortName]
		,Num_Of_Days_Of_Week = NOD.Num_Of_Days_Week
		,CASE 
			WHEN NOD.Num_Of_Days_Week = 7
				THEN 1
			ELSE 0
			END AS Is_Full_Week
		,CASE 
			WHEN NOD.Num_Of_Days_Week = 7
				THEN 'Full Week'
			ELSE 'Partial Week'
			END AS Is_Full_Week_Desc
		,
		--Day Of...
		[Day_Of_Month_Id] = DAY(DAYS.DATE_TIME)
		,[Day_Of_Quarter] = DATEDIFF(d, DATEADD(qq, DATEDIFF(qq, 0, DAYS.DATE_TIME), 0), DAYS.DATE_TIME) + 1
		,[Day_Of_Year_Id] = DATEPART(dayofyear, DAYS.DATE_TIME)
		,
		--indicators
		CASE 
			WHEN DAYS.DATE_TIME = DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE()))
				THEN 1
			ELSE 0
			END AS [CurrentDay_IND]
		,CASE 
			WHEN YEAR(DAYS.DATE_TIME) * 1000 + DATEPART(wk, DAYS.DATE_TIME) = YEAR(getdate()) * 1000 + DATEPART(wk, getdate())
				THEN 1
			ELSE 0
			END AS [CurrentWeek_IND]
		,CASE 
			WHEN YEAR(DAYS.DATE_TIME) * 100 + MONTH(DAYS.DATE_TIME) = YEAR(getdate()) * 100 + MONTH(getdate())
				THEN 1
			ELSE 0
			END AS [CurrentMonth_IND]
		,CASE 
			WHEN YEAR(DAYS.DATE_TIME) * 10 + DATEPART(q, DAYS.DATE_TIME) = YEAR(getdate()) * 10 + DATEPART(q, getdate())
				THEN 1
			ELSE 0
			END AS [CurrentQuarter_IND]
		,CASE 
			WHEN YEAR(DAYS.DATE_TIME) = YEAR(getdate())
				THEN 1
			ELSE 0
			END AS [CurrentYear_IND]
		,CASE 
			WHEN DAYS.DATE_TIME > DATEADD(dd, 0, DATEDIFF(dd, 0, GETDATE()))
				THEN 1
			ELSE 0
			END AS [FutureDay_IND]
		,CASE
			WHEN CONVERT(NVARCHAR(10),DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,DAYS.DATE_TIME)+1,0)),121) = CONVERT(NVARCHAR(10),DAYS.DATE_TIME,121)
				Then 1
			ELSE 0
			END AS [Is_Last_Day_Of_Month]
	
		--case when Campaign_Name is not null then 1 else 0 end as Campaign_Ind
	--	--Holidays
	--	,CASE 
	--		WHEN JEW.Name_Heb IS NOT NULL
	--			THEN JEW.Name_Heb
	--		ELSE '-'
	--		END AS Holiday_Name
	--	,CASE 
	--		WHEN JEW.Name_Heb IS NOT NULL
	--			THEN 1
	--		ELSE 0
	--		END AS Holiday_Ind
	--	,COALESCE(JEW.Half_Day_Ind,0) AS Half_Day_Ind
	--	,CASE WHEN JEW.Half_Day_Ind = 1 THEN '??? ????'
	--	ELSE '??? ???' END AS Half_Day_Ind_Name
 --       ,ISNULL(JEWISH.Holiday_Per_Week_Name,'-') AS Holiday_Per_Week_Name
	--	-- case when Holiday_Name is not null then 1 else 0 end as Holiday_Ind
	--	--Special_Exogenous_Events
	--	,CASE 
	--		WHEN LEN(EXE.Special_Exogenous_Event_Name) > 0
	--			THEN EXE.Special_Exogenous_Event_Name
	--		ELSE '-'
	--		END AS Special_Exogenous_Event_Name
	--	,CASE 
	--		WHEN LEN(EXE.Special_Exogenous_Event_Name) > 0
	--			THEN CAST(1 AS SMALLINT)
	--		ELSE CAST(0 AS SMALLINT)
	--		END Special_Exogenous_Event_Ind
	---- case when Special_Exogenous_Event_Name is not null then 1 else 0 end as Special_Exogenous_Event_Ind
	
	FROM UTL_DIM_TIME DAYS
	LEFT OUTER JOIN UTL_DIM_TIME_MONTH_PROPERTIES AS MNTH ON MONTH(DAYS.DATE_TIME) = MNTH.MonthOfYear_ID
	LEFT OUTER JOIN [Admin].[Dim_Time_Week_Day_Properties] AS DOW ON DATEPART(dw, DAYS.DATE_TIME) = DOW.DayOfWeek_ID
	--LEFT OUTER JOIN [Admin].[Dim_Jewish_Holidays] AS JEW ON JEW.DATE = DAYS.DATE_TIME
	
	LEFT OUTER JOIN (
				
		    SELECT Date_Time, Date_Groups, Num_Of_Days_Week
			FROM 
			(SELECT TIM.Date_Time,GRP.Num_Of_Days_Week,
					CASE WHEN day(Date_Time) = 1 AND MONTH(Date_Time) = 1 
						THEN (CASE WHEN Num_Of_Days_Week >= 4 THEN 1
						ELSE 2 END) 
						ELSE -1 END AS Date_Groups
			FROM
			Utl.Dim_Time TIM
			LEFT JOIN 
			(SELECT COUNT(*) AS Num_Of_Days_Week
						,YEAR(DATE_TIME) AS Year_Id
						,DATEPART(wk,DATE_TIME) AS Week_Id
					FROM Utl.Dim_Time DAYS
					GROUP BY YEAR(DATE_TIME),DATEPART(wk,DATE_TIME)) GRP

			ON YEAR(TIM.DATE_TIME) = GRP.Year_Id AND DATEPART(wk,TIM.DATE_TIME) = GRP. Week_Id) TOT
			WHERE Date_Groups = 1 OR Date_Groups = 2) UNI 
			ON YEAR(UNI.Date_Time) = YEAR(DAYS.DATE_TIME)
			
			
		LEFT OUTER JOIN (
		SELECT COUNT(*) AS Num_Of_Days_Week
			,YEAR(DAYS.DATE_TIME) AS Year_Id
			,DATEPART(wk, DAYS.DATE_TIME) AS Week_Id
		FROM Utl.Dim_Time DAYS
		GROUP BY YEAR(DAYS.DATE_TIME)
			,DATEPART(wk, DAYS.DATE_TIME)
		) NOD ON NOD.Year_Id = YEAR(DAYS.DATE_TIME)
		AND NOD.Week_Id = DATEPART(wk, DAYS.DATE_TIME)
		
	
	CROSS JOIN (
		SELECT cast(convert(VARCHAR, getdate(), 112) AS INT) AS Current_Date_ID
			,getdate() AS Current_Date_DT
			,convert(VARCHAR, getdate(), 108) Last_Dwh_Update
		) a
	) T
;