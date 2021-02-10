-- inkrementalno, TO tabele kot view-i
-- varianta (2)
-- v pogoju imamo konstanto

create or replace view TV_MET_ZAVAROVALNI_PROD_A_26 as 
select src.ID, src.VKA_ID, src.KTJ_ID, src.IZJEMA_UPORABE, src.SIFRA, src.NAZIV, src.VELJAVNOST_ZACETEK, src.VELJAVNOST_POTEK, src.DATSPR, src.DATVNO,
    src.UPOSPR, src.UPOVNO, src.DAN_ZACETKA_DOK,src.OBLIKA_DATUMA_DOK, src.LIMIT_POTEKA_OO, src.ZAVAROVALNA_SKUPINA, src.VRSTNI_RED_KZZ,
    src.AVT_ODPIRANJE_OO, src.PRIVZETO_NOVO, src.STORNO_INSTANCA, src.VRSTA_STORNA, src.FAKTURIRAJ, src.SPREMINJANJE_UR, src.REDNO_PRENEHANJE,
    src.RENTNI_PRODUKT, src.UPOSTEVAJ_PREMIJO, src.ST_MESECEV_DO_PODALJSANJA, src.ETL_HASH, 1 as ID_LOAD ,src.SYS_DT, src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."VKA_ID",src."KTJ_ID",src."IZJEMA_UPORABE",src."SIFRA",src."NAZIV",src."VELJAVNOST_ZACETEK",src."VELJAVNOST_POTEK",src."DATSPR",src."DATVNO",src."UPOSPR",src."UPOVNO",src."DAN_ZACETKA_DOK",src."OBLIKA_DATUMA_DOK",src."LIMIT_POTEKA_OO",src."ZAVAROVALNA_SKUPINA",src."VRSTNI_RED_KZZ",src."AVT_ODPIRANJE_OO",src."PRIVZETO_NOVO",src."STORNO_INSTANCA",src."VRSTA_STORNA",src."FAKTURIRAJ",src."SPREMINJANJE_UR",src."REDNO_PRENEHANJE",src."RENTNI_PRODUKT",src."UPOSTEVAJ_PREMIJO",src."ST_MESECEV_DO_PODALJSANJA"
             from AR_MET_ZAVAROVALNI_PRODUKT_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_OZP_INSTANCA_SKLEP_DOK_A_26 as 
select src.ID,src.NSE_ID,src.TSK_ID,src.POC_ID_POOBLASCENEC,src.PEA_ID,src.OSA_ID_ZAVAROVALEC,src.OZZ_ID,src.SDT_ID,src.ZPT_ID,src.ZTK_ID,src.NZO_ID,src.ZST,src.SDT_ID_KOL_POGODBA,src.SDT_ID_PONUDBA,src.OBLIKA_ZAVAROVANJA,src.DATUM_IZDELAVE,src.VEC_PLACNIKOV,src.RAZLICNA_VPLACILA_ZAV,src.KONCAN_VNOS,src.AKTIVEN,src.OBNOVLJEN,src.DATVNO,src.UPOVNO,src.NZA_ID,src.ZZK_ID,src.TOC_ID,src.SZK_ID,src.OVE_ID,src.ZPE_ID,src.ZSO_ID,src.TSK_ID_STARI,src.KON_STEVILKA_DOK,src.KON_STEVILKA_OSNDOK,src.TSK_ID_PRETHODNI_PRIPADA,src.OEA_ID,src.POC_ID,src.POC_ID_PROV,src.KKA_ID,src.TJE_ID,src.OZZ_ID_ZAVAROVANEC,src.OSA_ID,src.VLA_ID,src.STEVILKA_DOKUMENTA,src.STEVILKA_PAKETA,src.STEVILKA_OBRAZCA,src.STEVILKA_PREDHODNEGA,src.VELJAVNOST_ZAC_DOK,src.DATUM_OD_DOK,src.URA_OD_DOK,src.MINUTA_OD_DOK,src.VELJAVNOST_POT_DOK,src.DATUM_DO_DOK,src.URA_DO_DOK,src.MINUTA_DO_DOK,src.DATUM_PODALJSANJA,src.RAZLICNI_KRAJI_ZAV,src.POZAVAROVANJE,src.SOZAVAROVANJE,src.VELJAVNOST_ZAC_KRIT,src.DATUM_OD_KRIT,src.URA_OD_KRIT,src.MINUTA_OD_KRIT,src.VELJAVNOST_POT_KRIT,src.DATUM_DO_KRIT,src.URA_DO_KRIT,src.MINUTA_DO_KRIT,src.VELJAVNOST_ZAC_OO,src.DATUM_OD_OO,src.URA_OD_OO,src.MINUTA_OD_OO,src.VELJAVNOST_POT_OO,src.DATUM_DO_OO,src.URA_DO_OO,src.MINUTA_DO_OO,src.IZPISANI_DOD_TEKST,src.TIP_CENIKA,src.UPOSPR,src.DATSPR,src.ZACETEK_ROK_VP,src.POTEK_ROKA_VP,src.OPIS,src.VINKULIRAN,src.STORNO_OD_DNE,src.NOVO,src.VSTOPNO_STEV_ZAV,src.PRVA_VKLJUCITEV_PDPZ,src.PREDV_STAROST_UPOKOJ,src.VSTOPNA_BRUTO_PLACA,src.SSK_ID,src.DATUM_SKADENCE,src.PKL_ID,src.POC_ID_POOBL_POSREDNIKA,src.VSK_ID,src.ROCNO_JAMSTVO,src.SDT_ID_OSNOVNI,src.DZA_ID,src.DOGOVOR_O_BONUSU,src.TRAJANJE_OO,src.IPVNO,src.IPSPR,src.UPOVNO_POROCANJE,src.UPOSPR_POROCANJE,src.TJE_ID_PODALJSANJE,src.AVTOMATSKO_PODALJSANJE,src.PVA_ID,src.TJE_ID_POGODBA,src.VELJAVNOST_POT_POG,src.BPA_ID,src.OBSEG_KRITJA,src.DVZ_ID,src.PTF_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."NSE_ID",src."TSK_ID",src."POC_ID_POOBLASCENEC",src."PEA_ID",src."OSA_ID_ZAVAROVALEC",src."OZZ_ID",src."SDT_ID",src."ZPT_ID",src."ZTK_ID",src."NZO_ID",src."ZST",src."SDT_ID_KOL_POGODBA",src."SDT_ID_PONUDBA",src."OBLIKA_ZAVAROVANJA",src."DATUM_IZDELAVE",src."VEC_PLACNIKOV",src."RAZLICNA_VPLACILA_ZAV",src."KONCAN_VNOS",src."AKTIVEN",src."OBNOVLJEN",src."DATVNO",src."UPOVNO",src."NZA_ID",src."ZZK_ID",src."TOC_ID",src."SZK_ID",src."OVE_ID",src."ZPE_ID",src."ZSO_ID",src."TSK_ID_STARI",src."KON_STEVILKA_DOK",src."KON_STEVILKA_OSNDOK",src."TSK_ID_PRETHODNI_PRIPADA",src."OEA_ID",src."POC_ID",src."POC_ID_PROV",src."KKA_ID",src."TJE_ID",src."OZZ_ID_ZAVAROVANEC",src."OSA_ID",src."VLA_ID",src."STEVILKA_DOKUMENTA",src."STEVILKA_PAKETA",src."STEVILKA_OBRAZCA",src."STEVILKA_PREDHODNEGA",src."VELJAVNOST_ZAC_DOK",src."DATUM_OD_DOK",src."URA_OD_DOK",src."MINUTA_OD_DOK",src."VELJAVNOST_POT_DOK",src."DATUM_DO_DOK",src."URA_DO_DOK",src."MINUTA_DO_DOK",src."DATUM_PODALJSANJA",src."RAZLICNI_KRAJI_ZAV",src."POZAVAROVANJE",src."SOZAVAROVANJE",src."VELJAVNOST_ZAC_KRIT",src."DATUM_OD_KRIT",src."URA_OD_KRIT",src."MINUTA_OD_KRIT",src."VELJAVNOST_POT_KRIT",src."DATUM_DO_KRIT",src."URA_DO_KRIT",src."MINUTA_DO_KRIT",src."VELJAVNOST_ZAC_OO",src."DATUM_OD_OO",src."URA_OD_OO",src."MINUTA_OD_OO",src."VELJAVNOST_POT_OO",src."DATUM_DO_OO",src."URA_DO_OO",src."MINUTA_DO_OO",src."IZPISANI_DOD_TEKST",src."TIP_CENIKA",src."UPOSPR",src."DATSPR",src."ZACETEK_ROK_VP",src."POTEK_ROKA_VP",src."OPIS",src."VINKULIRAN",src."STORNO_OD_DNE",src."NOVO",src."VSTOPNO_STEV_ZAV",src."PRVA_VKLJUCITEV_PDPZ",src."PREDV_STAROST_UPOKOJ",src."VSTOPNA_BRUTO_PLACA",src."SSK_ID",src."DATUM_SKADENCE",src."PKL_ID",src."POC_ID_POOBL_POSREDNIKA",src."VSK_ID",src."ROCNO_JAMSTVO",src."SDT_ID_OSNOVNI",src."DZA_ID",src."DOGOVOR_O_BONUSU",src."TRAJANJE_OO",src."IPVNO",src."IPSPR",src."UPOVNO_POROCANJE",src."UPOSPR_POROCANJE",src."TJE_ID_PODALJSANJE",src."AVTOMATSKO_PODALJSANJE",src."PVA_ID",src."TJE_ID_POGODBA",src."VELJAVNOST_POT_POG",src."BPA_ID",src."OBSEG_KRITJA",src."DVZ_ID",src."PTF_ID"
             from AR_OZP_INSTANCA_SKLEP_DOK_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_OZP_OBJEKT_POSTAVKA_SD_A_26 as 
select src.ID,src.PSD_ID,src.OSK_ID,src.UPOVNO,src.DATVNO,src.ZAVAROVALNA_VSOTA,src.PREMIJA,src.DATSPR,src.UPOSPR,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."PSD_ID",src."OSK_ID",src."UPOVNO",src."DATVNO",src."ZAVAROVALNA_VSOTA",src."PREMIJA",src."DATSPR",src."UPOSPR"
             from AR_OZP_OBJEKT_POSTAVKA_SD_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_OZP_OBJEKT_SKLEPDOK_A_26 as 
select src.ID,src.ISK_ID,src.VELJAVNOST_OD,src.UPOVNO,src.DATVNO,src.OZO_ID,src.OBT_ID,src.OZZ_ID,src.KZA_ID,src.OSA_ID,src.VELJAVNOST_DO,src.DATSPR,src.UPOSPR,src.ZST,src.SDT_ID,src.VOZ_ID,src.VZO_ID,src.OPIS,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."ISK_ID",src."VELJAVNOST_OD",src."UPOVNO",src."DATVNO",src."OZO_ID",src."OBT_ID",src."OZZ_ID",src."KZA_ID",src."OSA_ID",src."VELJAVNOST_DO",src."DATSPR",src."UPOSPR",src."ZST",src."SDT_ID",src."VOZ_ID",src."VZO_ID",src."OPIS"
             from AR_OZP_OBJEKT_SKLEPDOK_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_OZP_POSTAVKA_SKLEP_DOK_A_26 as 
select src.ID,src.VLA_ID_PREMIJA,src.ZVA_ID,src.SDT_ID,src.ISK_ID,src.ZVE_ID,src.VLA_ID,src.OVE_ID_VSOTA,src.OVE_ID,src.ZCK_ID,src.ZST,src.VELJAVNOST_ZAC_KRIT,src.UPOVNO,src.DATVNO,src.NZA_ID,src.TJE_ID,src.KZA_ID,src.SZK_ID,src.ZNT_ID,src.VELJAVNOST_POT_KRIT,src.OPIS,src.POZAVAROVANJE,src.SOZAVAROVANJE,src.VELJAVNOST_ZAC_OO,src.VELJAVNOST_POT_OO,src.ZAVAROVALNA_VSOTA,src.ZNESEK_OSNOVE,src.PREMIJA_BREZ_DOD,src.PREMIJA_Z_DOD,src.KOLICINSKI_FAKTOR,src.KOLICINA_ENAKIH_OBJEKTOV,src.PREMIJSKA_STOPNJA,src.FIKSNA_PREMIJA,src.FAKTOR_OO,src.TIP_FAKTORJA_OO,src.TIP_CENIKA,src.KARENCA,src.DATUM_DO_KARENCE,src.ZACETEK_JAMSTVA,src.POTEK_JAMSTVA,src.DAN_ZAPADLOSTI_ZL,src.DATSPR,src.UPOSPR,src.FSA_ID,src.DATUM_OD_KRIT,src.URA_OD_KRIT,src.MINUTA_OD_KRIT,src.DATUM_DO_KRIT,src.URA_DO_KRIT,src.MINUTA_DO_KRIT,src.DATUM_OD_OO,src.URA_OD_OO,src.MINUTA_OD_OO,src.URA_DO_OO,src.MINUTA_DO_OO,src.DATUM_DO_OO,src.KON_DAVEK_ZNESEK,src.KON_DAVEK_ODSTOTEK,src.STATUS_TKS,src.OSNOVNA_PREMIJA,src.ZLP_ID,src.NOVO,src.PREMIJA_Z_VSEMI_DOD,src.NETO_PREMIJA,src.TCJ_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."VLA_ID_PREMIJA",src."ZVA_ID",src."SDT_ID",src."ISK_ID",src."ZVE_ID",src."VLA_ID",src."OVE_ID_VSOTA",src."OVE_ID",src."ZCK_ID",src."ZST",src."VELJAVNOST_ZAC_KRIT",src."UPOVNO",src."DATVNO",src."NZA_ID",src."TJE_ID",src."KZA_ID",src."SZK_ID",src."ZNT_ID",src."VELJAVNOST_POT_KRIT",src."OPIS",src."POZAVAROVANJE",src."SOZAVAROVANJE",src."VELJAVNOST_ZAC_OO",src."VELJAVNOST_POT_OO",src."ZAVAROVALNA_VSOTA",src."ZNESEK_OSNOVE",src."PREMIJA_BREZ_DOD",src."PREMIJA_Z_DOD",src."KOLICINSKI_FAKTOR",src."KOLICINA_ENAKIH_OBJEKTOV",src."PREMIJSKA_STOPNJA",src."FIKSNA_PREMIJA",src."FAKTOR_OO",src."TIP_FAKTORJA_OO",src."TIP_CENIKA",src."KARENCA",src."DATUM_DO_KARENCE",src."ZACETEK_JAMSTVA",src."POTEK_JAMSTVA",src."DAN_ZAPADLOSTI_ZL",src."DATSPR",src."UPOSPR",src."FSA_ID",src."DATUM_OD_KRIT",src."URA_OD_KRIT",src."MINUTA_OD_KRIT",src."DATUM_DO_KRIT",src."URA_DO_KRIT",src."MINUTA_DO_KRIT",src."DATUM_OD_OO",src."URA_OD_OO",src."MINUTA_OD_OO",src."URA_DO_OO",src."MINUTA_DO_OO",src."DATUM_DO_OO",src."KON_DAVEK_ZNESEK",src."KON_DAVEK_ODSTOTEK",src."STATUS_TKS",src."OSNOVNA_PREMIJA",src."ZLP_ID",src."NOVO",src."PREMIJA_Z_VSEMI_DOD",src."NETO_PREMIJA",src."TCJ_ID"
             from AR_OZP_POSTAVKA_SKLEP_DOK_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_OZP_SKLEPALNI_DOKUMENT_A_26 as 
select src.ID,src.TSK_ID,src.STEVILKA_DOKUMENTA,src.ZSO_ID,src.ZPT_ID,src.TSK_ID_STARI,src.KON_STEVILKA_DOK,src.KON_STEVILKA_OSNDOK,src.DATVNO,src.UPOVNO,src.DATSPR,src.UPOSPR,src.PODPISI,src.KRATKA_STEVILKA,src.IPVNO,src.IPSPR,src.UPOVNO_POROCANJE,src.UPOSPR_POROCANJE,src.STEVILKA_POTRDILA,src.NADOMESTNI_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."TSK_ID",src."STEVILKA_DOKUMENTA",src."ZSO_ID",src."ZPT_ID",src."TSK_ID_STARI",src."KON_STEVILKA_DOK",src."KON_STEVILKA_OSNDOK",src."DATVNO",src."UPOVNO",src."DATSPR",src."UPOSPR",src."PODPISI",src."KRATKA_STEVILKA",src."IPVNO",src."IPSPR",src."UPOVNO_POROCANJE",src."UPOSPR_POROCANJE",src."STEVILKA_POTRDILA",src."NADOMESTNI_ID"
             from AR_OZP_SKLEPALNI_DOKUMENT_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_OZP_ZAHT_SPR_SKLEPDOK_A_26 as 
select src.ID,src.TZO_ID,src.SDT_ID,src.ZST,src.DATUM_ZAHTEVKA,src.ISK_ID,src.VZD_ID,src.OPIS,src.SPREMEMBA_SPREJEMLJIVA,src.OBLIKA_SPREMEMBE,src.NACIN_SPREMEMBE,src.VELJAVNOST_SPR_OD,src.VELJAVNOST_SPR_DO,src.DATUM_DOGODKA,src.UPOSPR,src.DATSPR,src.UPOVNO,src.DATVNO,src.ISK_ID_NOV,src.SDT_ID_NOV,src.NACIN_MIROVANJA,src.IPVNO,src.IPSPR,src.SZK_ID,src.EID,src.TOC_ID,src.STEVILKA_OBRAZCA,src.RGR_ID,src.OBRAC_VRA_TRAJ_POP,src.OBRAC_ADM_STROSKE,src.OPIS_2,src.DVA_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."TZO_ID",src."SDT_ID",src."ZST",src."DATUM_ZAHTEVKA",src."ISK_ID",src."VZD_ID",src."OPIS",src."SPREMEMBA_SPREJEMLJIVA",src."OBLIKA_SPREMEMBE",src."NACIN_SPREMEMBE",src."VELJAVNOST_SPR_OD",src."VELJAVNOST_SPR_DO",src."DATUM_DOGODKA",src."UPOSPR",src."DATSPR",src."UPOVNO",src."DATVNO",src."ISK_ID_NOV",src."SDT_ID_NOV",src."NACIN_MIROVANJA",src."IPVNO",src."IPSPR",src."SZK_ID",src."EID",src."TOC_ID",src."STEVILKA_OBRAZCA",src."RGR_ID",src."OBRAC_VRA_TRAJ_POP",src."OBRAC_ADM_STROSKE",src."OPIS_2",src."DVA_ID"
             from AR_OZP_ZAHT_SPREM_SKLEPDOK_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;

create or replace view TV_OZP_ZGO_ST_SKLEPDOK_A_26 as 
select src.ID,src.ISK_ID,src.SSK_ID,src.SDT_ID,src.ZST,src.UPOVNO,src.DATVNO,src.ZSO_ID,src.VELJAVNOST_DOK_DO,src.DATSPR,src.UPOSPR,src.ZSK_ID_PREDHODI,src.DATUM_STATUSA,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."ISK_ID",src."SSK_ID",src."SDT_ID",src."ZST",src."UPOVNO",src."DATVNO",src."ZSO_ID",src."VELJAVNOST_DOK_DO",src."DATSPR",src."UPOSPR",src."ZSK_ID_PREDHODI",src."DATUM_STATUSA"
             from AR_OZP_ZGO_STATUSA_SKLEPDOK_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_PRODUCT_26 as 
select src.ID_PRODUCT,src.TP_BSN,src.PRD_IT,src.PRD,src.ID_CDE_TP_RCD,src.ID_CDE_ST,src.SDSC_SLO,src.DSC_SLO,src.SDSC,src.DSC,src.DT_BEG,src.DT_END,src.CREATED_BY,src.CREATED_ON,src.MODIFIED_BY,src.MODIFIED_ON,src.ID_COST_OWNER,src.COST_OWNER,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID_PRODUCT",src."TP_BSN",src."PRD_IT",src."PRD",src."ID_CDE_TP_RCD",src."ID_CDE_ST",src."SDSC_SLO",src."DSC_SLO",src."SDSC",src."DSC",src."DT_BEG",src."DT_END",src."CREATED_BY",src."CREATED_ON",src."MODIFIED_BY",src."MODIFIED_ON",src."ID_COST_OWNER",src."COST_OWNER"
             from AR_PRODUCT src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;

create or replace view TV_PRS_STATUS_SKLEP_DOK_A_26 as 
select src.ID,src.SIFRA,src.NAZIV,src.BILTEN,src.PROVIZIJA,src.VNOS_BREZ_ZAHTEVKA,src.DOKUMENT_ZAKLJUCEN,src.STORNO,src.STORNO_ZAKLJUCEN,src.UPOVNO,src.DATVNO,src.UPOSPR,src.DATSPR,src.SPREM_BREZ_ZAHTEVKA,src.FAKTURIRATI,src.KONCNI,src.OBRACUN_MR,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."SIFRA",src."NAZIV",src."BILTEN",src."PROVIZIJA",src."VNOS_BREZ_ZAHTEVKA",src."DOKUMENT_ZAKLJUCEN",src."STORNO",src."STORNO_ZAKLJUCEN",src."UPOVNO",src."DATVNO",src."UPOSPR",src."DATSPR",src."SPREM_BREZ_ZAHTEVKA",src."FAKTURIRATI",src."KONCNI",src."OBRACUN_MR"
             from AR_PRS_STATUS_SKLEP_DOK_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
            
create or replace view TV_PRS_ZAVAROVANJE_A_26 as 
select src.ID,src.ZVA_ID,src.SIFRA,src.NAZIV,src.DATVNO,src.UPOVNO,src.ZCK_ID,src.ZVE_ID,src.IZPISANI_NAZIV,src.VELJAVNOST_ZACETEK,src.VELJAVNOST_POTEK,src.UPOSPR,src.DATSPR,src.OZNAKA,src.SIFRA_ZAVAROVANJA,src.UREJENSTAT_OZZ_POGOJ,src.KZZ_ZAVAROVANJE,src.KZZ_SIFRA,src.KZZ_VSOTA,src.NALOZBENO,src.OBLIKA_KONVERZIJE,src.PRIKAZI,src.OSNOVA_OCP,src.VRSTA_KRITJA,src.GRUPIRAJ,src.ZUNANJA_SIFRA,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."ZVA_ID",src."SIFRA",src."NAZIV",src."DATVNO",src."UPOVNO",src."ZCK_ID",src."ZVE_ID",src."IZPISANI_NAZIV",src."VELJAVNOST_ZACETEK",src."VELJAVNOST_POTEK",src."UPOSPR",src."DATSPR",src."OZNAKA",src."SIFRA_ZAVAROVANJA",src."UREJENSTAT_OZZ_POGOJ",src."KZZ_ZAVAROVANJE",src."KZZ_SIFRA",src."KZZ_VSOTA",src."NALOZBENO",src."OBLIKA_KONVERZIJE",src."PRIKAZI",src."OSNOVA_OCP",src."VRSTA_KRITJA",src."GRUPIRAJ",src."ZUNANJA_SIFRA"
             from AR_PRS_ZAVAROVANJE_A src
             WHERE  sys_dt < to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS')
AND (to_dt >= to_date('29.07.2020 00:00:00','DD.MM.YYYY HH24:MI:SS'))
            )SRC;
