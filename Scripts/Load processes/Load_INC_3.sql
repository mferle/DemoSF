-- inkrementalno, TO tabele kot view-i
-- varianta (3)
-- naredimo enovrstično tabelo z datumom
-- v pogoju imamo cross join z enovrstično tabelo


CREATE OR REPLACE TABLE adm.ADM_INC_LOAD_DT
(
    load_date datetime
);

insert into adm.ADM_INC_LOAD_DT VALUES ('2020-07-29');
--select * from adm.ADM_INC_LOAD_DT;


create or replace view TV_MET_ZAVAROVALNI_PROD_A_26 as 
select src.ID, src.VKA_ID, src.KTJ_ID, src.IZJEMA_UPORABE, src.SIFRA, src.NAZIV, src.VELJAVNOST_ZACETEK, src.VELJAVNOST_POTEK, src.DATSPR, src.DATVNO,
    src.UPOSPR, src.UPOVNO, src.DAN_ZACETKA_DOK,src.OBLIKA_DATUMA_DOK, src.LIMIT_POTEKA_OO, src.ZAVAROVALNA_SKUPINA, src.VRSTNI_RED_KZZ,
    src.AVT_ODPIRANJE_OO, src.PRIVZETO_NOVO, src.STORNO_INSTANCA, src.VRSTA_STORNA, src.FAKTURIRAJ, src.SPREMINJANJE_UR, src.REDNO_PRENEHANJE,
    src.RENTNI_PRODUKT, src.UPOSTEVAJ_PREMIJO, src.ST_MESECEV_DO_PODALJSANJA, src.ETL_HASH, 1 as ID_LOAD ,src.SYS_DT, src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."VKA_ID",src."KTJ_ID",src."IZJEMA_UPORABE",src."SIFRA",src."NAZIV",src."VELJAVNOST_ZACETEK",src."VELJAVNOST_POTEK",src."DATSPR",src."DATVNO",src."UPOSPR",src."UPOVNO",src."DAN_ZACETKA_DOK",src."OBLIKA_DATUMA_DOK",src."LIMIT_POTEKA_OO",src."ZAVAROVALNA_SKUPINA",src."VRSTNI_RED_KZZ",src."AVT_ODPIRANJE_OO",src."PRIVZETO_NOVO",src."STORNO_INSTANCA",src."VRSTA_STORNA",src."FAKTURIRAJ",src."SPREMINJANJE_UR",src."REDNO_PRENEHANJE",src."RENTNI_PRODUKT",src."UPOSTEVAJ_PREMIJO",src."ST_MESECEV_DO_PODALJSANJA"
             from AR_MET_ZAVAROVALNI_PRODUKT_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_OZP_INSTANCA_SKLEP_DOK_A_26 as 
select src.ID,src.NSE_ID,src.TSK_ID,src.POC_ID_POOBLASCENEC,src.PEA_ID,src.OSA_ID_ZAVAROVALEC,src.OZZ_ID,src.SDT_ID,src.ZPT_ID,src.ZTK_ID,src.NZO_ID,src.ZST,src.SDT_ID_KOL_POGODBA,src.SDT_ID_PONUDBA,src.OBLIKA_ZAVAROVANJA,src.DATUM_IZDELAVE,src.VEC_PLACNIKOV,src.RAZLICNA_VPLACILA_ZAV,src.KONCAN_VNOS,src.AKTIVEN,src.OBNOVLJEN,src.DATVNO,src.UPOVNO,src.NZA_ID,src.ZZK_ID,src.TOC_ID,src.SZK_ID,src.OVE_ID,src.ZPE_ID,src.ZSO_ID,src.TSK_ID_STARI,src.KON_STEVILKA_DOK,src.KON_STEVILKA_OSNDOK,src.TSK_ID_PRETHODNI_PRIPADA,src.OEA_ID,src.POC_ID,src.POC_ID_PROV,src.KKA_ID,src.TJE_ID,src.OZZ_ID_ZAVAROVANEC,src.OSA_ID,src.VLA_ID,src.STEVILKA_DOKUMENTA,src.STEVILKA_PAKETA,src.STEVILKA_OBRAZCA,src.STEVILKA_PREDHODNEGA,src.VELJAVNOST_ZAC_DOK,src.DATUM_OD_DOK,src.URA_OD_DOK,src.MINUTA_OD_DOK,src.VELJAVNOST_POT_DOK,src.DATUM_DO_DOK,src.URA_DO_DOK,src.MINUTA_DO_DOK,src.DATUM_PODALJSANJA,src.RAZLICNI_KRAJI_ZAV,src.POZAVAROVANJE,src.SOZAVAROVANJE,src.VELJAVNOST_ZAC_KRIT,src.DATUM_OD_KRIT,src.URA_OD_KRIT,src.MINUTA_OD_KRIT,src.VELJAVNOST_POT_KRIT,src.DATUM_DO_KRIT,src.URA_DO_KRIT,src.MINUTA_DO_KRIT,src.VELJAVNOST_ZAC_OO,src.DATUM_OD_OO,src.URA_OD_OO,src.MINUTA_OD_OO,src.VELJAVNOST_POT_OO,src.DATUM_DO_OO,src.URA_DO_OO,src.MINUTA_DO_OO,src.IZPISANI_DOD_TEKST,src.TIP_CENIKA,src.UPOSPR,src.DATSPR,src.ZACETEK_ROK_VP,src.POTEK_ROKA_VP,src.OPIS,src.VINKULIRAN,src.STORNO_OD_DNE,src.NOVO,src.VSTOPNO_STEV_ZAV,src.PRVA_VKLJUCITEV_PDPZ,src.PREDV_STAROST_UPOKOJ,src.VSTOPNA_BRUTO_PLACA,src.SSK_ID,src.DATUM_SKADENCE,src.PKL_ID,src.POC_ID_POOBL_POSREDNIKA,src.VSK_ID,src.ROCNO_JAMSTVO,src.SDT_ID_OSNOVNI,src.DZA_ID,src.DOGOVOR_O_BONUSU,src.TRAJANJE_OO,src.IPVNO,src.IPSPR,src.UPOVNO_POROCANJE,src.UPOSPR_POROCANJE,src.TJE_ID_PODALJSANJE,src.AVTOMATSKO_PODALJSANJE,src.PVA_ID,src.TJE_ID_POGODBA,src.VELJAVNOST_POT_POG,src.BPA_ID,src.OBSEG_KRITJA,src.DVZ_ID,src.PTF_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."NSE_ID",src."TSK_ID",src."POC_ID_POOBLASCENEC",src."PEA_ID",src."OSA_ID_ZAVAROVALEC",src."OZZ_ID",src."SDT_ID",src."ZPT_ID",src."ZTK_ID",src."NZO_ID",src."ZST",src."SDT_ID_KOL_POGODBA",src."SDT_ID_PONUDBA",src."OBLIKA_ZAVAROVANJA",src."DATUM_IZDELAVE",src."VEC_PLACNIKOV",src."RAZLICNA_VPLACILA_ZAV",src."KONCAN_VNOS",src."AKTIVEN",src."OBNOVLJEN",src."DATVNO",src."UPOVNO",src."NZA_ID",src."ZZK_ID",src."TOC_ID",src."SZK_ID",src."OVE_ID",src."ZPE_ID",src."ZSO_ID",src."TSK_ID_STARI",src."KON_STEVILKA_DOK",src."KON_STEVILKA_OSNDOK",src."TSK_ID_PRETHODNI_PRIPADA",src."OEA_ID",src."POC_ID",src."POC_ID_PROV",src."KKA_ID",src."TJE_ID",src."OZZ_ID_ZAVAROVANEC",src."OSA_ID",src."VLA_ID",src."STEVILKA_DOKUMENTA",src."STEVILKA_PAKETA",src."STEVILKA_OBRAZCA",src."STEVILKA_PREDHODNEGA",src."VELJAVNOST_ZAC_DOK",src."DATUM_OD_DOK",src."URA_OD_DOK",src."MINUTA_OD_DOK",src."VELJAVNOST_POT_DOK",src."DATUM_DO_DOK",src."URA_DO_DOK",src."MINUTA_DO_DOK",src."DATUM_PODALJSANJA",src."RAZLICNI_KRAJI_ZAV",src."POZAVAROVANJE",src."SOZAVAROVANJE",src."VELJAVNOST_ZAC_KRIT",src."DATUM_OD_KRIT",src."URA_OD_KRIT",src."MINUTA_OD_KRIT",src."VELJAVNOST_POT_KRIT",src."DATUM_DO_KRIT",src."URA_DO_KRIT",src."MINUTA_DO_KRIT",src."VELJAVNOST_ZAC_OO",src."DATUM_OD_OO",src."URA_OD_OO",src."MINUTA_OD_OO",src."VELJAVNOST_POT_OO",src."DATUM_DO_OO",src."URA_DO_OO",src."MINUTA_DO_OO",src."IZPISANI_DOD_TEKST",src."TIP_CENIKA",src."UPOSPR",src."DATSPR",src."ZACETEK_ROK_VP",src."POTEK_ROKA_VP",src."OPIS",src."VINKULIRAN",src."STORNO_OD_DNE",src."NOVO",src."VSTOPNO_STEV_ZAV",src."PRVA_VKLJUCITEV_PDPZ",src."PREDV_STAROST_UPOKOJ",src."VSTOPNA_BRUTO_PLACA",src."SSK_ID",src."DATUM_SKADENCE",src."PKL_ID",src."POC_ID_POOBL_POSREDNIKA",src."VSK_ID",src."ROCNO_JAMSTVO",src."SDT_ID_OSNOVNI",src."DZA_ID",src."DOGOVOR_O_BONUSU",src."TRAJANJE_OO",src."IPVNO",src."IPSPR",src."UPOVNO_POROCANJE",src."UPOSPR_POROCANJE",src."TJE_ID_PODALJSANJE",src."AVTOMATSKO_PODALJSANJE",src."PVA_ID",src."TJE_ID_POGODBA",src."VELJAVNOST_POT_POG",src."BPA_ID",src."OBSEG_KRITJA",src."DVZ_ID",src."PTF_ID"
             from AR_OZP_INSTANCA_SKLEP_DOK_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_OZP_OBJEKT_POSTAVKA_SD_A_26 as 
select src.ID,src.PSD_ID,src.OSK_ID,src.UPOVNO,src.DATVNO,src.ZAVAROVALNA_VSOTA,src.PREMIJA,src.DATSPR,src.UPOSPR,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."PSD_ID",src."OSK_ID",src."UPOVNO",src."DATVNO",src."ZAVAROVALNA_VSOTA",src."PREMIJA",src."DATSPR",src."UPOSPR"
             from AR_OZP_OBJEKT_POSTAVKA_SD_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_OZP_OBJEKT_SKLEPDOK_A_26 as 
select src.ID,src.ISK_ID,src.VELJAVNOST_OD,src.UPOVNO,src.DATVNO,src.OZO_ID,src.OBT_ID,src.OZZ_ID,src.KZA_ID,src.OSA_ID,src.VELJAVNOST_DO,src.DATSPR,src.UPOSPR,src.ZST,src.SDT_ID,src.VOZ_ID,src.VZO_ID,src.OPIS,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."ISK_ID",src."VELJAVNOST_OD",src."UPOVNO",src."DATVNO",src."OZO_ID",src."OBT_ID",src."OZZ_ID",src."KZA_ID",src."OSA_ID",src."VELJAVNOST_DO",src."DATSPR",src."UPOSPR",src."ZST",src."SDT_ID",src."VOZ_ID",src."VZO_ID",src."OPIS"
             from AR_OZP_OBJEKT_SKLEPDOK_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_OZP_POSTAVKA_SKLEP_DOK_A_26 as 
select src.ID,src.VLA_ID_PREMIJA,src.ZVA_ID,src.SDT_ID,src.ISK_ID,src.ZVE_ID,src.VLA_ID,src.OVE_ID_VSOTA,src.OVE_ID,src.ZCK_ID,src.ZST,src.VELJAVNOST_ZAC_KRIT,src.UPOVNO,src.DATVNO,src.NZA_ID,src.TJE_ID,src.KZA_ID,src.SZK_ID,src.ZNT_ID,src.VELJAVNOST_POT_KRIT,src.OPIS,src.POZAVAROVANJE,src.SOZAVAROVANJE,src.VELJAVNOST_ZAC_OO,src.VELJAVNOST_POT_OO,src.ZAVAROVALNA_VSOTA,src.ZNESEK_OSNOVE,src.PREMIJA_BREZ_DOD,src.PREMIJA_Z_DOD,src.KOLICINSKI_FAKTOR,src.KOLICINA_ENAKIH_OBJEKTOV,src.PREMIJSKA_STOPNJA,src.FIKSNA_PREMIJA,src.FAKTOR_OO,src.TIP_FAKTORJA_OO,src.TIP_CENIKA,src.KARENCA,src.DATUM_DO_KARENCE,src.ZACETEK_JAMSTVA,src.POTEK_JAMSTVA,src.DAN_ZAPADLOSTI_ZL,src.DATSPR,src.UPOSPR,src.FSA_ID,src.DATUM_OD_KRIT,src.URA_OD_KRIT,src.MINUTA_OD_KRIT,src.DATUM_DO_KRIT,src.URA_DO_KRIT,src.MINUTA_DO_KRIT,src.DATUM_OD_OO,src.URA_OD_OO,src.MINUTA_OD_OO,src.URA_DO_OO,src.MINUTA_DO_OO,src.DATUM_DO_OO,src.KON_DAVEK_ZNESEK,src.KON_DAVEK_ODSTOTEK,src.STATUS_TKS,src.OSNOVNA_PREMIJA,src.ZLP_ID,src.NOVO,src.PREMIJA_Z_VSEMI_DOD,src.NETO_PREMIJA,src.TCJ_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."VLA_ID_PREMIJA",src."ZVA_ID",src."SDT_ID",src."ISK_ID",src."ZVE_ID",src."VLA_ID",src."OVE_ID_VSOTA",src."OVE_ID",src."ZCK_ID",src."ZST",src."VELJAVNOST_ZAC_KRIT",src."UPOVNO",src."DATVNO",src."NZA_ID",src."TJE_ID",src."KZA_ID",src."SZK_ID",src."ZNT_ID",src."VELJAVNOST_POT_KRIT",src."OPIS",src."POZAVAROVANJE",src."SOZAVAROVANJE",src."VELJAVNOST_ZAC_OO",src."VELJAVNOST_POT_OO",src."ZAVAROVALNA_VSOTA",src."ZNESEK_OSNOVE",src."PREMIJA_BREZ_DOD",src."PREMIJA_Z_DOD",src."KOLICINSKI_FAKTOR",src."KOLICINA_ENAKIH_OBJEKTOV",src."PREMIJSKA_STOPNJA",src."FIKSNA_PREMIJA",src."FAKTOR_OO",src."TIP_FAKTORJA_OO",src."TIP_CENIKA",src."KARENCA",src."DATUM_DO_KARENCE",src."ZACETEK_JAMSTVA",src."POTEK_JAMSTVA",src."DAN_ZAPADLOSTI_ZL",src."DATSPR",src."UPOSPR",src."FSA_ID",src."DATUM_OD_KRIT",src."URA_OD_KRIT",src."MINUTA_OD_KRIT",src."DATUM_DO_KRIT",src."URA_DO_KRIT",src."MINUTA_DO_KRIT",src."DATUM_OD_OO",src."URA_OD_OO",src."MINUTA_OD_OO",src."URA_DO_OO",src."MINUTA_DO_OO",src."DATUM_DO_OO",src."KON_DAVEK_ZNESEK",src."KON_DAVEK_ODSTOTEK",src."STATUS_TKS",src."OSNOVNA_PREMIJA",src."ZLP_ID",src."NOVO",src."PREMIJA_Z_VSEMI_DOD",src."NETO_PREMIJA",src."TCJ_ID"
             from AR_OZP_POSTAVKA_SKLEP_DOK_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_OZP_SKLEPALNI_DOKUMENT_A_26 as 
select src.ID,src.TSK_ID,src.STEVILKA_DOKUMENTA,src.ZSO_ID,src.ZPT_ID,src.TSK_ID_STARI,src.KON_STEVILKA_DOK,src.KON_STEVILKA_OSNDOK,src.DATVNO,src.UPOVNO,src.DATSPR,src.UPOSPR,src.PODPISI,src.KRATKA_STEVILKA,src.IPVNO,src.IPSPR,src.UPOVNO_POROCANJE,src.UPOSPR_POROCANJE,src.STEVILKA_POTRDILA,src.NADOMESTNI_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."TSK_ID",src."STEVILKA_DOKUMENTA",src."ZSO_ID",src."ZPT_ID",src."TSK_ID_STARI",src."KON_STEVILKA_DOK",src."KON_STEVILKA_OSNDOK",src."DATVNO",src."UPOVNO",src."DATSPR",src."UPOSPR",src."PODPISI",src."KRATKA_STEVILKA",src."IPVNO",src."IPSPR",src."UPOVNO_POROCANJE",src."UPOSPR_POROCANJE",src."STEVILKA_POTRDILA",src."NADOMESTNI_ID"
             from AR_OZP_SKLEPALNI_DOKUMENT_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_OZP_ZAHT_SPR_SKLEPDOK_A_26 as 
select src.ID,src.TZO_ID,src.SDT_ID,src.ZST,src.DATUM_ZAHTEVKA,src.ISK_ID,src.VZD_ID,src.OPIS,src.SPREMEMBA_SPREJEMLJIVA,src.OBLIKA_SPREMEMBE,src.NACIN_SPREMEMBE,src.VELJAVNOST_SPR_OD,src.VELJAVNOST_SPR_DO,src.DATUM_DOGODKA,src.UPOSPR,src.DATSPR,src.UPOVNO,src.DATVNO,src.ISK_ID_NOV,src.SDT_ID_NOV,src.NACIN_MIROVANJA,src.IPVNO,src.IPSPR,src.SZK_ID,src.EID,src.TOC_ID,src.STEVILKA_OBRAZCA,src.RGR_ID,src.OBRAC_VRA_TRAJ_POP,src.OBRAC_ADM_STROSKE,src.OPIS_2,src.DVA_ID,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."TZO_ID",src."SDT_ID",src."ZST",src."DATUM_ZAHTEVKA",src."ISK_ID",src."VZD_ID",src."OPIS",src."SPREMEMBA_SPREJEMLJIVA",src."OBLIKA_SPREMEMBE",src."NACIN_SPREMEMBE",src."VELJAVNOST_SPR_OD",src."VELJAVNOST_SPR_DO",src."DATUM_DOGODKA",src."UPOSPR",src."DATSPR",src."UPOVNO",src."DATVNO",src."ISK_ID_NOV",src."SDT_ID_NOV",src."NACIN_MIROVANJA",src."IPVNO",src."IPSPR",src."SZK_ID",src."EID",src."TOC_ID",src."STEVILKA_OBRAZCA",src."RGR_ID",src."OBRAC_VRA_TRAJ_POP",src."OBRAC_ADM_STROSKE",src."OPIS_2",src."DVA_ID"
             from AR_OZP_ZAHT_SPREM_SKLEPDOK_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;

create or replace view TV_OZP_ZGO_ST_SKLEPDOK_A_26 as 
select src.ID,src.ISK_ID,src.SSK_ID,src.SDT_ID,src.ZST,src.UPOVNO,src.DATVNO,src.ZSO_ID,src.VELJAVNOST_DOK_DO,src.DATSPR,src.UPOSPR,src.ZSK_ID_PREDHODI,src.DATUM_STATUSA,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."ISK_ID",src."SSK_ID",src."SDT_ID",src."ZST",src."UPOVNO",src."DATVNO",src."ZSO_ID",src."VELJAVNOST_DOK_DO",src."DATSPR",src."UPOSPR",src."ZSK_ID_PREDHODI",src."DATUM_STATUSA"
             from AR_OZP_ZGO_STATUSA_SKLEPDOK_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_PRODUCT_26 as 
select src.ID_PRODUCT,src.TP_BSN,src.PRD_IT,src.PRD,src.ID_CDE_TP_RCD,src.ID_CDE_ST,src.SDSC_SLO,src.DSC_SLO,src.SDSC,src.DSC,src.DT_BEG,src.DT_END,src.CREATED_BY,src.CREATED_ON,src.MODIFIED_BY,src.MODIFIED_ON,src.ID_COST_OWNER,src.COST_OWNER,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID_PRODUCT",src."TP_BSN",src."PRD_IT",src."PRD",src."ID_CDE_TP_RCD",src."ID_CDE_ST",src."SDSC_SLO",src."DSC_SLO",src."SDSC",src."DSC",src."DT_BEG",src."DT_END",src."CREATED_BY",src."CREATED_ON",src."MODIFIED_BY",src."MODIFIED_ON",src."ID_COST_OWNER",src."COST_OWNER"
             from AR_PRODUCT src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;

create or replace view TV_PRS_STATUS_SKLEP_DOK_A_26 as 
select src.ID,src.SIFRA,src.NAZIV,src.BILTEN,src.PROVIZIJA,src.VNOS_BREZ_ZAHTEVKA,src.DOKUMENT_ZAKLJUCEN,src.STORNO,src.STORNO_ZAKLJUCEN,src.UPOVNO,src.DATVNO,src.UPOSPR,src.DATSPR,src.SPREM_BREZ_ZAHTEVKA,src.FAKTURIRATI,src.KONCNI,src.OBRACUN_MR,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."SIFRA",src."NAZIV",src."BILTEN",src."PROVIZIJA",src."VNOS_BREZ_ZAHTEVKA",src."DOKUMENT_ZAKLJUCEN",src."STORNO",src."STORNO_ZAKLJUCEN",src."UPOVNO",src."DATVNO",src."UPOSPR",src."DATSPR",src."SPREM_BREZ_ZAHTEVKA",src."FAKTURIRATI",src."KONCNI",src."OBRACUN_MR"
             from AR_PRS_STATUS_SKLEP_DOK_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;
            
create or replace view TV_PRS_ZAVAROVANJE_A_26 as 
select src.ID,src.ZVA_ID,src.SIFRA,src.NAZIV,src.DATVNO,src.UPOVNO,src.ZCK_ID,src.ZVE_ID,src.IZPISANI_NAZIV,src.VELJAVNOST_ZACETEK,src.VELJAVNOST_POTEK,src.UPOSPR,src.DATSPR,src.OZNAKA,src.SIFRA_ZAVAROVANJA,src.UREJENSTAT_OZZ_POGOJ,src.KZZ_ZAVAROVANJE,src.KZZ_SIFRA,src.KZZ_VSOTA,src.NALOZBENO,src.OBLIKA_KONVERZIJE,src.PRIKAZI,src.OSNOVA_OCP,src.VRSTA_KRITJA,src.GRUPIRAJ,src.ZUNANJA_SIFRA,src.ETL_HASH, 1 AS ID_LOAD ,src.SYS_DT,src.PK_HASH  from

            (select  sys_dt, to_dt, SRC_HASH ETL_HASH, SRC_PK PK_HASH,  src."ID",src."ZVA_ID",src."SIFRA",src."NAZIV",src."DATVNO",src."UPOVNO",src."ZCK_ID",src."ZVE_ID",src."IZPISANI_NAZIV",src."VELJAVNOST_ZACETEK",src."VELJAVNOST_POTEK",src."UPOSPR",src."DATSPR",src."OZNAKA",src."SIFRA_ZAVAROVANJA",src."UREJENSTAT_OZZ_POGOJ",src."KZZ_ZAVAROVANJE",src."KZZ_SIFRA",src."KZZ_VSOTA",src."NALOZBENO",src."OBLIKA_KONVERZIJE",src."PRIKAZI",src."OSNOVA_OCP",src."VRSTA_KRITJA",src."GRUPIRAJ",src."ZUNANJA_SIFRA"
             from AR_PRS_ZAVAROVANJE_A src
             cross join adm.ADM_INC_LOAD_DT
             WHERE  sys_dt < load_date
AND (to_dt >= load_date)
            )SRC;


truncate table gen_plc;
truncate table LD_26_PLC_CANDIDATES;

INSERT INTO LD_26_PLC_CANDIDATES 
(	ID, 
    SDT_ID,
    ZPT_ID,
	STEVILKA_DOKUMENTA,
	ISK_ID,
    SDT_DATVNO,	
    DATVNO, 
    DATUM_DO_OO,
    VELJAVNOST_SPR_OD, 
    VELJAVNOST_ZAC_DOK, 
    VELJAVNOST_POT_DOK,
    DATUM_STATUSA_ORG,
	ST_PLC,	
	ST_PLC_DTL,
    SSK_ID, 
    SIFRA,
	TP_PLC,
    SYS_DT, --MF: to bi naj bil DT_VLD_SRC_BEG_INCR
    RANK
)
with
src as
(
    select  sdt.stevilka_dokumenta, isk2.datum_izdelave as sdt_datvno, sdt.ZPT_ID
            , zgs.id, zgs.sdt_id, zgs.isk_id, zgs.zst, zgs.datvno, zgs.datspr, zgs.datum_statusa as datum_statusa_org, to_date(zgs.datum_statusa) as datum_statusa, zgs.ssk_id
            ,  nvl(cde_trs_act.id_cde, -2)  AS ST_PLC
            ,  nvl(cde_trs_dtl.id_cde, -2) as ST_PLC_DTL
             , ssk.sifra, ssk.naziv
            , isk.VELJAVNOST_ZAC_DOK, isk.VELJAVNOST_POT_DOK, ISK.datum_do_oo, isk.zst as zst_isk
            , ZHS.VELJAVNOST_SPR_OD
            , case when zgs.ssk_id in (2,11,22000000) then 1 when zgs.ssk_id in (7,8,10000020) then 2 else 3 end as grp
			, greatest(nvl(zgs.sys_dt, ret_date_min()),
                       nvl(ssk.sys_dt, ret_date_min()),
                       nvl(isk.sys_dt, ret_date_min()),
                       nvl(sdt.sys_dt, ret_date_min()),
                       nvl(zhs.sys_dt, ret_date_min())) AS SYS_DT --MF: to bi naj bil DT_VLD_SRC_BEG_INCR
            , nvl(CT_TP_PLC.id_cde, -2) AS TP_PLC
    from    TV_OZP_ZGO_ST_SKLEPDOK_A_26 zgs    
            join TV_PRS_STATUS_SKLEP_DOK_A_26 ssk on zgs.ssk_id = ssk.id
            join TV_OZP_INSTANCA_SKLEP_DOK_A_26 isk on zgs.isk_id = isk.id
            join TV_OZP_INSTANCA_SKLEP_DOK_A_26 isk2 on isk.sdt_id = isk2.sdt_id and isk2.zst = 0        
            join TV_OZP_SKLEPALNI_DOKUMENT_A_26 sdt on isk.sdt_id = sdt.id and sdt.tsk_id in (10,16,21,22,27,32,2600059,400000066,400000067,400000086,400000326)            
            left join TV_OZP_ZAHT_SPR_SKLEPDOK_A_26 zhs on zhs.id=zgs.zso_id 
            left join (select to_number(code) zgs_ssk_id, id_cde from cde_trs where id_apl_src = 169 and id_tpe = 32 and code <> 'UM-ZA') cde_trs_act on cde_trs_act.zgs_ssk_id = zgs.ssk_id 
            left join (select to_number(code) zgs_ssk_id, id_cde from cde_trs where id_apl_src = 169 and id_tpe = 21 and code <> 'UM-ZA') cde_trs_dtl on cde_trs_dtl.zgs_ssk_id = zgs.ssk_id
            LEFT JOIN cde_trs CT_TP_PLC ON CT_TP_PLC.id_apl_src = 169 and CT_TP_PLC.id_tpe = 3467 AND TO_NUMBER(CT_TP_PLC.CODE) = sdt.tsk_id
       where   1 = 1
),
-- Izračunamo running max zst od instance
src_1 as
(
    select    id, sdt_id, datvno, ssk_id, sifra, grp, ZPT_ID
            , max(zst_isk) over (partition by sdt_id order by sdt_id, datvno, id) as zst_isk_max
            , zst_isk, VELJAVNOST_ZAC_DOK, VELJAVNOST_POT_DOK, stevilka_dokumenta
			, datum_statusa_org , isk_id, ST_PLC, ST_PLC_DTL, SDT_DATVNO, DATUM_DO_OO,VELJAVNOST_SPR_OD, SYS_DT, TP_PLC
    from    src
),
-- Izračunamo ali je zapis veljaven ali ne glede na zaporedje instanc
src_2 as
(
    select  id, sdt_id, datvno, ssk_id, sifra, grp, zst_isk_max, zst_isk, case when zst_isk < zst_isk_max then 0 else 1 end as valid, ZPT_ID
    , VELJAVNOST_ZAC_DOK, VELJAVNOST_POT_DOK, stevilka_dokumenta
	, datum_statusa_org , isk_id, ST_PLC, ST_PLC_DTL, SDT_DATVNO, DATUM_DO_OO, VELJAVNOST_SPR_OD, SYS_DT, TP_PLC
    from    src_1
    --order by sdt_id, datvno
),
-- Izločimo vse neveljavne zapise in brezpogojne statuse
src_3 as
(
    select    id, sdt_id, datvno, ssk_id, sifra, grp, zst_isk_max, zst_isk, ZPT_ID
            , VELJAVNOST_ZAC_DOK, VELJAVNOST_POT_DOK, stevilka_dokumenta, datum_statusa_org
			, ISK_ID, ST_PLC, ST_PLC_DTL, SDT_DATVNO, DATUM_DO_OO, VELJAVNOST_SPR_OD, SYS_DT, TP_PLC
    from    src_2
    where   valid = 1
      and   grp <> 1
),
-- Izločimo vse pogojne statuse v kolikor ne obstajajo predhodni statusi iz kategorije 3
src_4 as
(
    select    ID, SDT_ID, STEVILKA_DOKUMENTA, ISK_ID, DATVNO, VELJAVNOST_ZAC_DOK, VELJAVNOST_POT_DOK, DATUM_STATUSA_ORG, SSK_ID, SIFRA, GRP, ZPT_ID
            , max(grp) over (partition by sdt_id order by sdt_id, datvno, id) as grp_max
			, ST_PLC, ST_PLC_DTL, SDT_DATVNO, DATUM_DO_OO, VELJAVNOST_SPR_OD, SYS_DT, TP_PLC
    from    src_3
),
-- Ohranimo samo zadnji status v dnevu
src_5 as
(
    select    ID, SDT_ID, STEVILKA_DOKUMENTA, ISK_ID, DATVNO, VELJAVNOST_ZAC_DOK, VELJAVNOST_POT_DOK, DATUM_STATUSA_ORG, SSK_ID, SIFRA, GRP, ZPT_ID
			, ST_PLC, ST_PLC_DTL, SDT_DATVNO, DATUM_DO_OO, VELJAVNOST_SPR_OD, SYS_DT, TP_PLC
            , row_number() over (partition by sdt_id, to_date(datvno) order by datvno desc, id desc) as rn            
    from    src_4
    where   grp_max <> 2
), 
src_6 as (
    select  ID, SDT_ID, ZPT_ID, STEVILKA_DOKUMENTA, ISK_ID, SDT_DATVNO, DATVNO, DATUM_DO_OO, VELJAVNOST_SPR_OD, VELJAVNOST_ZAC_DOK, VELJAVNOST_POT_DOK, DATUM_STATUSA_ORG
        , ST_PLC, ST_PLC_DTL, SSK_ID, SIFRA, SYS_DT, TP_PLC
        , row_number() over (partition by sdt_id order by datvno desc) as rank
    from    src_5
    where   rn = 1 
)
    select  ID, SDT_ID, ZPT_ID, STEVILKA_DOKUMENTA, ISK_ID, SDT_DATVNO, DATVNO, DATUM_DO_OO, VELJAVNOST_SPR_OD, VELJAVNOST_ZAC_DOK
    , nvl(last_value(veljavnost_pot_dok ignore nulls) over (partition by sdt_id order by datvno), datum_do_oo) AS VELJAVNOST_POT_DOK, DATUM_STATUSA_ORG
        , ST_PLC, ST_PLC_DTL, SSK_ID, SIFRA, TP_PLC, SYS_DT, RANK
    from src_6
 ;

 
truncate table gen_plc_cmp;
truncate table LD_26_PLC_CMP_42_DVOJCKI;

 INSERT INTO LD_26_PLC_CMP_42_DVOJCKI
(   SDT_ID,  
    ZPT_ID,
	EXT_REF,
	ISK_ID,
    ZVE_ID,
    PSD_ID,
	PRD_EXT_REF,	
    SDT_DATVNO,	
    DT_VLD_BSN_BEG, 
    DT_VLD_SRC_BEG, 
    DT_VLD_SRC_BEG_INCR, 
    VELJAVNOST_ZAC_KRIT,
    VELJAVNOST_POT_KRIT, 
	ST_PLC_CMP,	
	ST_PLC_CMP_DTL,
    SSK_ID,
    TP_PLC,
    RN
)
with
ld_can_vsi as
(
    select distinct ld_c.sdt_id, ld_c.isk_id as isk_id, LD_C.datvno, psd.zve_id
    , coalesce(ld_c.veljavnost_spr_od, ld_c.datum_statusa_org) as DT_VLD_BSN_BEG
    from    LD_26_PLC_CANDIDATES ld_c
            left join (select distinct sdt_id, zve_id from TO_OZP_POSTAVKA_SKLEP_DOK_A_26) psd on ld_c.sdt_id = psd.sdt_id     
)
, ld_can as 
( 
    select distinct sdt_id, isk_id, zve_id, datvno, DT_VLD_BSN_BEG 
    from ld_can_vsi
),
src as
(     select psd.sdt_id as psd_sdt, psd.zve_id as psd_zve, psd.isk_id as psd_isk, ld_c.sdt_id as ld_sdc, ld_c.isk_id as ld_isk 
            , ld_c.STEVILKA_DOKUMENTA as STEVILKA_DOKUMENTA
             , psd.zst as psd_zst
            , psd.id as psd_id, ld_c.sdt_id as sdt_id
			, ld_c.isk_id as isk_id, psd.zve_id
			, psd.sdt_id || '|' || psd.zve_id  as ext_ref       
            , MIN(psd.veljavnost_zac_krit) OVER (PARTITION BY psd.SDT_ID, psd.ZVE_ID) as veljavnost_zac_krit, psd.veljavnost_pot_krit
            , LD_C.ID,  ld_c.st_plc, ld_c.st_plc_dtl, ld_c.SSK_ID       
            , LD_C.datvno as DT_VLD_SRC_BEG
            , LD_C.ZPT_ID, LD_C.ID AS IID    
            , greatest(nvl(psd.sys_dt, ret_date_min()), 
                       nvl(isk.sys_dt, ret_date_min()), 
                       nvl(ld_c.sys_dt, ret_date_min())) AS DT_VLD_SRC_BEG_INCR
            , LD_C.TP_PLC, nvl(last_value(isk.veljavnost_pot_dok ignore nulls) over (partition by ld_c.sdt_id order by ld_c.datvno), isk.datum_do_oo) as veljavnost_pot_dok
    from    LD_26_PLC_CANDIDATES ld_c 
            LEFT JOIN TO_OZP_POSTAVKA_SKLEP_DOK_A_26 psd on psd.sdt_id = ld_c.sdt_id and  psd.isk_id = ld_c.isk_id     
           left join TO_OZP_INSTANCA_SKLEP_DOK_A_26 isk on psd.isk_id = isk.id 
)
, src_datumi as 
(
    select  distinct sdt_id, isk_id, psd_zve 
     , min(to_date(veljavnost_zac_krit)) AS veljavnost_zac_krit
    , COALESCE(max(to_date(veljavnost_pot_krit)), max(to_date(veljavnost_pot_dok)), ret_date_max()) AS veljavnost_pot_krit    
    FROM src
    GROUP BY SDT_ID, isk_id, psd_zve
) 
, src_1 as
(
    select distinct ss.sdt_id, ss.isk_id, ss.zve_id, ss.psd_id, ss.STEVILKA_DOKUMENTA, ss.SSK_ID, sc.veljavnost_zac_krit, sc.veljavnost_pot_krit, ss.st_plc, ss.st_plc_dtl, ss.ext_ref
    , ss.DT_VLD_SRC_BEG, ss.ZPT_ID, ss.TP_PLC, ss.DT_VLD_SRC_BEG_INCR, ss.IID
    from  SRC_DATUMI sc
    left JOIN src ss on ss.sdt_id = sc.sdt_id and sc.psd_zve = ss.zve_id and ss.isk_id = sc.isk_id
) 
,
SRC_2 AS 
(
SELECT distinct ld_can.SDT_ID, ld_can.ISK_ID,SRC_1.ISK_ID  as src_isk_id, ld_can.zve_id as ld_zve_id, SRC_1.zve_id, SRC_1.psd_id, SRC_1.STEVILKA_DOKUMENTA, SRC_1.SSK_ID, SRC_1.veljavnost_zac_krit, SRC_1.veljavnost_pot_krit, SRC_1.st_plc, SRC_1.st_plc_dtl, SRC_1.ext_ref
, ld_can.datvno as dt_vld_src_beg, ld_can.DT_VLD_BSN_BEG, SRC_1.ZPT_ID, SRC_1.TP_PLC, src_1.DT_VLD_SRC_BEG_INCR
, LAG(ld_can.datvno) over (partition by ld_can.SDT_ID, ld_can.zve_id order by ld_can.datvno) as l_src   
, max(SRC_1.zve_id) over (partition by ld_can.SDT_ID, ld_can.zve_id order by ld_can.ISK_ID) as prev_ex
, max(SRC_1.zve_id) over (partition by ld_can.SDT_ID, ld_can.zve_id order by ld_can.ISK_ID desc) as next_ex
, src_1.IID 
FROM ld_can 
LEFT JOIN SRC_1 ON SRC_1.SDT_ID = ld_can.SDT_ID AND SRC_1.zve_id = ld_can.zve_id AND SRC_1.ISK_ID = ld_can.ISK_ID and src_1.DT_VLD_SRC_BEG = ld_can.datvno
)
,
src_3 as 
(
    select distinct sdt_id, isk_id, zve_id, ld_ZVE_ID, psd_id, STEVILKA_DOKUMENTA, SSK_ID, veljavnost_zac_krit, veljavnost_pot_krit, l_src
    , case when ZVE_ID is null and l_src is not null then CDE.id_cde else st_plc end as st_plc
    , case when ZVE_ID is null and l_src is not null then CDE_DTL.id_cde else st_plc_dtl end as st_plc_dtl
    , ext_ref, DT_VLD_SRC_BEG, DT_VLD_BSN_BEG, ZPT_ID, TP_PLC, DT_VLD_SRC_BEG_INCR
--ta rn označi zadnji zapis v dnevu
    , row_number() over (partition by sdt_id, ld_ZVE_ID, to_date(DT_VLD_SRC_BEG) order by DT_VLD_SRC_BEG desc, IID desc) as rn 
    , IID, prev_ex, next_ex
    from SRC_2
    LEFT JOIN CDE_TRS CDE_DTL ON CDE_DTL.ID_TPE = 21 and CDE_DTL.id_apl_src = 169 AND CDE_DTL.CODE = 'UM-ZA'
    LEFT JOIN CDE_TRS cde on cde.id_apl_src = 169 and cde.id_tpe = 32 and CDE.CODE = 'UM-ZA' 
    where not (prev_ex is null) and not (zve_id is null and l_src is null)
) 
, SRC_4 AS
(
    SELECT sdt_id, isk_id, zve_id, ld_ZVE_ID, psd_id, STEVILKA_DOKUMENTA, SSK_ID, veljavnost_zac_krit, veljavnost_pot_krit,  st_plc, st_plc_dtl, l_src
    , LAG(veljavnost_zac_krit) over (partition by sdt_id, ld_ZVE_ID order by DT_VLD_SRC_BEG) as l_zac_krit
    , LAG(veljavnost_pot_krit) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as l_pot_krit
    , LAG(STEVILKA_DOKUMENTA) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as l_st_dokumenta
    , LAG(DT_VLD_SRC_BEG_INCR) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as l_DT_VLD_SRC_BEG_INCR
    , LAG(TP_PLC) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as l_TP_PLC
    , LAG(ZPT_ID) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as l_ZPT_ID
    , LAG(PSD_ID) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as l_PSD_ID
--MF: dodala sem last_value, ki se uporabi za tiste zapise, kjer prideta po dva umetna zapisa skupaj in prejšnji nima vrednosti
    , last_value(veljavnost_zac_krit ignore nulls) over (partition by sdt_id, ld_ZVE_ID order by DT_VLD_SRC_BEG) as lv_zac_krit
    , last_value(veljavnost_pot_krit ignore nulls) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as lv_pot_krit
    , last_value(STEVILKA_DOKUMENTA ignore nulls) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as lv_st_dokumenta
    , last_value(DT_VLD_SRC_BEG_INCR ignore nulls) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as lv_DT_VLD_SRC_BEG_INCR
    , last_value(TP_PLC ignore nulls) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as lv_TP_PLC
    , last_value(ZPT_ID ignore nulls) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as lv_ZPT_ID
    , last_value(PSD_ID ignore nulls) over (partition by sdt_id, ld_ZVE_ID order by  DT_VLD_SRC_BEG) as lv_PSD_ID
    , DT_VLD_BSN_BEG, DT_VLD_SRC_BEG_INCR 
    , DT_VLD_SRC_BEG, ZPT_ID, TP_PLC, IID, prev_ex, next_ex
    from src_3
    WHERE RN = 1
) 
, SRC_5 AS 
(
 SELECT sdt_id, isk_id, ld_zve_id AS zve_id
    , COALESCE(psd_id, l_psd_id, lv_psd_id) AS psd_id
    , COALESCE(STEVILKA_DOKUMENTA, to_char(l_st_dokumenta), to_char(lv_st_dokumenta)) AS STEVILKA_DOKUMENTA
    , SSK_ID, st_plc, st_plc_dtl
    , COALESCE(veljavnost_zac_krit, l_zac_krit, lv_zac_krit) AS veljavnost_zac_krit
    , COALESCE(veljavnost_pot_krit, l_pot_krit, lv_pot_krit) AS veljavnost_pot_krit
    , sdt_id || '|' || ld_ZVE_ID as ext_ref, DT_VLD_SRC_BEG
    , COALESCE(ZPT_ID, l_zpt_id, lv_zpt_id) as zpt_id
    , coalesce(TP_PLC, l_tp_plc, lv_tp_plc) as TP_PLC, DT_VLD_BSN_BEG
    , COALESCE(DT_VLD_SRC_BEG_INCR, l_DT_VLD_SRC_BEG_INCR, lv_DT_VLD_SRC_BEG_INCR) AS DT_VLD_SRC_BEG_INCR
    , IID, prev_ex, next_ex
    from src_4
) 
, src_6 as
(
    select sdt_id, isk_id, zve_id, psd_id, STEVILKA_DOKUMENTA, SSK_ID,  veljavnost_zac_krit, veljavnost_pot_krit
    , ST_PLC AS ST_PLC_CMP, ST_PLC_DTL AS ST_PLC_CMP_DTL
    , ext_ref, DT_VLD_SRC_BEG, ZPT_ID, null as SDT_DATVNO, TP_PLC, DT_VLD_SRC_BEG_INCR, DT_VLD_BSN_BEG
--RN uporabimo pri inkrementalnem polnjenju
  ,  row_number() over (partition by ext_ref order by DT_VLD_SRC_BEG desc, IID desc) as RN
  , IID
    from src_5
    where not (next_ex is null and veljavnost_zac_krit is null and veljavnost_pot_krit is null)
)
select  SDT_ID, ZPT_ID, EXT_REF, ISK_ID, src_6.zve_id, psd_id, ZPT.SIFRA || '|' || PRD.TP_BSN AS PRD_EXT_REF
, SDT_DATVNO, DT_VLD_BSN_BEG, DT_VLD_SRC_BEG, DT_VLD_SRC_BEG_INCR, veljavnost_zac_krit, veljavnost_pot_krit, ST_PLC_CMP, ST_PLC_CMP_DTL, SSK_ID, TP_PLC, RN 
from src_6 
INNER join TO_PRS_ZAVAROVANJE_A_26 ZPT on ZPT.id = src_6.zve_id 
LEFT JOIN (select * from TO_PRODUCT_26 where id_cde_tp_rcd = 12443) prd on PRD.PRD_IT = ZPT.SIFRA
order by ext_ref, dt_vld_src_beg  
;