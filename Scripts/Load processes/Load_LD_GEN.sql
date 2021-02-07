-- LD inicialno

TRUNCATE TABLE LD_26_PLC_CANDIDATES;
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
    SYS_DT,
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
    from    TO_OZP_ZGO_ST_SKLEPDOK_A_26 zgs    
            join TO_PRS_STATUS_SKLEP_DOK_A_26 ssk on zgs.ssk_id = ssk.id
            join TO_OZP_INSTANCA_SKLEP_DOK_A_26 isk on zgs.isk_id = isk.id
            join TO_OZP_INSTANCA_SKLEP_DOK_A_26 isk2 on isk.sdt_id = isk2.sdt_id and isk2.zst = 0        
            join TO_OZP_SKLEPALNI_DOKUMENT_A_26 sdt on isk.sdt_id = sdt.id and sdt.tsk_id in (10,16,21,22,27,32,2600059,400000066,400000067,400000086,400000326)            
            left join TO_OZP_ZAHT_SPR_SKLEPDOK_A_26 zhs on zhs.id=zgs.zso_id 
            left join (select to_number(code) zgs_ssk_id, id_cde from cde_trs where id_apl_src = 169 and id_tpe = 32 and code <> 'UM-ZA') cde_trs_act on cde_trs_act.zgs_ssk_id = zgs.ssk_id 
            left join (select to_number(code) zgs_ssk_id, id_cde from cde_trs where id_apl_src = 169 and id_tpe = 21 and code <> 'UM-ZA') cde_trs_dtl on cde_trs_dtl.zgs_ssk_id = zgs.ssk_id
            LEFT JOIN cde_trs CT_TP_PLC ON CT_TP_PLC.id_apl_src = 169 and CT_TP_PLC.id_tpe = 3467 AND TO_NUMBER(CT_TP_PLC.CODE) = sdt.tsk_id
       where   1 = 1
--  and zgs.sdt_id = 403132574
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

--select * from ld_26_plc_candidates where sdt_id = 403132574;

--select count(*) from LD_26_PLC_CANDIDATES; --24.026.176
 

TRUNCATE TABLE LD_26_PLC_CMP_42_DVOJCKI;
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
    , row_number() over (partition by sdt_id, ld_ZVE_ID, to_date(DT_VLD_SRC_BEG) order by DT_VLD_SRC_BEG desc, IID desc, psd_id desc) as rn 
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

--select count(*) from LD_26_PLC_CMP_42_DVOJCKI; --83.253.569

-- GEN inicialno

TRUNCATE TABLE GEN_PLC; 
INSERT INTO GEN_PLC(ID_SLOT,AGM_EXT_REF, AGM_ID_APL_SRC,AGM_ID_TPE, PRD_EXT_REF,PRD_ID_APL_SRC, PRD_ID_TPE, CODE, DT_BEG, DT_END, ST_PLC, ST_PLC_DTL, ID_CDE_CY, DT_CRT_PLC, DT_VLD_BSN_BEG
, DT_VLD_SRC_BEG, SEQ_PLC_CHG ,ID_APL_SRC,ID_TPE, ID_LOAD,TP_PLC, ID_PRT, AGM_ID_PRT, PRD_ID_PRT) 
SELECT 
        26 as ID_SLOT
		, CAN.SDT_ID AS AGM_EXT_REF
		, 169 AS AGM_ID_APL_SRC
         , 6 AS AGM_ID_TPE
        , ZPT.SIFRA || '|' || PRD.TP_BSN AS PRD_EXT_REF
		, 186 PRD_ID_APL_SRC
        , 27 PRD_ID_TPE
        , CAN.STEVILKA_DOKUMENTA AS CODE
        , CAN.VELJAVNOST_ZAC_DOK AS DT_BEG
        , CAN.veljavnost_pot_dok AS dt_end		
        , CAN.ST_PLC AS ST_PLC
        , CAN.ST_PLC_DTL AS ST_PLC_DTL	
        , 97 ID_CDE_CY ---valuta
        , CAN.SDT_DATVNO AS DT_CRT_PLC
        , NVL(CAN.VELJAVNOST_SPR_OD, CAN.DATUM_STATUSA_ORG) AS DT_VLD_BSN_BEG
        , CAN.DATVNO AS DT_VLD_SRC_BEG
        , CAN.ISK_ID AS SEQ_PLC_CHG
		, 169 AS ID_APL_SRC
		, 6 AS ID_TPE 
		, 1 ID_LOAD
        , CAN.TP_PLC
        , '0169_0006' as ID_PRT
        , '0169_0006' as AGM_ID_PRT
        , '0186_0027' as PRD_ID_PRT
FROM LD_26_PLC_CANDIDATES CAN
INNER join TO_MET_ZAVAROVALNI_PROD_A_26 ZPT on ZPT.id = CAN.ZPT_ID
LEFT JOIN (select * from TO_PRODUCT_26 where id_cde_tp_rcd = 18989) prd on PRD.PRD_IT = ZPT.SIFRA
;

TRUNCATE TABLE GEN_PLC_CMP;
INSERT INTO GEN_PLC_CMP(ID_SLOT, AGM_EXT_REF_PLC, AGM_ID_APL_SRC_PLC, AGM_ID_TPE_PLC, AGM_EXT_REF, AGM_ID_APL_SRC,AGM_ID_TPE, AGM_EXT_REF_PARENT
,AGM_ID_APL_SRC_PARENT, AGM_ID_TPE_PARENT, PRD_EXT_REF,PRD_ID_APL_SRC, PRD_ID_TPE, CODE, ST_PLC_CMP_DTL, DT_BEG, DT_END, ST_PLC_CMP, DT_VLD_BSN_BEG 
, DT_VLD_SRC_BEG, SEQ_PLC_CHG ,ID_APL_SRC,ID_TPE, ID_LOAD, ID_PRT, TP_PLC_CMP, PRD_ID_PRT, AGM_ID_PRT_PARENT, AGM_ID_PRT, AGM_ID_PRT_PLC) 
SELECT  26 as ID_SLOT
		, PLC_CMP.SDT_ID AS AGM_EXT_REF_PLC
		, 169 AS AGM_ID_APL_SRC_PLC
         , 6 AS AGM_ID_TPE_PLC
         , PLC_CMP.EXT_REF AS AGM_EXT_REF
         ,170 AS AGM_ID_APL_SRC
         , 23 AS AGM_ID_TPE 
        , PLC_CMP.SDT_ID AS AGM_EXT_REF_PARENT
        , 169 as AGM_ID_APL_SRC_PARENT
        , 6 as AGM_ID_TPE_PARENT
        , PLC_CMP.PRD_EXT_REF AS PRD_EXT_REF
		, 38 PRD_ID_APL_SRC
        , 28 PRD_ID_TPE
        , PLC_CMP.EXT_REF AS CODE
        , PLC_CMP.ST_PLC_CMP_DTL AS ST_PLC_CMP_DTL	
        , PLC_CMP.VELJAVNOST_ZAC_KRIT AS DT_BEG
        , PLC_CMP.VELJAVNOST_POT_KRIT AS DT_END	
        , PLC_CMP.ST_PLC_CMP AS ST_PLC_CMP
        , PLC_CMP.DT_VLD_BSN_BEG AS DT_VLD_BSN_BEG
        , PLC_CMP.DT_VLD_SRC_BEG AS DT_VLD_SRC_BEG
        , PLC_CMP.ISK_ID AS SEQ_PLC_CHG 
		, 170 AS ID_APL_SRC        
		, 23 AS ID_TPE 
		, 1 ID_LOAD
        , '0170_0023' AS ID_PRT
        , PLC_CMP.TP_PLC AS TP_PLC_CMP
        , '0038_0028' AS PRD_ID_PRT
        , '0169_0006' AS AGM_ID_PRT_PARENT
        , '0170_0023' AS AGM_ID_PRT
        , '0169_0006' AS AGM_ID_PRT_PLC
FROM LD_26_PLC_CMP_42_DVOJCKI PLC_CMP
;

--  select count(*) from ld_26_plc_cmp_42_dvojcki; --83 million