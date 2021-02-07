-- DW inicialno
TRUNCATE TABLE A_AGM;

    insert into A_AGM (ID_AGM, ID_APL_SRC, ID_TPE, EXT_REF, ID_LOAD, ID_PRT)
    select SEQ_A_AGM.nextval, ID_APL_SRC, ID_TPE, EXT_REF, 1 AS ID_LOAD, ID_PRT
    from
    (Select distinct Id_apl_src
                    ,id_tpe
                    ,ext_ref
                    ,LPAD(TO_CHAR("ID_APL_SRC"),4,'0')||'_'||LPAD(TO_CHAR("ID_TPE"),4,'0') AS id_prt
     from (

--      select  ID_APL_SRC, AGM_EXT_REF EXT_REF, ID_TPE, ID_PRT from GEN_AGM_ATR where id_slot=p_id_slot
--       UNION 
SELECT AGM_id_apl_src ID_APL_SRC,AGM_ext_ref EXT_REF,AGM_id_tpe ID_TPE,AGM_id_prt ID_PRT FROM gen_PLC_CMP where id_slot=26
UNION
SELECT AGM_id_apl_src ID_APL_SRC,AGM_ext_ref EXT_REF,AGM_id_tpe ID_TPE,AGM_id_prt ID_PRT FROM gen_PLC where id_slot=26
--UNION
--SELECT AGM_id_apl_src ID_APL_SRC,AGM_ext_ref EXT_REF,AGM_id_tpe ID_TPE,AGM_id_prt ID_PRT FROM gen_PLC_PRY where id_slot=p_id_slot
--UNION
--SELECT AGM_id_apl_src ID_APL_SRC,AGM_ext_ref EXT_REF,AGM_id_tpe ID_TPE,AGM_id_prt ID_PRT FROM gen_AGM_IDX where id_slot=p_id_slot
     ) c      
      WHERE NOT EXISTS (select 1
                        from A_AGM
                        where A_AGM.ext_ref = c.ext_ref
                        and   A_AGM.id_tpe     = c.id_tpe
                        and   A_AGM.id_apl_src     = c.id_apl_src)
    );

--select * from a_agm;
--select id_prt, count(*) from a_agm group by id_prt;

-- PLC

truncate table LD_RID;
truncate table PLC;

       INSERT  ALL 
      WHEN (last_src_beg < DT_VLD_SRC_BEG or last_src_beg is null) AND (hsh!=lag_hsh or lag_hsh is null) and src_tbl = 1  THEN 
        INTO PLC (SID_PLC,ID_AGM,ID_PRD,CODE,DT_BEG,DT_END,ST_PLC,ST_PLC_DTL,ID_CDE_CY,DT_CRT_PLC,TP_PLC,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, DT_VLD_SRC_END, ID_LOAD, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE)
      VALUES (SEQ_PLC.nextval,ID_AGM,ID_PRD,CODE,DT_BEG,DT_END,ST_PLC,ST_PLC_DTL,ID_CDE_CY,DT_CRT_PLC,TP_PLC,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, nvl(DT_VLD_SRC_END, ret_date_max()),1, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE)
--      WHEN last_src_beg >= DT_VLD_SRC_BEG and src_tbl = 1  THEN
--        INTO CTRL_PLC (ID_SLOT, ID_AGM,ID_PRD,CODE,DT_BEG,DT_END,ST_PLC,ST_PLC_DTL,ID_CDE_CY,DT_CRT_PLC,TP_PLC,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, ID_LOAD, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE)
--                   VALUES ( p_id_slot, ID_AGM,ID_PRD,CODE,DT_BEG,DT_END,ST_PLC,ST_PLC_DTL,ID_CDE_CY,DT_CRT_PLC,TP_PLC,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, p_id_load, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE) 
--      WHEN (last_src_beg < DT_VLD_SRC_BEG or last_src_beg is null) AND (hsh!=lag_hsh or lag_hsh is null) and src_tbl = 1 and (ID_PRD < -2) THEN
--                                INTO MISS_REL(SID_MISS_REL, TBL_NM, COL_NM_01, ID_ANC_01, EXT_REF_01, ID_TPE_01, ID_APL_SRC_01, ANC_01, COL_NM_02, ID_ANC_02, EXT_REF_02, ID_TPE_02 , ID_APL_SRC_02 , ANC_02, DT_VLD_BSN_BEG, DT_VLD_SRC_BEG, ID_TPE, ID_APL_SRC, ID_LOAD)
--                                VALUES(seq_miss_rel.nextval, 'PLC', 'ID_AGM' ,ID_AGM, ID_AGM_EXT_REF , ID_AGM_ID_TPE , ID_AGM_ID_APL_SRC , AGM_ANC , 'ID_PRD' ,ID_PRD, ID_PRD_EXT_REF , ID_PRD_ID_TPE , ID_PRD_ID_APL_SRC , PRD_ANC, DT_VLD_BSN_BEG,  DT_VLD_SRC_BEG, ID_TPE, ID_APL_SRC, p_id_load)             
     WHEN dt_vld_src_end is not null and rid is not null THEN INTO LD_RID
        (RID,
        DT_VLD_SRC_BEG,
        DT_VLD_SRC_END)
         VALUES
        (rid
        ,dt_vld_src_beg
        ,dt_vld_src_end)
    with tmp_gen_PLC as
    (      select /*+ ID_PRD||'|'||CODE||'|'||TO_CHAR(DT_BEG,'YYYYMMDDHH24MISS')||'|'||TO_CHAR(DT_END,'YYYYMMDDHH24MISS')||'|'||ST_PLC||'|'||ST_PLC_DTL||'|'||ID_CDE_CY||'|'||TO_CHAR(DT_CRT_PLC,'YYYYMMDDHH24MISS')||'|'||TP_PLC */
             1 src_tbl,NVL(A_AGM.ID_AGM,-hash(AGM_EXT_REF||AGM_ID_APL_SRC||AGM_ID_TPE||'A_AGM')) ID_AGM,NVL(A_PRD.ID_PRD,-hash(PRD_EXT_REF||PRD_ID_APL_SRC||PRD_ID_TPE||'A_PRD')) ID_PRD,GEN_PLC.CODE,GEN_PLC.DT_BEG,GEN_PLC.DT_END,GEN_PLC.ST_PLC,GEN_PLC.ST_PLC_DTL,GEN_PLC.ID_CDE_CY,GEN_PLC.DT_CRT_PLC,GEN_PLC.TP_PLC,
             gen_PLC.DT_VLD_BSN_BEG, 
             gen_PLC.DT_VLD_SRC_BEG, 
             gen_PLC.SEQ_PLC_CHG,
             gen_PLC.ID_APL_SRC,
             gen_PLC.ID_TPE,
             null RID,
             gen_PLC.AGM_ext_ref  ID_AGM_EXT_REF ,
   gen_PLC.AGM_id_apl_src ID_AGM_ID_APL_SRC ,
  gen_PLC.AGM_id_tpe ID_AGM_ID_TPE ,
 'A_AGM' AGM_ANC  
 , gen_PLC.PRD_ext_ref  ID_PRD_EXT_REF ,
   gen_PLC.PRD_id_apl_src ID_PRD_ID_APL_SRC ,
  gen_PLC.PRD_id_tpe ID_PRD_ID_TPE ,
 'A_PRD' PRD_ANC  
      from gen_PLC, A_AGM A_AGM,A_PRD A_PRD
      where gen_PLC.AGM_ext_ref = a_AGM.ext_ref(+) 
and   gen_PLC.AGM_id_prt = a_AGM.id_prt(+) 
 and gen_PLC.PRD_ext_ref = a_PRD.ext_ref(+) 
and   gen_PLC.PRD_id_prt = a_PRD.id_prt(+) 
      and   GEN_PLC.id_SLOT = 26 
    ),
    iv_gen_PLC as
    ( select 0 src_tbl,  PLC.ID_AGM,PLC.ID_PRD,PLC.CODE,PLC.DT_BEG,PLC.DT_END,PLC.ST_PLC,PLC.ST_PLC_DTL,PLC.ID_CDE_CY,PLC.DT_CRT_PLC,PLC.TP_PLC, 
             PLC.DT_VLD_BSN_BEG, PLC.DT_VLD_SRC_BEG, PLC.SEQ_PLC_CHG, PLC.ID_APL_SRC, PLC.ID_TPE, PLC.SID_PLC as RID, null  ID_AGM_EXT_REF ,
   null ID_AGM_ID_APL_SRC ,
  null ID_AGM_ID_TPE ,
 null AGM_ANC  
 , null  ID_PRD_EXT_REF ,
   null ID_PRD_ID_APL_SRC ,
  null ID_PRD_ID_TPE ,
 null PRD_ANC  
 
        from  PLC
            , (select id_AGM from tmp_gen_PLC group by id_AGM) tmp_gen_PLC
        where  PLC.id_AGM = tmp_gen_PLC.id_AGM
        AND DT_VLD_SRC_END = ret_date_max()

      UNION ALL
      select *
      from tmp_gen_PLC
    ),
    iv_gen_PLC_changes as
    (
      select b.*
      ,lead(case when hsh = lag_hsh then null else dt_vld_src_beg end) ignore nulls over (partition by ID_AGM, ID_TPE order by dt_vld_src_beg, seq_plc_chg, src_tbl desc) dt_vld_src_end      
      from
      (
        select a.*, lag(hsh) over (partition by ID_AGM, ID_TPE order by dt_vld_src_beg, seq_plc_chg, src_tbl desc) lag_hsh,
                   MAX(decode(src_tbl,0,DT_VLD_SRC_BEG)) over (partition by ID_AGM, ID_TPE) last_src_beg                         
        from
        (
          select src_tbl, ID_AGM,ID_PRD,CODE,DT_BEG,DT_END,ST_PLC,ST_PLC_DTL,ID_CDE_CY,DT_CRT_PLC,TP_PLC, ID_AGM_EXT_REF ,
ID_AGM_ID_APL_SRC ,
ID_AGM_ID_TPE ,
AGM_ANC  
 , ID_PRD_EXT_REF ,
ID_PRD_ID_APL_SRC ,
ID_PRD_ID_TPE ,
PRD_ANC  
, DT_VLD_BSN_BEG, DT_VLD_SRC_BEG, seq_plc_chg,
                   hash(ID_PRD||'|'||CODE||'|'||TO_CHAR(DT_BEG,'YYYYMMDDHH24MISS')||'|'||TO_CHAR(DT_END,'YYYYMMDDHH24MISS')||'|'||ST_PLC||'|'||ST_PLC_DTL||'|'||ID_CDE_CY||'|'||TO_CHAR(DT_CRT_PLC,'YYYYMMDDHH24MISS')||'|'||TP_PLC) hsh, ID_APL_SRC, ID_TPE, RID
          from iv_gen_PLC
        ) a        
      ) b
    )
      select ID_AGM,ID_PRD,CODE,DT_BEG,DT_END,ST_PLC,ST_PLC_DTL,ID_CDE_CY,DT_CRT_PLC,TP_PLC, ID_AGM_EXT_REF ,
ID_AGM_ID_APL_SRC ,
ID_AGM_ID_TPE ,
AGM_ANC  
 , ID_PRD_EXT_REF ,
ID_PRD_ID_APL_SRC ,
ID_PRD_ID_TPE ,
PRD_ANC  
,
             DT_VLD_BSN_BEG, DT_VLD_SRC_BEG, SEQ_PLC_CHG, ID_APL_SRC, last_src_beg, hsh, lag_hsh, ID_TPE, RID, dt_vld_src_end, src_tbl
      from iv_gen_PLC_changes;



--PLC_CMP

truncate table LD_RID;
truncate table PLC_CMP;
  
       INSERT  ALL 
      WHEN (last_src_beg < DT_VLD_SRC_BEG or last_src_beg is null) AND (hsh!=lag_hsh or lag_hsh is null) and src_tbl = 1  THEN 
        INTO PLC_CMP (SID_PLC_CMP,ID_AGM_PLC,ID_AGM,ID_AGM_PARENT,ID_PRD,CODE,ST_PLC_CMP_DTL,DT_BEG,DT_END,ST_PLC_CMP,TP_PLC_CMP,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, DT_VLD_SRC_END, ID_LOAD, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE)
      VALUES (SEQ_PLC_CMP.nextval,ID_AGM_PLC,ID_AGM,ID_AGM_PARENT,ID_PRD,CODE,ST_PLC_CMP_DTL,DT_BEG,DT_END,ST_PLC_CMP,TP_PLC_CMP,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, nvl(DT_VLD_SRC_END, ret_date_max()),1, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE)
--      WHEN last_src_beg >= DT_VLD_SRC_BEG and src_tbl = 1  THEN
--        INTO CTRL_PLC_CMP (ID_SLOT, ID_AGM_PLC,ID_AGM,ID_AGM_PARENT,ID_PRD,CODE,ST_PLC_CMP_DTL,DT_BEG,DT_END,ST_PLC_CMP,TP_PLC_CMP,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, ID_LOAD, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE)
--                   VALUES ( p_id_slot, ID_AGM_PLC,ID_AGM,ID_AGM_PARENT,ID_PRD,CODE,ST_PLC_CMP_DTL,DT_BEG,DT_END,ST_PLC_CMP,TP_PLC_CMP,DT_VLD_BSN_BEG,DT_VLD_SRC_BEG, p_id_load, SEQ_PLC_CHG, ID_APL_SRC, ID_TPE) 
--      WHEN (last_src_beg < DT_VLD_SRC_BEG or last_src_beg is null) AND (hsh!=lag_hsh or lag_hsh is null) and src_tbl = 1 and (ID_AGM_PLC < -2 or ID_AGM_PARENT < -2 or ID_PRD < -2) THEN
--                                INTO DW.MISS_REL(SID_MISS_REL, TBL_NM, COL_NM_01, ID_ANC_01, EXT_REF_01, ID_TPE_01, ID_APL_SRC_01, ANC_01, COL_NM_02, ID_ANC_02, EXT_REF_02, ID_TPE_02 , ID_APL_SRC_02 , ANC_02 , COL_NM_03, ID_ANC_03, EXT_REF_03, ID_TPE_03 , ID_APL_SRC_03 , ANC_03 , COL_NM_04, ID_ANC_04, EXT_REF_04, ID_TPE_04 , ID_APL_SRC_04 , ANC_04, DT_VLD_BSN_BEG, DT_VLD_SRC_BEG, ID_TPE, ID_APL_SRC, ID_LOAD)
--                                VALUES(seq_miss_rel.nextval, 'PLC_CMP', 'ID_AGM' ,ID_AGM, ID_AGM_EXT_REF , ID_AGM_ID_TPE , ID_AGM_ID_APL_SRC , AGM_ANC , 'ID_AGM_PLC' ,ID_AGM_PLC, ID_AGM_PLC_EXT_REF , ID_AGM_PLC_ID_TPE , ID_AGM_PLC_ID_APL_SRC , AGM_PLC_ANC , 'ID_AGM_PARENT' ,ID_AGM_PARENT, ID_AGM_PARENT_EXT_REF , ID_AGM_PARENT_ID_TPE , ID_AGM_PARENT_ID_APL_SRC , AGM_PARENT_ANC , 'ID_PRD' ,ID_PRD, ID_PRD_EXT_REF , ID_PRD_ID_TPE , ID_PRD_ID_APL_SRC , PRD_ANC, DT_VLD_BSN_BEG,  DT_VLD_SRC_BEG, ID_TPE, ID_APL_SRC, p_id_load)             
     WHEN dt_vld_src_end is not null and rid is not null THEN INTO LD_RID
        (RID,
        DT_VLD_SRC_BEG,
        DT_VLD_SRC_END)
         VALUES
        (rid
        ,dt_vld_src_beg
        ,dt_vld_src_end)
    with tmp_gen_PLC_CMP as
    (      select /*+ ID_AGM_PLC||'|'||ID_AGM_PARENT||'|'||ID_PRD||'|'||CODE||'|'||ST_PLC_CMP_DTL||'|'||TO_CHAR(DT_BEG,'YYYYMMDDHH24MISS')||'|'||TO_CHAR(DT_END,'YYYYMMDDHH24MISS')||'|'||ST_PLC_CMP||'|'||TP_PLC_CMP */
             1 src_tbl,NVL(A_AGM_PLC.ID_AGM,-hash(AGM_EXT_REF_PLC||AGM_ID_APL_SRC_PLC||AGM_ID_TPE_PLC||'A_AGM')) ID_AGM_PLC,NVL(A_AGM.ID_AGM,-hash(AGM_EXT_REF||AGM_ID_APL_SRC||AGM_ID_TPE||'A_AGM')) ID_AGM,NVL(A_AGM_PARENT.ID_AGM,-hash(AGM_EXT_REF_PARENT||AGM_ID_APL_SRC_PARENT||AGM_ID_TPE_PARENT||'A_AGM')) ID_AGM_PARENT,NVL(A_PRD.ID_PRD,-hash(PRD_EXT_REF||PRD_ID_APL_SRC||PRD_ID_TPE||'A_PRD')) ID_PRD,GEN_PLC_CMP.CODE,GEN_PLC_CMP.ST_PLC_CMP_DTL,GEN_PLC_CMP.DT_BEG,GEN_PLC_CMP.DT_END,GEN_PLC_CMP.ST_PLC_CMP,GEN_PLC_CMP.TP_PLC_CMP,
             gen_PLC_CMP.DT_VLD_BSN_BEG, 
             gen_PLC_CMP.DT_VLD_SRC_BEG, 
             gen_PLC_CMP.SEQ_PLC_CHG,
             gen_PLC_CMP.ID_APL_SRC,
             gen_PLC_CMP.ID_TPE,
             null RID,
             gen_PLC_CMP.AGM_ext_ref_PLC  ID_AGM_PLC_EXT_REF ,
   gen_PLC_CMP.AGM_id_apl_src_PLC ID_AGM_PLC_ID_APL_SRC ,
  gen_PLC_CMP.AGM_id_tpe_PLC ID_AGM_PLC_ID_TPE ,
 'A_AGM' AGM_PLC_ANC  
 , gen_PLC_CMP.AGM_ext_ref  ID_AGM_EXT_REF ,
   gen_PLC_CMP.AGM_id_apl_src ID_AGM_ID_APL_SRC ,
  gen_PLC_CMP.AGM_id_tpe ID_AGM_ID_TPE ,
 'A_AGM' AGM_ANC  
 , gen_PLC_CMP.AGM_ext_ref_PARENT  ID_AGM_PARENT_EXT_REF ,
   gen_PLC_CMP.AGM_id_apl_src_PARENT ID_AGM_PARENT_ID_APL_SRC ,
  gen_PLC_CMP.AGM_id_tpe_PARENT ID_AGM_PARENT_ID_TPE ,
 'A_AGM' AGM_PARENT_ANC  
 , gen_PLC_CMP.PRD_ext_ref  ID_PRD_EXT_REF ,
   gen_PLC_CMP.PRD_id_apl_src ID_PRD_ID_APL_SRC ,
  gen_PLC_CMP.PRD_id_tpe ID_PRD_ID_TPE ,
 'A_PRD' PRD_ANC  
      from gen_PLC_CMP, A_AGM A_AGM_PLC,A_AGM A_AGM,A_AGM A_AGM_PARENT,A_PRD A_PRD
      where gen_PLC_CMP.AGM_ext_ref_PLC = a_AGM_PLC.ext_ref(+) 
and   gen_PLC_CMP.AGM_id_prt_PLC = a_AGM_PLC.id_prt(+) 
 and gen_PLC_CMP.AGM_ext_ref = a_AGM.ext_ref(+) 
and   gen_PLC_CMP.AGM_id_prt = a_AGM.id_prt(+) 
 and gen_PLC_CMP.AGM_ext_ref_PARENT = a_AGM_PARENT.ext_ref(+) 
and   gen_PLC_CMP.AGM_id_prt_PARENT = a_AGM_PARENT.id_prt(+) 
 and gen_PLC_CMP.PRD_ext_ref = a_PRD.ext_ref(+) 
and   gen_PLC_CMP.PRD_id_prt = a_PRD.id_prt(+) 
      and   GEN_PLC_CMP.id_SLOT = 26 
    ),
    iv_gen_PLC_CMP as
    ( select 0 src_tbl,  PLC_CMP.ID_AGM_PLC,PLC_CMP.ID_AGM,PLC_CMP.ID_AGM_PARENT,PLC_CMP.ID_PRD,PLC_CMP.CODE,PLC_CMP.ST_PLC_CMP_DTL,PLC_CMP.DT_BEG,PLC_CMP.DT_END,PLC_CMP.ST_PLC_CMP,PLC_CMP.TP_PLC_CMP, 
             PLC_CMP.DT_VLD_BSN_BEG, PLC_CMP.DT_VLD_SRC_BEG, PLC_CMP.SEQ_PLC_CHG, PLC_CMP.ID_APL_SRC, PLC_CMP.ID_TPE, PLC_CMP.SID_PLC_CMP as RID, null  ID_AGM_PLC_EXT_REF ,
   null ID_AGM_PLC_ID_APL_SRC ,
  null ID_AGM_PLC_ID_TPE ,
 null AGM_PLC_ANC  
 , null  ID_AGM_EXT_REF ,
   null ID_AGM_ID_APL_SRC ,
  null ID_AGM_ID_TPE ,
 null AGM_ANC  
 , null  ID_AGM_PARENT_EXT_REF ,
   null ID_AGM_PARENT_ID_APL_SRC ,
  null ID_AGM_PARENT_ID_TPE ,
 null AGM_PARENT_ANC  
 , null  ID_PRD_EXT_REF ,
   null ID_PRD_ID_APL_SRC ,
  null ID_PRD_ID_TPE ,
 null PRD_ANC  
 
        from  PLC_CMP
            , (select id_AGM from tmp_gen_PLC_CMP group by id_AGM) tmp_gen_PLC_CMP
        where  PLC_CMP.id_AGM = tmp_gen_PLC_CMP.id_AGM
        AND DT_VLD_SRC_END = ret_date_max()

      UNION ALL
      select *
      from tmp_gen_PLC_CMP
    ),
    iv_gen_PLC_CMP_changes as
    (
      select b.*
      ,lead(case when hsh = lag_hsh then null else dt_vld_src_beg end) ignore nulls over (partition by ID_AGM, ID_TPE order by dt_vld_src_beg, seq_plc_chg, src_tbl desc) dt_vld_src_end      
      from
      (
        select a.*, lag(hsh) over (partition by ID_AGM, ID_TPE order by dt_vld_src_beg, seq_plc_chg, src_tbl desc) lag_hsh,
                   MAX(decode(src_tbl,0,DT_VLD_SRC_BEG)) over (partition by ID_AGM, ID_TPE) last_src_beg                         
        from
        (
          select src_tbl, ID_AGM_PLC,ID_AGM,ID_AGM_PARENT,ID_PRD,CODE,ST_PLC_CMP_DTL,DT_BEG,DT_END,ST_PLC_CMP,TP_PLC_CMP, ID_AGM_PLC_EXT_REF ,
ID_AGM_PLC_ID_APL_SRC ,
ID_AGM_PLC_ID_TPE ,
AGM_PLC_ANC  
 , ID_AGM_EXT_REF ,
ID_AGM_ID_APL_SRC ,
ID_AGM_ID_TPE ,
AGM_ANC  
 , ID_AGM_PARENT_EXT_REF ,
ID_AGM_PARENT_ID_APL_SRC ,
ID_AGM_PARENT_ID_TPE ,
AGM_PARENT_ANC  
 , ID_PRD_EXT_REF ,
ID_PRD_ID_APL_SRC ,
ID_PRD_ID_TPE ,
PRD_ANC  
, DT_VLD_BSN_BEG, DT_VLD_SRC_BEG, seq_plc_chg,
                   hash(ID_AGM_PLC||'|'||ID_AGM_PARENT||'|'||ID_PRD||'|'||CODE||'|'||ST_PLC_CMP_DTL||'|'||TO_CHAR(DT_BEG,'YYYYMMDDHH24MISS')||'|'||TO_CHAR(DT_END,'YYYYMMDDHH24MISS')||'|'||ST_PLC_CMP||'|'||TP_PLC_CMP) hsh, ID_APL_SRC, ID_TPE, RID
          from iv_gen_PLC_CMP
        ) a        
      ) b
    )
      select ID_AGM_PLC,ID_AGM,ID_AGM_PARENT,ID_PRD,CODE,ST_PLC_CMP_DTL,DT_BEG,DT_END,ST_PLC_CMP,TP_PLC_CMP, ID_AGM_PLC_EXT_REF ,
ID_AGM_PLC_ID_APL_SRC ,
ID_AGM_PLC_ID_TPE ,
AGM_PLC_ANC  
 , ID_AGM_EXT_REF ,
ID_AGM_ID_APL_SRC ,
ID_AGM_ID_TPE ,
AGM_ANC  
 , ID_AGM_PARENT_EXT_REF ,
ID_AGM_PARENT_ID_APL_SRC ,
ID_AGM_PARENT_ID_TPE ,
AGM_PARENT_ANC  
 , ID_PRD_EXT_REF ,
ID_PRD_ID_APL_SRC ,
ID_PRD_ID_TPE ,
PRD_ANC  
,
             DT_VLD_BSN_BEG, DT_VLD_SRC_BEG, SEQ_PLC_CHG, ID_APL_SRC, last_src_beg, hsh, lag_hsh, ID_TPE, RID, dt_vld_src_end, src_tbl
      from iv_gen_PLC_CMP_changes;
  
  update plc_cmp set id_prt = (LPAD(TO_CHAR("ID_APL_SRC"),4,'0')||'_'||LPAD(TO_CHAR("ID_TPE"),4,'0'));
  
  select count(*) from ld_26_plc_candidates; --24 million
