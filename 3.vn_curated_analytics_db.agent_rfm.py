# Databricks notebook source
# MAGIC %run /Curation/Utilities/01_Initialize

# COMMAND ----------

# MAGIC %run /Curation/Utilities/02_Functions

# COMMAND ----------

#spark.conf.set("spark.sql.session.timeZone","UTC+8")
#spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", storage_secret)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS aso_agt_vio;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_agt_vio AS
# MAGIC SELECT
# MAGIC         agt_cd,
# MAGIC         COUNT(agt_cd) AS AGT_VIO_COUNT
# MAGIC FROM
# MAGIC         (
# MAGIC                 SELECT *
# MAGIC                 FROM
# MAGIC                         vn_published_ams_db.tams_agt_violation
# MAGIC                 WHERE
# MAGIC                         (
# MAGIC                                 rmk LIKE '%verbal%'
# MAGIC                         OR      rmk LIKE '%Verbal%'
# MAGIC                         OR      rmk LIKE '%Remind%'
# MAGIC                         OR      rmk LIKE '%remind%'
# MAGIC                         OR      rmk LIKE '%Warning%'
# MAGIC                         OR      rmk LIKE '%warning%'
# MAGIC                         OR      rmk LIKE '%Temp%'
# MAGIC                         OR      rmk LIKE '%temp%')
# MAGIC                 AND     tams_agt_violation.vio_dt<=last_day(add_months(CURRENT_DATE(),-4))) a
# MAGIC GROUP BY
# MAGIC         agt_cd

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS aso_agents;
# MAGIC CREATE
# MAGIC or replace TEMP VIEW aso_agents AS
# MAGIC SELECT
# MAGIC   tams_agents.agt_code,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.rank_cd IN (
# MAGIC       'FA',
# MAGIC       'TUM',
# MAGIC       'UM',
# MAGIC       'SUM',
# MAGIC       'ADM',
# MAGIC       'SADM',
# MAGIC       'DM',
# MAGIC       'SDM',
# MAGIC       'BM',
# MAGIC       'AM',
# MAGIC       'SAM'
# MAGIC     )
# MAGIC     AND tams_agents.comp_prvd_num = '01' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS AGENCY_IND,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.loc_code IN (
# MAGIC       'SIH01',
# MAGIC       'SIH02',
# MAGIC       'ACB01',
# MAGIC       'ANZ01',
# MAGIC       'ANZ02',
# MAGIC       'ANZ03',
# MAGIC       'HDB01',
# MAGIC       'HDB02',
# MAGIC       'HDB03',
# MAGIC       'MDB01',
# MAGIC       'MDB02',
# MAGIC       'MDB03',
# MAGIC       'MDB04',
# MAGIC       'MHB01',
# MAGIC       'MHB02',
# MAGIC       'NAS01',
# MAGIC       'NAS02',
# MAGIC       'NAS03',
# MAGIC       'SAG01',
# MAGIC       'SAG02',
# MAGIC       'SAG03',
# MAGIC       'TCB01',
# MAGIC       'TCB02',
# MAGIC       'TCB03',
# MAGIC       'TCB04',
# MAGIC       'TCB05',
# MAGIC       'TCB09',
# MAGIC       'TPB01',
# MAGIC       'TPB02',
# MAGIC       'VIB01',
# MAGIC       'VIB02',
# MAGIC       'VPB01',
# MAGIC       'VPB02',
# MAGIC       'VPB03'
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS BANCA_IND,
# MAGIC   tams_agents.agt_nm,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.agt_stat_code = 1 THEN 'Active'
# MAGIC     ELSE 'Terminated'
# MAGIC   END AS agt_status,
# MAGIC   tams_agents.agt_join_dt,
# MAGIC   tams_agents.agt_term_dt,
# MAGIC   tams_candidates.can_dob,
# MAGIC   tams_candidates.apply_dt,
# MAGIC   tams_candidates.sex_code,
# MAGIC   tams_candidates.mar_stat_cd,
# MAGIC   tams_candidates.ins_exp_ind,
# MAGIC   tams_candidates.agt_chnl,
# MAGIC   tams_agents.agt_addr,
# MAGIC   CAST(
# MAGIC     floor(
# MAGIC       DATEDIFF(
# MAGIC         last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC         tams_candidates.can_dob
# MAGIC       ) / 365
# MAGIC     ) AS INT
# MAGIC   ) AS agt_age,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.agt_term_dt IS NULL THEN CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(
# MAGIC           last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC           tams_agents.agt_join_dt
# MAGIC         ) / 30
# MAGIC       ) AS INT
# MAGIC     )
# MAGIC     ELSE CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(tams_agents.agt_term_dt, tams_agents.agt_join_dt) / 30
# MAGIC       ) AS INT
# MAGIC     )
# MAGIC   END AS agt_tenure_mths,
# MAGIC   CAST(
# MAGIC     DATEDIFF(
# MAGIC       tams_agents.agt_join_dt,
# MAGIC       tams_candidates.apply_dt
# MAGIC     ) / 30 AS INT
# MAGIC   ) AS agt_app_mths,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.mailbox_num IS NULL THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS Has_Mailbox,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.bus_phone IS NULL THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS Has_BusinessPhone,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.mobl_phon_num IS NULL THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS Has_MobilePhone,
# MAGIC   CASE
# MAGIC     WHEN tams_candidates.email_addr IS NULL THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS Has_Email,
# MAGIC   tams_candidates.offer_rank_cd,
# MAGIC   tams_agents.rank_cd,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.agt_term_dt IS NULL THEN CAST(
# MAGIC       DATEDIFF(
# MAGIC         last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC         tams_agents.rank_eff_dt
# MAGIC       ) / 30 AS INT
# MAGIC     )
# MAGIC     ELSE CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(tams_agents.agt_term_dt, tams_agents.rank_eff_dt) / 30
# MAGIC       ) AS INT
# MAGIC     )
# MAGIC   END AS agt_rank_mths,
# MAGIC   CASE
# MAGIC     WHEN r2.rank_cd IN (
# MAGIC       'FA',
# MAGIC       'TUM',
# MAGIC       'UM',
# MAGIC       'SUM',
# MAGIC       'ADM',
# MAGIC       'SADM',
# MAGIC       'DM',
# MAGIC       'SDM',
# MAGIC       'BM',
# MAGIC       'AM',
# MAGIC       'SAM'
# MAGIC     ) THEN (
# MAGIC       (
# MAGIC         CASE
# MAGIC           WHEN r2.rank_ordr < 70 THEN r2.rank_ordr
# MAGIC           ELSE 10 *(r2.rank_ordr - 70) + 70
# MAGIC         END
# MAGIC       ) -(
# MAGIC         CASE
# MAGIC           WHEN r1.rank_ordr < 70 THEN r1.rank_ordr
# MAGIC           ELSE 10 *(r1.rank_ordr - 70) + 70
# MAGIC         END
# MAGIC       )
# MAGIC     ) / 10
# MAGIC     ELSE 0
# MAGIC   END AS RANK_JUMP,
# MAGIC   tams_agents.comp_prvd_num,
# MAGIC   r2.is_mgr_ind,
# MAGIC   CASE
# MAGIC     WHEN tams_rehire_agt.agt_cd IS NULL THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS is_rehire_ind,
# MAGIC   tams_agents.loc_code,
# MAGIC   tams_agents.br_code,
# MAGIC   tams_agents.team_code,
# MAGIC   tams_agents.unit_code,
# MAGIC   0 AS agt_sup_cnt,
# MAGIC   'Unknown' AS AGT_SEGMENT,
# MAGIC   EducationCount,
# MAGIC   ExamCount,
# MAGIC   (
# MAGIC     CASE
# MAGIC       WHEN EducationCount IS NULL THEN 0
# MAGIC       ELSE EducationCount
# MAGIC     END
# MAGIC   ) + (
# MAGIC     CASE
# MAGIC       WHEN ExamCount IS NULL THEN 0
# MAGIC       ELSE ExamCount
# MAGIC     END
# MAGIC   ) AS TrainingCount,
# MAGIC   AGT_VIO_COUNT
# MAGIC FROM
# MAGIC   vn_published_ams_db.tams_agents
# MAGIC   LEFT JOIN vn_published_ams_db.tams_candidates ON tams_agents.can_num = tams_candidates.can_num
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT agt_cd
# MAGIC     FROM
# MAGIC       vn_published_ams_db.tams_rehire_agt
# MAGIC   ) tams_rehire_agt ON tams_agents.agt_code = tams_rehire_agt.AGT_CD
# MAGIC   LEFT JOIN vn_published_ams_db.tams_ranks r1 ON r1.rank_cd = tams_candidates.offer_rank_cd
# MAGIC   LEFT JOIN vn_published_ams_db.tams_ranks r2 ON r2.rank_cd = tams_agents.rank_cd
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       tams_exam_rslts.can_num,
# MAGIC       COUNT(tams_exam_rslts.exam_type_cd) AS ExamCount
# MAGIC     FROM
# MAGIC       vn_published_ams_db.TAMS_EXAM_RSLTS
# MAGIC       LEFT JOIN vn_published_ams_db.tams_exams ON tams_exam_rslts.exam_type_cd = tams_exams.exam_type_cd
# MAGIC     WHERE
# MAGIC       tams_exam_rslts.grade IS NOT NULL
# MAGIC       AND tams_exams.training_cat_cd NOT IN (
# MAGIC         'MIT',
# MAGIC         'MITBASIC',
# MAGIC         'MITPRODUCT'
# MAGIC       )
# MAGIC     GROUP BY
# MAGIC       tams_exam_rslts.can_num
# MAGIC   ) exam ON exam.can_num = tams_agents.can_num
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       tams_education_backgrounds.can_num,
# MAGIC       COUNT(*) AS EducationCount
# MAGIC     FROM
# MAGIC       vn_published_ams_db.TAMS_EDUCATION_BACKGROUNDS
# MAGIC     WHERE
# MAGIC       tams_education_backgrounds.edu_typ NOT IN (
# MAGIC         'ABO',
# MAGIC         'BABO',
# MAGIC         'BCOL',
# MAGIC         'BUNI',
# MAGIC         'COL',
# MAGIC         'EFNA',
# MAGIC         'FABO',
# MAGIC         'FCOL',
# MAGIC         'FUNI',
# MAGIC         'HIG',
# MAGIC         'IABO',
# MAGIC         'ICOL',
# MAGIC         'IUNI',
# MAGIC         'MIT',
# MAGIC         'MIT CERT',
# MAGIC         'MITB',
# MAGIC         'MITP',
# MAGIC         'UNI'
# MAGIC       )
# MAGIC     GROUP BY
# MAGIC       tams_education_backgrounds.can_num
# MAGIC   ) educ ON educ.can_num = tams_agents.can_num
# MAGIC   LEFT JOIN aso_agt_vio e ON tams_agents.agt_code = e.agt_cd
# MAGIC WHERE
# MAGIC   tams_agents.agt_join_dt <= last_day(add_months(CURRENT_DATE(), -4));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS aso_coverages;
# MAGIC CREATE
# MAGIC or replace TEMP VIEW aso_coverages AS
# MAGIC SELECT
# MAGIC   tpolicys.wa_cd_1 AS WRITING_AGENT,
# MAGIC   tpolicys.agt_code AS SERVICING_AGENT,
# MAGIC   p.cli_num,
# MAGIC   tcoverages.pol_num,
# MAGIC   tcoverages.plan_code,
# MAGIC   tcoverages.vers_num,
# MAGIC   tcoverages.cvg_typ,
# MAGIC   tcoverages.cvg_eff_dt,
# MAGIC   tpolicys.pol_eff_dt,
# MAGIC   tpolicys.pol_trmn_dt,
# MAGIC   tpolicys.pol_stat_cd,
# MAGIC   tcoverages.cvg_stat_cd,
# MAGIC   tcoverages.cvg_prem * 12 / tpolicys.pmt_mode AS APE,
# MAGIC   tcommission_trailers.FYC,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_typ = 'B' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS BASE_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_typ = 'B' THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS RIDER_IND,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'Accident' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_ACC,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'Critical Illness' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_CI,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'Disability' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_DIS,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'GLH' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_GLH,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'Investments' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_INV,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'Life Protection' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_LP,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'Long Term Savings' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_LTS,
# MAGIC   CASE
# MAGIC     WHEN need.customer_needs = 'Medical' THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PROD_MED,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'A',
# MAGIC       'N',
# MAGIC       'X',
# MAGIC       'R'
# MAGIC     ) THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS ISSUED_IND,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       tcoverages.cvg_eff_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -4)), -3)
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -4))
# MAGIC     )
# MAGIC     AND tcoverages.cvg_stat_cd NOT IN (
# MAGIC       'A',
# MAGIC       'N',
# MAGIC       'X',
# MAGIC       'R'
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS ISSUED_3MO,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       tcoverages.cvg_eff_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -4)), -3)
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -4))
# MAGIC     )
# MAGIC     AND tcoverages.cvg_stat_cd NOT IN (
# MAGIC       'A',
# MAGIC       'N',
# MAGIC       'X',
# MAGIC       'R'
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS ISSUED_1YR,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       tcoverages.cvg_eff_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -4)), -3)
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -4))
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS BOUGHT_3MO,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       tcoverages.cvg_eff_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -4)), -3)
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -4))
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS BOUGHT_1YR,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       '1',
# MAGIC       '2',
# MAGIC       '3',
# MAGIC       '5',
# MAGIC       '7'
# MAGIC     ) THEN 1
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'B',
# MAGIC       'D',
# MAGIC       'E',
# MAGIC       'F',
# MAGIC       'H',
# MAGIC       'M',
# MAGIC       'T',
# MAGIC       'X'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -4)) THEN 0
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'B',
# MAGIC       'D',
# MAGIC       'E',
# MAGIC       'F',
# MAGIC       'H',
# MAGIC       'M',
# MAGIC       'T',
# MAGIC       'X'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt > last_day(add_months(CURRENT_DATE(), -4)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS INFORCE_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.xpry_dt IS NULL THEN 0
# MAGIC     WHEN tcoverages.xpry_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -4)), -6)
# MAGIC     AND add_months(last_day(add_months(CURRENT_DATE(), -4)), 6) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS MATURITY_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'B',
# MAGIC       'E'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -4)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS LAPSED_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'F',
# MAGIC       'H'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -4)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS TERM_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'A',
# MAGIC       'N'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -4)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS NOTTAKEN_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'R',
# MAGIC       'X'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -4)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS REJECTED_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN (
# MAGIC       'D',
# MAGIC       'M',
# MAGIC       'T'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -4)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS CLAIM_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_reiss_dt IS NULL THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS REISS_IND,
# MAGIC   CASE
# MAGIC     WHEN tcoverages.cvg_stat_cd IN ('7') THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PH_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_trmn_dt IS NULL THEN CAST(
# MAGIC       DATEDIFF(
# MAGIC         last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC         tcoverages.cvg_eff_dt
# MAGIC       ) / 30 AS INT
# MAGIC     )
# MAGIC     ELSE CAST(
# MAGIC       DATEDIFF(tpolicys.pol_trmn_dt, tcoverages.cvg_eff_dt) / 30 AS INT
# MAGIC     )
# MAGIC   END AS CVG_TENURE,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.agt_term_dt IS NULL THEN CAST(
# MAGIC       DATEDIFF(
# MAGIC         last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC         tams_agents.agt_join_dt
# MAGIC       ) / 30 AS INT
# MAGIC     )
# MAGIC     ELSE CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(tams_agents.agt_term_dt, tams_agents.agt_join_dt) / 30
# MAGIC       ) AS INT
# MAGIC     )
# MAGIC   END AS AGT_TENURE,
# MAGIC   CAST(
# MAGIC     floor(
# MAGIC       DATEDIFF(tcoverages.cvg_eff_dt, tams_agents.agt_join_dt) / 30
# MAGIC     ) AS INT
# MAGIC   ) AS TIME_OF_SALE_MTHS,
# MAGIC   CAST(
# MAGIC     floor(
# MAGIC       DATEDIFF(
# MAGIC         last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC         tcoverages.cvg_eff_dt
# MAGIC       ) / 30
# MAGIC     ) AS INT
# MAGIC   ) AS AGT_TIME_OF_SALE,
# MAGIC   CAST(
# MAGIC     add_months(tcoverages.cvg_eff_dt, 0) AS CHAR(7)
# MAGIC   ) AS TIME_OF_SALE_YRMNTH
# MAGIC FROM
# MAGIC   vn_published_cas_db.tcoverages
# MAGIC   LEFT JOIN vn_published_cas_db.tpolicys ON tpolicys.pol_num = tcoverages.pol_num
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.wa_cd_1
# MAGIC   LEFT JOIN VN_CURATED_CAMPAIGN_DB.VN_PLAN_CODE_MAP_CURATION_EXTERNAL need ON need.plan_code = tcoverages.plan_code
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       pol_num,
# MAGIC       cli_num,
# MAGIC       plan_code,
# MAGIC       vers_num,
# MAGIC       agt_code,
# MAGIC       SUM(comm_earn_1st) AS FYC
# MAGIC     FROM
# MAGIC       vn_published_cas_db.tcommission_trailers
# MAGIC     GROUP BY
# MAGIC       pol_num,
# MAGIC       cli_num,
# MAGIC       plan_code,
# MAGIC       vers_num,
# MAGIC       agt_code
# MAGIC   ) tcommission_trailers ON tcommission_trailers.pol_num = tcoverages.pol_num
# MAGIC   AND tcommission_trailers.cli_num = tcoverages.cli_num
# MAGIC   AND tcommission_trailers.plan_code = tcoverages.plan_code
# MAGIC   AND tcommission_trailers.vers_num = tcoverages.vers_num
# MAGIC   AND tcommission_trailers.agt_code = tpolicys.wa_cd_1
# MAGIC WHERE
# MAGIC   TO_DATE(tcoverages.cvg_eff_dt) <= last_day(add_months(CURRENT_DATE(), -4))
# MAGIC ORDER BY
# MAGIC   p.cli_num,
# MAGIC   tcoverages.pol_num;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  *
# MAGIC from    aso_coverages
# MAGIC where   writing_agent='RO766'
# MAGIC order by cvg_eff_dt DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_agt_actvLTD;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_agt_actvLTD AS
# MAGIC SELECT
# MAGIC         writing_agent                                                                                            ,
# MAGIC         CASE WHEN COUNT(writing_agent)>AVG(TENURE) THEN AVG(TENURE) ELSE COUNT(writing_agent) END               AS MONTHS_ACTV ,
# MAGIC         (CASE WHEN COUNT(writing_agent)>AVG(TENURE) THEN AVG(TENURE) ELSE COUNT(writing_agent) END)/AVG(TENURE) AS LTD_AGENT_ACTIVENESS
# MAGIC FROM
# MAGIC         (
# MAGIC                 SELECT
# MAGIC                         WRITING_AGENT            ,
# MAGIC                         AGT_TIME_OF_SALE         ,
# MAGIC                         AVG(AGT_TENURE) AS TENURE,
# MAGIC                         SUM(ISSUED_IND) AS MONTHLY_SALE
# MAGIC                 FROM
# MAGIC                         aso_coverages
# MAGIC                 GROUP BY
# MAGIC                         WRITING_AGENT,
# MAGIC                         AGT_TIME_OF_SALE) aa
# MAGIC WHERE
# MAGIC         MONTHLY_SALE>0
# MAGIC GROUP BY
# MAGIC         writing_agent;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_agt_actv1YR;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_agt_actv1YR AS
# MAGIC SELECT
# MAGIC         writing_agent                                                                          ,
# MAGIC         CASE WHEN COUNT(writing_agent)>12 THEN 12 ELSE COUNT(writing_agent) END      AS MONTHS_ACTV ,
# MAGIC         (CASE WHEN COUNT(writing_agent)>12 THEN 12 ELSE COUNT(writing_agent) END)/12 AS 1YR_AGENT_ACTIVENESS
# MAGIC FROM
# MAGIC         (
# MAGIC                 SELECT
# MAGIC                         WRITING_AGENT   ,
# MAGIC                         AGT_TIME_OF_SALE,
# MAGIC                         SUM(ISSUED_1YR) AS MONTHLY_SALE
# MAGIC                 FROM
# MAGIC                         aso_coverages
# MAGIC                 GROUP BY
# MAGIC                         WRITING_AGENT,
# MAGIC                         AGT_TIME_OF_SALE) aa
# MAGIC WHERE
# MAGIC         MONTHLY_SALE>0
# MAGIC GROUP BY
# MAGIC         writing_agent;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_agt_actv3MO;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_agt_actv3MO AS
# MAGIC SELECT
# MAGIC         writing_agent                                                                        ,
# MAGIC         CASE WHEN COUNT(writing_agent)>3 THEN 3 ELSE COUNT(writing_agent) END     AS MONTHS_ACTV ,
# MAGIC         (CASE WHEN COUNT(writing_agent)>3 THEN 3 ELSE COUNT(writing_agent) END)/3 AS 3MO_AGENT_ACTIVENESS
# MAGIC FROM
# MAGIC         (
# MAGIC                 SELECT
# MAGIC                         WRITING_AGENT   ,
# MAGIC                         AGT_TIME_OF_SALE,
# MAGIC                         SUM(ISSUED_3MO) AS MONTHLY_SALE
# MAGIC                 FROM
# MAGIC                         aso_coverages
# MAGIC                 GROUP BY
# MAGIC                         WRITING_AGENT,
# MAGIC                         AGT_TIME_OF_SALE) aa
# MAGIC WHERE
# MAGIC         MONTHLY_SALE>0
# MAGIC GROUP BY
# MAGIC         writing_agent;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_agt_sales;
# MAGIC CREATE
# MAGIC or replace TEMP VIEW aso_agt_sales AS
# MAGIC SELECT
# MAGIC   LTD.*,
# MAGIC   LTD_AGENT_ACTIVENESS,
# MAGIC   LTD_need_count,
# MAGIC   MO3.*,
# MAGIC   3MO_AGENT_ACTIVENESS,
# MAGIC   3MO_need_count,
# MAGIC   YR1.*,
# MAGIC   1YR_AGENT_ACTIVENESS,
# MAGIC   1YR_need_count
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       WRITING_AGENT,
# MAGIC       COUNT(DISTINCT cli_num) AS LTD_cus_write_count,
# MAGIC       COUNT(pol_num) AS LTD_cc_total,
# MAGIC       SUM(issued_ind) AS LTD_cc_issued,
# MAGIC       SUM(nottaken_ind) AS LTD_cc_not_taken,
# MAGIC       SUM(rejected_ind) AS LTD_cc_rejected,
# MAGIC       SUM(base_ind) AS LTD_cc_issued_base,
# MAGIC       SUM(rider_ind) AS LTD_cc_issued_rider,
# MAGIC       SUM(ape) AS LTD_ape_total,
# MAGIC       SUM(ape * issued_ind) AS LTD_ape_issued,
# MAGIC       SUM(ape * nottaken_ind) AS LTD_ape_not_taken,
# MAGIC       SUM(ape * rejected_ind) AS LTD_ape_rejected,
# MAGIC       SUM(issued_ind * base_ind * ape) AS LTD_ape_issued_base,
# MAGIC       SUM(issued_ind * rider_ind * ape) AS LTD_ape_issued_rider,
# MAGIC       SUM(fyc) AS LTD_fyc_total,
# MAGIC       SUM(fyc * base_ind) AS LTD_fyc_base,
# MAGIC       SUM(fyc * rider_ind) AS LTD_fyc_rider,
# MAGIC       SUM(prod_acc * issued_ind) AS LTD_prod_acc,
# MAGIC       SUM(prod_ci * issued_ind) AS LTD_prod_ci,
# MAGIC       SUM(prod_dis * issued_ind) AS LTD_prod_dis,
# MAGIC       SUM(prod_glh * issued_ind) AS LTD_prod_glh,
# MAGIC       SUM(prod_inv * issued_ind) AS LTD_prod_inv,
# MAGIC       SUM(prod_lp * issued_ind) AS LTD_prod_lp,
# MAGIC       SUM(prod_lts * issued_ind) AS LTD_prod_lts,
# MAGIC       SUM(prod_med * issued_ind) AS LTD_prod_med
# MAGIC     FROM
# MAGIC       aso_coverages
# MAGIC     GROUP BY
# MAGIC       writing_agent
# MAGIC   ) LTD
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       WRITING_AGENT AS wa_3MO,
# MAGIC       COUNT(DISTINCT cli_num) AS 3MO_cus_write_count,
# MAGIC       COUNT(pol_num) AS 3MO_cc_total,
# MAGIC       SUM(issued_ind) AS 3MO_cc_issued,
# MAGIC       SUM(nottaken_ind) AS 3MO_cc_not_taken,
# MAGIC       SUM(rejected_ind) AS 3MO_cc_rejected,
# MAGIC       SUM(base_ind) AS 3MO_cc_issued_base,
# MAGIC       SUM(rider_ind) AS 3MO_cc_issued_rider,
# MAGIC       SUM(ape) AS 3MO_ape_total,
# MAGIC       SUM(ape * issued_ind) AS 3MO_ape_issued,
# MAGIC       SUM(ape * nottaken_ind) AS 3MO_ape_not_taken,
# MAGIC       SUM(ape * rejected_ind) AS 3MO_ape_rejected,
# MAGIC       SUM(issued_ind * base_ind * ape) AS 3MO_ape_issued_base,
# MAGIC       SUM(issued_ind * rider_ind * ape) AS 3MO_ape_issued_rider,
# MAGIC       SUM(fyc) AS 3MO_fyc_total,
# MAGIC       SUM(fyc * base_ind) AS 3MO_fyc_base,
# MAGIC       SUM(fyc * rider_ind) AS 3MO_fyc_rider,
# MAGIC       SUM(prod_acc * issued_ind) AS 3MO_prod_acc,
# MAGIC       SUM(prod_ci * issued_ind) AS 3MO_prod_ci,
# MAGIC       SUM(prod_dis * issued_ind) AS 3MO_prod_dis,
# MAGIC       SUM(prod_glh * issued_ind) AS 3MO_prod_glh,
# MAGIC       SUM(prod_inv * issued_ind) AS 3MO_prod_inv,
# MAGIC       SUM(prod_lp * issued_ind) AS 3MO_prod_lp,
# MAGIC       SUM(prod_lts * issued_ind) AS 3MO_prod_lts,
# MAGIC       SUM(prod_med * issued_ind) AS 3MO_prod_med
# MAGIC     FROM
# MAGIC       aso_coverages
# MAGIC     WHERE
# MAGIC       BOUGHT_3MO = 1
# MAGIC     GROUP BY
# MAGIC       writing_agent
# MAGIC   ) MO3 ON LTD.WRITING_AGENT = wa_3MO
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       WRITING_AGENT AS wa_1YR,
# MAGIC       COUNT(DISTINCT cli_num) AS 1YR_cus_write_count,
# MAGIC       COUNT(pol_num) AS 1YR_cc_total,
# MAGIC       SUM(issued_ind) AS 1YR_cc_issued,
# MAGIC       SUM(nottaken_ind) AS 1YR_cc_not_taken,
# MAGIC       SUM(rejected_ind) AS 1YR_cc_rejected,
# MAGIC       SUM(base_ind) AS 1YR_cc_issued_base,
# MAGIC       SUM(rider_ind) AS 1YR_cc_issued_rider,
# MAGIC       SUM(ape) AS 1YR_ape_total,
# MAGIC       SUM(ape * issued_ind) AS 1YR_ape_issued,
# MAGIC       SUM(ape * nottaken_ind) AS 1YR_ape_not_taken,
# MAGIC       SUM(ape * rejected_ind) AS 1YR_ape_rejected,
# MAGIC       SUM(issued_ind * base_ind * ape) AS 1YR_ape_issued_base,
# MAGIC       SUM(issued_ind * rider_ind * ape) AS 1YR_ape_issued_rider,
# MAGIC       SUM(fyc) AS 1YR_fyc_total,
# MAGIC       SUM(fyc * base_ind) AS 1YR_fyc_base,
# MAGIC       SUM(fyc * rider_ind) AS 1YR_fyc_rider,
# MAGIC       SUM(prod_acc * issued_ind) AS 1YR_prod_acc,
# MAGIC       SUM(prod_ci * issued_ind) AS 1YR_prod_ci,
# MAGIC       SUM(prod_dis * issued_ind) AS 1YR_prod_dis,
# MAGIC       SUM(prod_glh * issued_ind) AS 1YR_prod_glh,
# MAGIC       SUM(prod_inv * issued_ind) AS 1YR_prod_inv,
# MAGIC       SUM(prod_lp * issued_ind) AS 1YR_prod_lp,
# MAGIC       SUM(prod_lts * issued_ind) AS 1YR_prod_lts,
# MAGIC       SUM(prod_med * issued_ind) AS 1YR_prod_med
# MAGIC     FROM
# MAGIC       aso_coverages
# MAGIC     WHERE
# MAGIC       BOUGHT_1YR = 1
# MAGIC     GROUP BY
# MAGIC       writing_agent
# MAGIC   ) YR1 ON LTD.WRITING_AGENT = wa_1YR
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       writing_agent,
# MAGIC       SUM(need_count) / COUNT(DISTINCT cli_num) AS 3MO_need_count
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           cli_num,
# MAGIC           writing_agent,
# MAGIC           SUM(PROD_ACC) AS need_acc,
# MAGIC           SUM(PROD_CI) AS need_ci,
# MAGIC           SUM(PROD_DIS) AS need_dis,
# MAGIC           SUM(PROD_GLH) AS need_glh,
# MAGIC           SUM(PROD_INV) AS need_inv,
# MAGIC           SUM(PROD_LP) AS need_lp,
# MAGIC           SUM(PROD_MED) AS need_med,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_ACC) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_CI) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_DIS) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_GLH) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_INV) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_LP) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_LTS) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_MED) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) AS NEED_COUNT
# MAGIC         FROM
# MAGIC           aso_coverages
# MAGIC         WHERE
# MAGIC           issued_ind = 1
# MAGIC           AND issued_3MO = 1
# MAGIC         GROUP BY
# MAGIC           cli_num,
# MAGIC           writing_agent
# MAGIC       ) aa
# MAGIC     GROUP BY
# MAGIC       writing_agent
# MAGIC   ) need3mo ON LTD.writing_agent = need3mo.writing_agent
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       writing_agent,
# MAGIC       SUM(need_count) / COUNT(DISTINCT cli_num) AS 1YR_need_count
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           cli_num,
# MAGIC           writing_agent,
# MAGIC           SUM(PROD_ACC) AS need_acc,
# MAGIC           SUM(PROD_CI) AS need_ci,
# MAGIC           SUM(PROD_DIS) AS need_dis,
# MAGIC           SUM(PROD_GLH) AS need_glh,
# MAGIC           SUM(PROD_INV) AS need_inv,
# MAGIC           SUM(PROD_LP) AS need_lp,
# MAGIC           SUM(PROD_MED) AS need_med,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_ACC) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_CI) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_DIS) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_GLH) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_INV) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_LP) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_LTS) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_MED) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) AS NEED_COUNT
# MAGIC         FROM
# MAGIC           aso_coverages
# MAGIC         WHERE
# MAGIC           issued_ind = 1
# MAGIC           AND issued_1YR = 1
# MAGIC         GROUP BY
# MAGIC           cli_num,
# MAGIC           writing_agent
# MAGIC       ) aa
# MAGIC     GROUP BY
# MAGIC       writing_agent
# MAGIC   ) need1YR ON LTD.writing_agent = need1YR.writing_agent
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       writing_agent,
# MAGIC       SUM(need_count) / COUNT(DISTINCT cli_num) AS LTD_need_count
# MAGIC     FROM
# MAGIC       (
# MAGIC         SELECT
# MAGIC           cli_num,
# MAGIC           writing_agent,
# MAGIC           SUM(PROD_ACC) AS need_acc,
# MAGIC           SUM(PROD_CI) AS need_ci,
# MAGIC           SUM(PROD_DIS) AS need_dis,
# MAGIC           SUM(PROD_GLH) AS need_glh,
# MAGIC           SUM(PROD_INV) AS need_inv,
# MAGIC           SUM(PROD_LP) AS need_lp,
# MAGIC           SUM(PROD_MED) AS need_med,
# MAGIC           (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_ACC) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_CI) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_DIS) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_GLH) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_INV) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_LP) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_LTS) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) + (
# MAGIC             CASE
# MAGIC               WHEN SUM(PROD_MED) > 0 THEN 1
# MAGIC               ELSE 0
# MAGIC             END
# MAGIC           ) AS NEED_COUNT
# MAGIC         FROM
# MAGIC           aso_coverages
# MAGIC         WHERE
# MAGIC           issued_ind = 1
# MAGIC         GROUP BY
# MAGIC           cli_num,
# MAGIC           writing_agent
# MAGIC       ) aa
# MAGIC     GROUP BY
# MAGIC       writing_agent
# MAGIC   ) needLTD ON LTD.writing_agent = needLTD.writing_agent
# MAGIC   LEFT JOIN aso_agt_actv1YR a ON LTD.WRITING_AGENT = a.writing_agent
# MAGIC   LEFT JOIN aso_agt_actv3MO b ON LTD.WRITING_AGENT = b.writing_agent
# MAGIC   LEFT JOIN aso_agt_actvLTD c ON LTD.WRITING_AGENT = c.writing_agent;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  *
# MAGIC from    aso_agt_sales
# MAGIC where   writing_agent='RO766'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.ao_fyc;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         ao_fyc AS
# MAGIC SELECT
# MAGIC         pol_num  ,
# MAGIC         agt_code ,
# MAGIC         SUM(comm_earn_1st) AS FYC
# MAGIC FROM
# MAGIC         vn_published_cas_db.tcommission_trailers
# MAGIC GROUP BY
# MAGIC         pol_num,
# MAGIC         agt_code;
# MAGIC
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.ao_syc;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         ao_syc AS
# MAGIC SELECT
# MAGIC         pol.pol_num,
# MAGIC         SUM(CASE WHEN cr_or_dr='D' THEN acct_gen_amt ELSE 0 END) + SUM(CASE WHEN cr_or_dr='C' THEN -acct_gen_amt ELSE 0 END) AS comm2y
# MAGIC FROM
# MAGIC         vn_published_cas_db.tacct_extracts acc,
# MAGIC         vn_published_cas_db.tplans         pln,
# MAGIC         vn_published_cas_db.tpolicys       pol
# MAGIC WHERE
# MAGIC         acct_num='6241220'
# MAGIC AND     extract_typ IN ('N',
# MAGIC                         'X')
# MAGIC AND     acct_mne_cd IN ('COMM2Y')
# MAGIC AND     acc.plan_code = pln.plan_code
# MAGIC AND     acc.vers_num  = pln.vers_num
# MAGIC AND     acc.pol_num   = pol.pol_num
# MAGIC GROUP BY
# MAGIC         pol.pol_num;
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.ao_ryc;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         ao_ryc AS
# MAGIC SELECT
# MAGIC         pol.pol_num,
# MAGIC         SUM(CASE WHEN cr_or_dr='D' THEN acct_gen_amt ELSE 0 END) + SUM(CASE WHEN cr_or_dr='C' THEN -acct_gen_amt ELSE 0 END) AS commry
# MAGIC FROM
# MAGIC         vn_published_cas_db.tacct_extracts acc,
# MAGIC         vn_published_cas_db.tplans         pln,
# MAGIC         vn_published_cas_db.tpolicys       pol
# MAGIC WHERE
# MAGIC         acct_num='6241230'
# MAGIC AND     extract_typ IN ('N',
# MAGIC                         'X')
# MAGIC AND     acct_mne_cd IN ('COMMRY',
# MAGIC                         'COMM3Y')
# MAGIC AND     acc.plan_code = pln.plan_code
# MAGIC AND     acc.vers_num  = pln.vers_num
# MAGIC AND     acc.pol_num   = pol.pol_num
# MAGIC GROUP BY
# MAGIC         pol.pol_num;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_pol_serviced;
# MAGIC CREATE
# MAGIC or replace TEMP VIEW aso_pol_serviced AS
# MAGIC SELECT
# MAGIC   tpolicys.agt_code AS SERVICING_AGENT,
# MAGIC   tpolicys.wa_cd_1,
# MAGIC   tpolicys.wa_cd_2,
# MAGIC   tams_agents.agt_join_dt,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.agt_code = tpolicys.wa_cd_1
# MAGIC     OR tpolicys.agt_code = tpolicys.wa_cd_2 THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS AGT_WRITESERVE,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.plan_code_base IN (
# MAGIC       'UL001',
# MAGIC       'UL002',
# MAGIC       'UL003',
# MAGIC       'UL004',
# MAGIC       'UL005',
# MAGIC       'UL007'
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PLAN_UL,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       tpolicys.pol_eff_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -1)), -3)
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -1))
# MAGIC     )
# MAGIC     AND tpolicys.pol_stat_cd NOT IN (
# MAGIC       'A',
# MAGIC       'N',
# MAGIC       'X',
# MAGIC       'R'
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS ISSUED_3MO,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       tpolicys.pol_eff_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -1)), -12)
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -1))
# MAGIC     )
# MAGIC     AND tpolicys.pol_stat_cd NOT IN (
# MAGIC       'A',
# MAGIC       'N',
# MAGIC       'X',
# MAGIC       'R'
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS ISSUED_1YR,
# MAGIC   CASE
# MAGIC     WHEN (
# MAGIC       tpolicys.pol_eff_dt BETWEEN add_months(last_day(add_months(CURRENT_DATE(), -1)), -36)
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -1))
# MAGIC     )
# MAGIC     AND tpolicys.pol_stat_cd NOT IN (
# MAGIC       'A',
# MAGIC       'N',
# MAGIC       'X',
# MAGIC       'R'
# MAGIC     ) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS ISSUED_3YR,
# MAGIC   CASE
# MAGIC     WHEN tams_agents.agt_term_dt IS NULL THEN CAST(
# MAGIC       DATEDIFF(
# MAGIC         last_day(add_months(CURRENT_DATE(), -1)),
# MAGIC         tams_agents.agt_join_dt
# MAGIC       ) / 30 AS INT
# MAGIC     )
# MAGIC     ELSE CAST(
# MAGIC       DATEDIFF(tams_agents.agt_term_dt, tams_agents.agt_join_dt) / 30 AS INT
# MAGIC     )
# MAGIC   END AS AGT_TENURE,
# MAGIC   CAST(
# MAGIC     DATEDIFF(tpolicys.pol_eff_dt, tams_agents.agt_join_dt) / 30 AS INT
# MAGIC   ) AS TIME_OF_SALE_MTHS,
# MAGIC   CAST(
# MAGIC     add_months(tpolicys.pol_eff_dt, 0) AS CHAR(7)
# MAGIC   ) AS TIME_OF_SALE_YRMNTH,
# MAGIC   p.cli_num,
# MAGIC   tpolicys.pol_num,
# MAGIC   tpolicys.pol_eff_dt,
# MAGIC   tpolicys.pol_trmn_dt,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_trmn_dt IS NULL THEN CAST(
# MAGIC       DATEDIFF(
# MAGIC         last_day(add_months(CURRENT_DATE(), -1)),
# MAGIC         tpolicys.pol_eff_dt
# MAGIC       ) / 30 AS INT
# MAGIC     )
# MAGIC     ELSE CAST(
# MAGIC       DATEDIFF(tpolicys.pol_trmn_dt, tpolicys.pol_eff_dt) / 30 AS INT
# MAGIC     )
# MAGIC   END AS POL_TENURE,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       '1',
# MAGIC       '2',
# MAGIC       '3',
# MAGIC       '5',
# MAGIC       '7'
# MAGIC     ) THEN 1
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'B',
# MAGIC       'D',
# MAGIC       'E',
# MAGIC       'F',
# MAGIC       'H',
# MAGIC       'M',
# MAGIC       'T',
# MAGIC       'X'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -1)) THEN 0
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'B',
# MAGIC       'D',
# MAGIC       'E',
# MAGIC       'F',
# MAGIC       'H',
# MAGIC       'M',
# MAGIC       'T',
# MAGIC       'X'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt > last_day(add_months(CURRENT_DATE(), -1)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS INFORCE_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'A',
# MAGIC       'N',
# MAGIC       'X',
# MAGIC       'R'
# MAGIC     ) THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS ISSUED_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'B',
# MAGIC       'E'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -1)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS LAPSED_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'F',
# MAGIC       'H'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -1)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS TERM_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'A',
# MAGIC       'N'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -1)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS NOTTAKEN_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'R',
# MAGIC       'X'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -1)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS REJECTED_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN (
# MAGIC       'D',
# MAGIC       'M',
# MAGIC       'T'
# MAGIC     )
# MAGIC     AND tpolicys.pol_trmn_dt <= last_day(add_months(CURRENT_DATE(), -1)) THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS CLAIM_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_reiss_dt IS NULL THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS REISS_IND,
# MAGIC   CASE
# MAGIC     WHEN tpolicys.pol_stat_cd IN ('7') THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PH_IND,
# MAGIC   tpolicys.pol_stat_cd,
# MAGIC   tpolicys.mode_prem * 12 / tpolicys.pmt_mode AS APE,
# MAGIC   comm1.FYC AS FYC,
# MAGIC   rcomm2.comm2y + rcommr.commry AS RYC
# MAGIC FROM
# MAGIC   vn_published_cas_db.tpolicys
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.agt_code
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN VN_CURATED_CAMPAIGN_DB.VN_PLAN_CODE_MAP_CURATION_EXTERNAL need ON need.plan_code = tpolicys.plan_code_base
# MAGIC   LEFT JOIN ao_fyc comm1 ON comm1.pol_num = tpolicys.pol_num
# MAGIC   AND tpolicys.agt_code = comm1.agt_code
# MAGIC   LEFT JOIN ao_syc rcomm2 ON rcomm2.pol_num = tpolicys.pol_num
# MAGIC   LEFT JOIN ao_ryc rcommr ON rcommr.pol_num = tpolicys.pol_num
# MAGIC WHERE
# MAGIC   tpolicys.pol_eff_dt <= last_day(add_months(CURRENT_DATE(), -4));

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_cus_typeLTD;
# MAGIC CREATE
# MAGIC or replace TEMP VIEW aso_cus_typeLTD AS
# MAGIC SELECT
# MAGIC   cli_num,
# MAGIC   MIN(pol_eff_dt) AS JOIN_DT,
# MAGIC   SUM(INFORCE_IND) AS CC_INFORCE,
# MAGIC   CASE
# MAGIC     WHEN SUM(INFORCE_IND) > 0 THEN 'ACTIVE'
# MAGIC     ELSE 'TERMINATED'
# MAGIC   END AS CUS_RLTN_MLI,
# MAGIC   CASE
# MAGIC     WHEN SUM(INFORCE_IND) > 0 THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS CUS_RLTN_MLI_ACTV,
# MAGIC   CASE
# MAGIC     WHEN SUM(INFORCE_IND) > 0 THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS CUS_RLTN_MLI_TERM,
# MAGIC   CASE
# MAGIC     WHEN CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(
# MAGIC           last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC           MIN(pol_eff_dt)
# MAGIC         )
# MAGIC       ) AS INT
# MAGIC     ) > 365
# MAGIC     AND SUM(ISSUED_1YR) > 0 THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS PAST_1YR_PURCHASE,
# MAGIC   CASE
# MAGIC     WHEN CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(
# MAGIC           last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC           MIN(pol_eff_dt)
# MAGIC         )
# MAGIC       ) AS INT
# MAGIC     ) <= 365 THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS CUS_NEW,
# MAGIC   CASE
# MAGIC     WHEN CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(
# MAGIC           last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC           MIN(pol_eff_dt)
# MAGIC         )
# MAGIC       ) AS INT
# MAGIC     ) <= 365 THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS CUS_EXISTING,
# MAGIC   CASE
# MAGIC     WHEN SUM(INFORCE_IND) <= 0 THEN CAST(
# MAGIC       floor(DATEDIFF(MAX(pol_trmn_dt), MIN(pol_eff_dt)) / 30) AS INT
# MAGIC     )
# MAGIC     ELSE CAST(
# MAGIC       floor(
# MAGIC         DATEDIFF(
# MAGIC           last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC           MIN(pol_eff_dt)
# MAGIC         ) / 30
# MAGIC       ) AS INT
# MAGIC     )
# MAGIC   END AS CUSTOMER_TENURE
# MAGIC FROM
# MAGIC   aso_pol_serviced
# MAGIC GROUP BY
# MAGIC   cli_num;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.ao_xtemp2;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         ao_xtemp2 AS
# MAGIC SELECT
# MAGIC         a.servicing_agent                                                                     AS SERV_AGT                 ,
# MAGIC         COUNT(DISTINCT a.cli_num)                                                             AS CUS_SERV_COUNT           ,
# MAGIC         SUM(c.CUS_RLTN_MLI_ACTV)                                                              AS CUS_SERV_ACTV_COUNT      ,
# MAGIC         SUM(c.CUS_RLTN_MLI_TERM)                                                              AS CUS_SERV_TERM_COUNT      ,
# MAGIC         MIN(c.CUS_RLTN_MLI_ACTV*c.CUSTOMER_TENURE)                                            AS CUS_SERV_ACTV_TENURE_MIN ,
# MAGIC         MAX(c.CUS_RLTN_MLI_ACTV*c.CUSTOMER_TENURE)                                            AS CUS_SERV_ACTV_TENURE_MAX ,
# MAGIC         AVG(c.CUS_RLTN_MLI_ACTV*c.CUSTOMER_TENURE)                                            AS CUS_SERV_ACTV_TENURE_AVG ,
# MAGIC         MIN(c.CUS_RLTN_MLI_TERM*c.CUSTOMER_TENURE)                                            AS CUS_SERV_TERM_TENURE_MIN ,
# MAGIC         MAX(c.CUS_RLTN_MLI_TERM*c.CUSTOMER_TENURE)                                            AS CUS_SERV_TERM_TENURE_MAX ,
# MAGIC         AVG(c.CUS_RLTN_MLI_TERM*c.CUSTOMER_TENURE)                                            AS CUS_SERV_TERM_TENURE_AVG ,
# MAGIC         MIN(c.CUSTOMER_TENURE)                                                                AS CUS_SERV_TERM_RECENT     ,
# MAGIC         MAX(c.CUSTOMER_TENURE)                                                                AS CUS_SERV_TERM_OLDEST     ,
# MAGIC         SUM(c.CUS_RLTN_MLI_ACTV*c.CUSTOMER_TENURE)                                            AS CUS_SERV_ACTV_TENURE_TOT ,
# MAGIC         SUM(c.CUS_RLTN_MLI_TERM*c.CUSTOMER_TENURE)                                            AS CUS_SERV_TERM_TENURE_TOT ,
# MAGIC         SUM(c.CUS_RLTN_MLI_TERM*c.CUSTOMER_TENURE)+SUM(c.CUS_RLTN_MLI_ACTV*c.CUSTOMER_TENURE) AS CUS_SERV_TENURE_TOT      ,
# MAGIC         SUM(c.cus_new)                                                                        AS CUS_NEW                  ,
# MAGIC         SUM(c.cus_existing)                                                                   AS CUS_EXISTING
# MAGIC FROM
# MAGIC         (
# MAGIC                 SELECT
# MAGIC                         servicing_agent,
# MAGIC                         cli_num
# MAGIC                 FROM
# MAGIC                         aso_pol_serviced gg
# MAGIC                 GROUP BY
# MAGIC                         servicing_agent,
# MAGIC                         cli_num) a
# MAGIC LEFT JOIN
# MAGIC         aso_cus_typeLTD c
# MAGIC ON
# MAGIC         a.cli_num=c.cli_num
# MAGIC GROUP BY
# MAGIC         a.SERVICING_AGENT;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_lapsed1YR;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_lapsed1YR AS
# MAGIC SELECT
# MAGIC         tpolicys.agt_code,
# MAGIC         COUNT(DISTINCT p.cli_num) AS 1YR_CUS_LAPSED
# MAGIC FROM
# MAGIC         vn_published_cas_db.tcoverages
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tpolicys
# MAGIC ON
# MAGIC         tpolicys.pol_num=tcoverages.pol_num
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tclient_policy_links p
# MAGIC ON
# MAGIC         tpolicys.pol_num=p.pol_num
# MAGIC AND     p.LINK_TYP      ='O'
# MAGIC AND     p.REC_STATUS    ='A'
# MAGIC LEFT JOIN
# MAGIC         vn_published_ams_db.tams_agents
# MAGIC ON
# MAGIC         tams_agents.agt_code=tpolicys.agt_code
# MAGIC WHERE
# MAGIC         (
# MAGIC                 tpolicys.pol_trmn_dt BETWEEN LAST_DAY(add_months(last_day(add_months(CURRENT_DATE(),-4)), -12)) AND     last_day(add_months(CURRENT_DATE(),-4)))
# MAGIC AND     tcoverages.cvg_stat_cd IN ('B',
# MAGIC                                    'E')
# MAGIC GROUP BY
# MAGIC         tpolicys.agt_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_lapsedLTD;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_lapsedLTD AS
# MAGIC SELECT
# MAGIC         tpolicys.agt_code,
# MAGIC         COUNT(DISTINCT p.cli_num) AS LTD_CUS_LAPSED
# MAGIC FROM
# MAGIC         vn_published_cas_db.tcoverages
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tpolicys
# MAGIC ON
# MAGIC         tpolicys.pol_num=tcoverages.pol_num
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tclient_policy_links p
# MAGIC ON
# MAGIC         tpolicys.pol_num=p.pol_num
# MAGIC AND     p.LINK_TYP      ='O'
# MAGIC AND     p.REC_STATUS    ='A'
# MAGIC LEFT JOIN
# MAGIC         vn_published_ams_db.tams_agents
# MAGIC ON
# MAGIC         tams_agents.agt_code=tpolicys.agt_code
# MAGIC WHERE
# MAGIC         (
# MAGIC                 tpolicys.pol_trmn_dt IS NOT NULL)
# MAGIC AND     tcoverages.cvg_stat_cd IN ('B',
# MAGIC                                    'E')
# MAGIC GROUP BY
# MAGIC         tpolicys.agt_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_lapsed3MO;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_lapsed3MO AS
# MAGIC SELECT
# MAGIC         tpolicys.agt_code,
# MAGIC         COUNT(DISTINCT p.cli_num) AS 3MO_CUS_LAPSED
# MAGIC FROM
# MAGIC         vn_published_cas_db.tcoverages
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tpolicys
# MAGIC ON
# MAGIC         tpolicys.pol_num=tcoverages.pol_num
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tclient_policy_links p
# MAGIC ON
# MAGIC         tpolicys.pol_num=p.pol_num
# MAGIC AND     p.LINK_TYP      ='O'
# MAGIC AND     p.REC_STATUS    ='A'
# MAGIC LEFT JOIN
# MAGIC         vn_published_ams_db.tams_agents
# MAGIC ON
# MAGIC         tams_agents.agt_code=tpolicys.agt_code
# MAGIC WHERE
# MAGIC         (
# MAGIC                 tpolicys.pol_trmn_dt BETWEEN LAST_DAY(add_months(last_day(add_months(CURRENT_DATE(),-4)), -3)) AND     last_day(add_months(CURRENT_DATE(),-4)))
# MAGIC AND     tcoverages.cvg_stat_cd IN ('B',
# MAGIC                                    'E')
# MAGIC GROUP BY
# MAGIC         tpolicys.agt_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_matured3MO;
# MAGIC
# MAGIC CREATE or replace TEMP VIEW
# MAGIC         aso_matured3MO AS
# MAGIC SELECT
# MAGIC         tpolicys.agt_code,
# MAGIC         COUNT(DISTINCT p.cli_num) AS 3MO_CUS_MATURE
# MAGIC FROM
# MAGIC         vn_published_cas_db.tcoverages
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tpolicys
# MAGIC ON
# MAGIC         tpolicys.pol_num=tcoverages.pol_num
# MAGIC LEFT JOIN
# MAGIC         vn_published_cas_db.tclient_policy_links p
# MAGIC ON
# MAGIC         tpolicys.pol_num=p.pol_num
# MAGIC AND     p.LINK_TYP      ='O'
# MAGIC AND     p.REC_STATUS    ='A'
# MAGIC LEFT JOIN
# MAGIC         vn_published_ams_db.tams_agents
# MAGIC ON
# MAGIC         tams_agents.agt_code=tpolicys.agt_code
# MAGIC WHERE
# MAGIC         tcoverages.xpry_dt BETWEEN LAST_DAY(add_months(last_day(add_months(CURRENT_DATE(),-4)), -3)) AND     LAST_DAY(add_months(last_day(add_months(CURRENT_DATE(),-4)), 3))
# MAGIC GROUP BY
# MAGIC         tpolicys.agt_code;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_matured1YR;
# MAGIC CREATE
# MAGIC or replace TEMP VIEW aso_matured1YR AS
# MAGIC SELECT
# MAGIC   tpolicys.agt_code,
# MAGIC   COUNT(DISTINCT p.cli_num) AS 1YR_CUS_MATURE
# MAGIC FROM
# MAGIC   vn_published_cas_db.tcoverages
# MAGIC   LEFT JOIN vn_published_cas_db.tpolicys ON tpolicys.pol_num = tcoverages.pol_num
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.agt_code
# MAGIC WHERE
# MAGIC   tcoverages.xpry_dt BETWEEN LAST_DAY(
# MAGIC     add_months(last_day(add_months(CURRENT_DATE(), -4)), -12)
# MAGIC   )
# MAGIC   AND LAST_DAY(
# MAGIC     add_months(last_day(add_months(CURRENT_DATE(), -4)), 12)
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   tpolicys.agt_code;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_matured6MO;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_matured6MO AS
# MAGIC SELECT
# MAGIC   tpolicys.agt_code,
# MAGIC   COUNT(DISTINCT p.cli_num) AS 6MO_CUS_MATURE
# MAGIC FROM
# MAGIC   vn_published_cas_db.tcoverages
# MAGIC   LEFT JOIN vn_published_cas_db.tpolicys ON tpolicys.pol_num = tcoverages.pol_num
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.agt_code
# MAGIC WHERE
# MAGIC   tcoverages.xpry_dt BETWEEN LAST_DAY(
# MAGIC     add_months(last_day(add_months(CURRENT_DATE(), -4)), -6)
# MAGIC   )
# MAGIC   AND LAST_DAY(
# MAGIC     add_months(last_day(add_months(CURRENT_DATE(), -4)), 6)
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   tpolicys.agt_code;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_claimedLTD;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_claimedLTD AS
# MAGIC SELECT
# MAGIC   tpolicys.agt_code,
# MAGIC   COUNT(DISTINCT p.cli_num) AS LTD_CUS_CLAIM
# MAGIC FROM
# MAGIC   vn_published_cas_db.tcoverages
# MAGIC   LEFT JOIN vn_published_cas_db.tpolicys ON tpolicys.pol_num = tcoverages.pol_num
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.agt_code
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT cli_num
# MAGIC     FROM
# MAGIC       vn_published_cas_db.TCLAIM_DETAILS
# MAGIC     WHERE
# MAGIC       tclaim_details.clm_stat_code = 'A'
# MAGIC       AND tclaim_details.clm_eff_dt < LAST_DAY(last_day(add_months(CURRENT_DATE(), -4)))
# MAGIC   ) claim ON claim.cli_num = p.cli_num
# MAGIC WHERE
# MAGIC   tpolicys.agt_code IS NOT NULL
# MAGIC   AND claim.cli_num IS NOT NULL
# MAGIC GROUP BY
# MAGIC   tpolicys.agt_code;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_claimed1YR;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_claimed1YR AS
# MAGIC SELECT
# MAGIC   tpolicys.agt_code,
# MAGIC   COUNT(DISTINCT p.cli_num) AS 1YR_CUS_CLAIM
# MAGIC FROM
# MAGIC   vn_published_cas_db.tcoverages
# MAGIC   LEFT JOIN vn_published_cas_db.tpolicys ON tpolicys.pol_num = tcoverages.pol_num
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.agt_code
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT cli_num
# MAGIC     FROM
# MAGIC       vn_published_cas_db.TCLAIM_DETAILS
# MAGIC     WHERE
# MAGIC       tclaim_details.clm_stat_code = 'A'
# MAGIC       AND (
# MAGIC         tclaim_details.clm_eff_dt BETWEEN LAST_DAY(
# MAGIC           add_months(last_day(add_months(CURRENT_DATE(), -4)), -12)
# MAGIC         )
# MAGIC         AND LAST_DAY(last_day(add_months(CURRENT_DATE(), -4)))
# MAGIC       )
# MAGIC   ) claim ON claim.cli_num = p.cli_num
# MAGIC WHERE
# MAGIC   tpolicys.agt_code IS NOT NULL
# MAGIC   AND claim.cli_num IS NOT NULL
# MAGIC GROUP BY
# MAGIC   tpolicys.agt_code;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_claimed6MO;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_claimed6MO AS
# MAGIC SELECT
# MAGIC   tpolicys.agt_code,
# MAGIC   COUNT(DISTINCT p.cli_num) AS 6MO_CUS_CLAIM
# MAGIC FROM
# MAGIC   vn_published_cas_db.tcoverages
# MAGIC   LEFT JOIN vn_published_cas_db.tpolicys ON tpolicys.pol_num = tcoverages.pol_num
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.agt_code
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT cli_num
# MAGIC     FROM
# MAGIC       vn_published_cas_db.TCLAIM_DETAILS
# MAGIC     WHERE
# MAGIC       tclaim_details.clm_stat_code = 'A'
# MAGIC       AND (
# MAGIC         tclaim_details.clm_eff_dt BETWEEN LAST_DAY(
# MAGIC           add_months(last_day(add_months(CURRENT_DATE(), -4)), -6)
# MAGIC         )
# MAGIC         AND LAST_DAY(last_day(add_months(CURRENT_DATE(), -4)))
# MAGIC       )
# MAGIC   ) claim ON claim.cli_num = p.cli_num
# MAGIC WHERE
# MAGIC   tpolicys.agt_code IS NOT NULL
# MAGIC   AND claim.cli_num IS NOT NULL
# MAGIC GROUP BY
# MAGIC   tpolicys.agt_code;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_claimed3MO;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_claimed3MO AS
# MAGIC SELECT
# MAGIC   tpolicys.agt_code,
# MAGIC   COUNT(DISTINCT p.cli_num) AS 3MO_CUS_CLAIM
# MAGIC FROM
# MAGIC   vn_published_cas_db.tcoverages
# MAGIC   LEFT JOIN vn_published_cas_db.tpolicys ON tpolicys.pol_num = tcoverages.pol_num
# MAGIC   LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC   AND p.LINK_TYP = 'O'
# MAGIC   AND p.REC_STATUS = 'A'
# MAGIC   LEFT JOIN vn_published_ams_db.tams_agents ON tams_agents.agt_code = tpolicys.agt_code
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT cli_num
# MAGIC     FROM
# MAGIC       vn_published_cas_db.TCLAIM_DETAILS
# MAGIC     WHERE
# MAGIC       tclaim_details.clm_stat_code = 'A'
# MAGIC       AND (
# MAGIC         tclaim_details.clm_eff_dt BETWEEN LAST_DAY(
# MAGIC           add_months(last_day(add_months(CURRENT_DATE(), -4)), -3)
# MAGIC         )
# MAGIC         AND LAST_DAY(last_day(add_months(CURRENT_DATE(), -4)))
# MAGIC       )
# MAGIC   ) claim ON claim.cli_num = p.cli_num
# MAGIC WHERE
# MAGIC   tpolicys.agt_code IS NOT NULL
# MAGIC   AND claim.cli_num IS NOT NULL
# MAGIC GROUP BY
# MAGIC   tpolicys.agt_code;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_cus_new3MO;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_cus_new3MO AS
# MAGIC SELECT
# MAGIC   servicing_agent,
# MAGIC   COUNT(DISTINCT b.cli_num) AS 3MO_CUS_NEW
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       servicing_agent,
# MAGIC       cli_num
# MAGIC     FROM
# MAGIC       aso_pol_serviced
# MAGIC     GROUP BY
# MAGIC       servicing_agent,
# MAGIC       cli_num
# MAGIC   ) a
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT cli_num
# MAGIC     FROM
# MAGIC       aso_cus_typeLTD
# MAGIC     WHERE
# MAGIC       JOIN_DT BETWEEN LAST_DAY(
# MAGIC         add_months(last_day(add_months(CURRENT_DATE(), -4)), -3)
# MAGIC       )
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -4))
# MAGIC   ) b ON a.cli_num = b.cli_num
# MAGIC WHERE
# MAGIC   b.cli_num IS NOT NULL
# MAGIC GROUP BY
# MAGIC   servicing_agent;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_cus_new6MO;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_cus_new6MO AS
# MAGIC SELECT
# MAGIC   servicing_agent,
# MAGIC   COUNT(DISTINCT b.cli_num) AS 6MO_CUS_NEW
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       servicing_agent,
# MAGIC       cli_num
# MAGIC     FROM
# MAGIC       aso_pol_serviced
# MAGIC     GROUP BY
# MAGIC       servicing_agent,
# MAGIC       cli_num
# MAGIC   ) a
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT cli_num
# MAGIC     FROM
# MAGIC       aso_cus_typeLTD
# MAGIC     WHERE
# MAGIC       JOIN_DT BETWEEN LAST_DAY(
# MAGIC         add_months(last_day(add_months(CURRENT_DATE(), -4)), -6)
# MAGIC       )
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -4))
# MAGIC   ) b ON a.cli_num = b.cli_num
# MAGIC WHERE
# MAGIC   b.cli_num IS NOT NULL
# MAGIC GROUP BY
# MAGIC   servicing_agent;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_cus_new1YR;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_cus_new1YR AS
# MAGIC SELECT
# MAGIC   servicing_agent,
# MAGIC   COUNT(DISTINCT b.cli_num) AS 1YR_CUS_NEW
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       servicing_agent,
# MAGIC       cli_num
# MAGIC     FROM
# MAGIC       aso_pol_serviced
# MAGIC     GROUP BY
# MAGIC       servicing_agent,
# MAGIC       cli_num
# MAGIC   ) a
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       DISTINCT cli_num
# MAGIC     FROM
# MAGIC       aso_cus_typeLTD
# MAGIC     WHERE
# MAGIC       JOIN_DT BETWEEN LAST_DAY(
# MAGIC         add_months(last_day(add_months(CURRENT_DATE(), -4)), -12)
# MAGIC       )
# MAGIC       AND last_day(add_months(CURRENT_DATE(), -4))
# MAGIC   ) b ON a.cli_num = b.cli_num
# MAGIC WHERE
# MAGIC   b.cli_num IS NOT NULL
# MAGIC GROUP BY
# MAGIC   servicing_agent;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_agt_service;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_agt_service AS
# MAGIC SELECT
# MAGIC   a.*,
# MAGIC   LTD_CUS_LAPSED,
# MAGIC   1YR_CUS_LAPSED,
# MAGIC   3MO_CUS_LAPSED,
# MAGIC   1YR_CUS_MATURE,
# MAGIC   6MO_CUS_MATURE,
# MAGIC   3MO_CUS_MATURE,
# MAGIC   LTD_CUS_CLAIM,
# MAGIC   1YR_CUS_CLAIM,
# MAGIC   3MO_CUS_CLAIM,
# MAGIC   LTD_UPSELL,
# MAGIC   1YR_UPSELL,
# MAGIC   3MO_UPSELL,
# MAGIC   1YR_CUS_NEW,
# MAGIC   6MO_CUS_NEW,
# MAGIC   3MO_CUS_NEW
# MAGIC FROM
# MAGIC   ao_xtemp2 a
# MAGIC   LEFT JOIN aso_lapsedLTD b ON a.SERV_AGT = b.agt_code
# MAGIC   LEFT JOIN aso_lapsed1YR c ON a.SERV_AGT = c.agt_code
# MAGIC   LEFT JOIN aso_lapsed3MO d ON a.SERV_AGT = d.agt_code
# MAGIC   LEFT JOIN aso_matured6MO e ON a.SERV_AGT = e.agt_code
# MAGIC   LEFT JOIN aso_matured1YR f ON a.SERV_AGT = f.agt_code
# MAGIC   LEFT JOIN aso_matured3MO g ON a.SERV_AGT = g.agt_code
# MAGIC   LEFT JOIN aso_claimedLTD h ON a.SERV_AGT = h.agt_code
# MAGIC   LEFT JOIN aso_claimed1YR i ON a.SERV_AGT = i.agt_code
# MAGIC   LEFT JOIN aso_claimed3MO j ON a.SERV_AGT = j.agt_code
# MAGIC   LEFT JOIN aso_cus_new6MO k ON a.SERV_AGT = k.servicing_agent
# MAGIC   LEFT JOIN aso_cus_new1YR l ON a.SERV_AGT = l.servicing_agent
# MAGIC   LEFT JOIN aso_cus_new3MO m ON a.SERV_AGT = m.servicing_agent
# MAGIC   LEFT JOIN (
# MAGIC     SELECT
# MAGIC       servicing_agent,
# MAGIC       SUM(issued_ind) AS LTD_UPSELL,
# MAGIC       SUM(issued_1YR) AS 1YR_UPSELL,
# MAGIC       SUM(issued_3mo) AS 3MO_UPSELL
# MAGIC     FROM
# MAGIC       aso_pol_serviced a
# MAGIC       LEFT JOIN aso_cus_typeLTD b ON a.cli_num = b.cli_num
# MAGIC     WHERE
# MAGIC       b.join_dt < a.pol_eff_dt
# MAGIC       AND DATEDIFF(a.pol_eff_dt, b.join_dt) > 10
# MAGIC     GROUP BY
# MAGIC       servicing_agent
# MAGIC   ) upsell ON a.SERV_AGT = upsell.servicing_agent;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_xtemp3;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_xtemp3 AS
# MAGIC SELECT
# MAGIC   tpolicys.*,
# MAGIC   CASE
# MAGIC     WHEN POL_EFF_DT > EVAL_DT THEN 0
# MAGIC     ELSE 1
# MAGIC   END AS IND_ISSUED,
# MAGIC   CASE
# MAGIC     WHEN POL_EFF_DT <= EVAL_DT
# MAGIC     AND POL_TRMN_DT IS NULL THEN 1
# MAGIC     WHEN POL_EFF_DT <= EVAL_DT
# MAGIC     AND POL_TRMN_DT > EVAL_DT THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS IND_INFORCE,
# MAGIC   CASE
# MAGIC     WHEN POL_EFF_DT <= EVAL_DT
# MAGIC     AND POL_TRMN_DT IS NULL THEN 0
# MAGIC     WHEN POL_EFF_DT <= EVAL_DT
# MAGIC     AND POL_TRMN_DT <= EVAL_DT THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS IND_TERMINATED
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       p.cli_num,
# MAGIC       tpolicys.pol_num,
# MAGIC       tpolicys.pol_eff_dt,
# MAGIC       tpolicys.pol_stat_cd,
# MAGIC       CASE
# MAGIC         WHEN tpolicys.pol_trmn_dt IS NOT NULL THEN FROM_UNIXTIME(
# MAGIC           UNIX_TIMESTAMP(tpolicys.pol_trmn_dt, 'yyyy-MM-dd'),
# MAGIC           'yyyy-MM-dd'
# MAGIC         )
# MAGIC         WHEN tpolicys.pol_stat_cd IN (
# MAGIC           'F',
# MAGIC           'H'
# MAGIC         ) THEN FROM_UNIXTIME(
# MAGIC           UNIX_TIMESTAMP(polidm.xpry_dt, 'yyyy-MM-dd'),
# MAGIC           'yyyy-MM-dd'
# MAGIC         )
# MAGIC         ELSE FROM_UNIXTIME(
# MAGIC           UNIX_TIMESTAMP(tpolicys.pol_trmn_dt, 'yyyy-MM-dd'),
# MAGIC           'yyyy-MM-dd'
# MAGIC         )
# MAGIC       END AS POL_TRMN_DT,
# MAGIC       CASE
# MAGIC         WHEN tpolicys.pol_trmn_dt IS NULL THEN FROM_UNIXTIME(
# MAGIC           UNIX_TIMESTAMP(
# MAGIC             last_day(add_months(CURRENT_DATE(), -1)),
# MAGIC             'yyyy-MM-dd'
# MAGIC           ),
# MAGIC           'yyyy-MM-dd'
# MAGIC         )
# MAGIC         WHEN tpolicys.pol_trmn_dt IS NOT NULL THEN FROM_UNIXTIME(
# MAGIC           UNIX_TIMESTAMP(tpolicys.pol_trmn_dt, 'yyyy-MM-dd'),
# MAGIC           'yyyy-MM-dd'
# MAGIC         )
# MAGIC         WHEN tpolicys.pol_stat_cd IN (
# MAGIC           'F',
# MAGIC           'H'
# MAGIC         ) THEN FROM_UNIXTIME(
# MAGIC           UNIX_TIMESTAMP(polidm.xpry_dt, 'yyyy-MM-dd'),
# MAGIC           'yyyy-MM-dd'
# MAGIC         )
# MAGIC         ELSE FROM_UNIXTIME(
# MAGIC           UNIX_TIMESTAMP(tpolicys.pol_trmn_dt, 'yyyy-MM-dd'),
# MAGIC           'yyyy-MM-dd'
# MAGIC         )
# MAGIC       END AS EVAL_DT,
# MAGIC       tpolicys.wa_cd_1 AS WRITING_AGENT,
# MAGIC       tpolicys.agt_code AS SERVICING_AGENT,
# MAGIC       tpolicys.plan_code_base
# MAGIC     FROM
# MAGIC       vn_published_cas_db.tpolicys
# MAGIC       LEFT JOIN vn_published_cas_db.tclient_policy_links p ON tpolicys.pol_num = p.pol_num
# MAGIC       AND p.LINK_TYP = 'O'
# MAGIC       AND p.REC_STATUS = 'A'
# MAGIC       LEFT JOIN vn_curated_datamart_db.tpolidm_daily_CURATION_EXTERNAL polidm ON polidm.pol_num = tpolicys.pol_num
# MAGIC     WHERE
# MAGIC       tpolicys.pol_eff_dt <= last_day(add_months(CURRENT_DATE(), -4))
# MAGIC       AND tpolicys.pol_stat_cd NOT IN (
# MAGIC         'A',
# MAGIC         'N',
# MAGIC         'X',
# MAGIC         'R'
# MAGIC       )
# MAGIC     ORDER BY
# MAGIC       p.cli_num,
# MAGIC       tpolicys.pol_eff_dt
# MAGIC   ) tpolicys;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_xtemp3a;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_xtemp3a AS
# MAGIC SELECT
# MAGIC   t1.CLI_NUM,
# MAGIC   t1.EVAL_DT,
# MAGIC   (SUM(t1.IND_ISSUED)) AS SUM_of_IND_ISSUED,
# MAGIC   (SUM(t1.IND_INFORCE)) AS SUM_of_IND_INFORCE,
# MAGIC   (SUM(t1.IND_TERMINATED)) AS SUM_of_IND_TERMINATED,
# MAGIC   CASE
# MAGIC     WHEN (SUM(t1.IND_INFORCE)) > 0 THEN 'Inforce'
# MAGIC     WHEN (SUM(t1.IND_ISSUED)) =(SUM(t1.IND_TERMINATED)) THEN 'Lapsed'
# MAGIC     ELSE 'Unknown'
# MAGIC   END AS CUSTOMER_RELATIONSHIP
# MAGIC FROM
# MAGIC   aso_xtemp3 t1
# MAGIC GROUP BY
# MAGIC   t1.CLI_NUM,
# MAGIC   t1.EVAL_DT;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_xtemp3b;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_xtemp3b AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       t1.CLI_NUM,
# MAGIC       (COUNT(t1.CLI_NUM)) AS COUNT_of_CLI_NUM
# MAGIC     FROM
# MAGIC       aso_xtemp3a t1
# MAGIC     GROUP BY
# MAGIC       t1.CLI_NUM
# MAGIC   ) aa
# MAGIC WHERE
# MAGIC   COUNT_of_CLI_NUM > 1;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_xtemp3c;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_xtemp3c AS
# MAGIC SELECT
# MAGIC   a.*
# MAGIC FROM
# MAGIC   aso_xtemp3a a
# MAGIC   LEFT JOIN aso_xtemp3b b ON a.cli_num = b.cli_num
# MAGIC WHERE
# MAGIC   b.cli_num IS NOT NULL
# MAGIC ORDER BY
# MAGIC   a.cli_num,
# MAGIC   a.eval_dt;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_xtemp3d;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_xtemp3d AS
# MAGIC SELECT
# MAGIC   ROW_NUMBER () OVER (
# MAGIC     PARTITION BY CLI_NUM
# MAGIC     ORDER BY
# MAGIC       EVAL_DT
# MAGIC   ) AS ROW_VAL,
# MAGIC   a.*
# MAGIC FROM
# MAGIC   aso_xtemp3c a;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_xtemp3e;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_xtemp3e AS
# MAGIC SELECT
# MAGIC   a.*,
# MAGIC   b.eval_dt AS lapse_dt,
# MAGIC   concat(
# MAGIC     b.customer_relationship,
# MAGIC     '-',
# MAGIC     a.customer_relationship
# MAGIC   ) AS lbl
# MAGIC FROM
# MAGIC   aso_xtemp3d a
# MAGIC   LEFT JOIN aso_xtemp3d b ON a.row_val = b.row_val + 1
# MAGIC   AND a.cli_num = b.cli_num
# MAGIC WHERE
# MAGIC   b.customer_relationship = 'Lapsed'
# MAGIC   AND a.customer_relationship = 'Inforce';
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_xtemp3f;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_xtemp3f AS
# MAGIC SELECT
# MAGIC   a.servicing_agent,
# MAGIC   a.cli_num,
# MAGIC   CAST(
# MAGIC     floor(DATEDIFF(a.pol_eff_dt, lapse_dt) / 30) AS INT
# MAGIC   ) AS WINBACK_MONTHS,
# MAGIC   CASE
# MAGIC     WHEN CAST(floor(DATEDIFF(a.pol_eff_dt, lapse_dt)) AS INT) <= 180 THEN 1
# MAGIC     ELSE 0
# MAGIC   END AS WINBACK_180DAYS,
# MAGIC   CAST(
# MAGIC     floor(DATEDIFF(b.eval_dt, a.pol_eff_dt) / 30) AS INT
# MAGIC   ) AS WINBACK_RECENCY_MONTHS
# MAGIC FROM
# MAGIC   aso_xtemp3 a
# MAGIC   LEFT JOIN aso_xtemp3e b ON a.cli_num = b.cli_num
# MAGIC WHERE
# MAGIC   a.pol_eff_dt > lapse_dt
# MAGIC   AND a.pol_eff_dt <= b.eval_dt;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_agt_winback;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_agt_winback AS
# MAGIC SELECT
# MAGIC   servicing_agent AS winback_agt,
# MAGIC   COUNT(DISTINCT cli_num) AS WINBACK_CUS_COUNT,
# MAGIC   SUM(WINBACK_MONTHS) / COUNT(cli_num) AS WINBACK_MONTHS,
# MAGIC   SUM(WINBACK_180DAYS) / COUNT(cli_num) AS WINBACK_180DAYS,
# MAGIC   SUM(WINBACK_RECENCY_MONTHS) / COUNT(cli_num) AS WINBACK_RECENCY_MONTHS
# MAGIC FROM
# MAGIC   aso_xtemp3f
# MAGIC WHERE
# MAGIC   WINBACK_180DAYS = 1
# MAGIC GROUP BY
# MAGIC   servicing_agent;
# MAGIC -- DROP TABLE IF EXISTS vn_curated_analytics_db.aso_agt_alldata;
# MAGIC   CREATE
# MAGIC   or replace TEMP VIEW aso_agt_alldata AS
# MAGIC SELECT
# MAGIC   LAST_DAY(last_day(add_months(CURRENT_DATE(), -4))) AS eval_dt,
# MAGIC   aso_agents.*,
# MAGIC   aso_agt_sales.ltd_cus_write_count,
# MAGIC   aso_agt_sales.ltd_cc_total,
# MAGIC   aso_agt_sales.ltd_cc_issued,
# MAGIC   aso_agt_sales.ltd_cc_not_taken,
# MAGIC   aso_agt_sales.ltd_cc_rejected,
# MAGIC   aso_agt_sales.ltd_cc_issued_base,
# MAGIC   aso_agt_sales.ltd_cc_issued_rider,
# MAGIC   aso_agt_sales.ltd_ape_total,
# MAGIC   aso_agt_sales.ltd_ape_issued,
# MAGIC   aso_agt_sales.ltd_ape_not_taken,
# MAGIC   aso_agt_sales.ltd_ape_rejected,
# MAGIC   aso_agt_sales.ltd_ape_issued_base,
# MAGIC   aso_agt_sales.ltd_ape_issued_rider,
# MAGIC   aso_agt_sales.ltd_fyc_total,
# MAGIC   aso_agt_sales.ltd_fyc_base,
# MAGIC   aso_agt_sales.ltd_fyc_rider,
# MAGIC   aso_agt_sales.ltd_prod_acc,
# MAGIC   aso_agt_sales.ltd_prod_ci,
# MAGIC   aso_agt_sales.ltd_prod_dis,
# MAGIC   aso_agt_sales.ltd_prod_glh,
# MAGIC   aso_agt_sales.ltd_prod_inv,
# MAGIC   aso_agt_sales.ltd_prod_lp,
# MAGIC   aso_agt_sales.ltd_prod_lts,
# MAGIC   aso_agt_sales.ltd_prod_med,
# MAGIC   aso_agt_sales.ltd_agent_activeness,
# MAGIC   aso_agt_sales.ltd_need_count,
# MAGIC   aso_agt_sales.3mo_cus_write_count,
# MAGIC   aso_agt_sales.3mo_cc_total,
# MAGIC   aso_agt_sales.3mo_cc_issued,
# MAGIC   aso_agt_sales.3mo_cc_not_taken,
# MAGIC   aso_agt_sales.3mo_cc_rejected,
# MAGIC   aso_agt_sales.3mo_cc_issued_base,
# MAGIC   aso_agt_sales.3mo_cc_issued_rider,
# MAGIC   aso_agt_sales.3mo_ape_total,
# MAGIC   aso_agt_sales.3mo_ape_issued,
# MAGIC   aso_agt_sales.3mo_ape_not_taken,
# MAGIC   aso_agt_sales.3mo_ape_rejected,
# MAGIC   aso_agt_sales.3mo_ape_issued_base,
# MAGIC   aso_agt_sales.3mo_ape_issued_rider,
# MAGIC   aso_agt_sales.3mo_fyc_total,
# MAGIC   aso_agt_sales.3mo_fyc_base,
# MAGIC   aso_agt_sales.3mo_fyc_rider,
# MAGIC   aso_agt_sales.3mo_prod_acc,
# MAGIC   aso_agt_sales.3mo_prod_ci,
# MAGIC   aso_agt_sales.3mo_prod_dis,
# MAGIC   aso_agt_sales.3mo_prod_glh,
# MAGIC   aso_agt_sales.3mo_prod_inv,
# MAGIC   aso_agt_sales.3mo_prod_lp,
# MAGIC   aso_agt_sales.3mo_prod_lts,
# MAGIC   aso_agt_sales.3mo_prod_med,
# MAGIC   aso_agt_sales.3mo_agent_activeness,
# MAGIC   aso_agt_sales.3mo_need_count,
# MAGIC   aso_agt_sales.1yr_cus_write_count,
# MAGIC   aso_agt_sales.1yr_cc_total,
# MAGIC   aso_agt_sales.1yr_cc_issued,
# MAGIC   aso_agt_sales.1yr_cc_not_taken,
# MAGIC   aso_agt_sales.1yr_cc_rejected,
# MAGIC   aso_agt_sales.1yr_cc_issued_base,
# MAGIC   aso_agt_sales.1yr_cc_issued_rider,
# MAGIC   aso_agt_sales.1yr_ape_total,
# MAGIC   aso_agt_sales.1yr_ape_issued,
# MAGIC   aso_agt_sales.1yr_ape_not_taken,
# MAGIC   aso_agt_sales.1yr_ape_rejected,
# MAGIC   aso_agt_sales.1yr_ape_issued_base,
# MAGIC   aso_agt_sales.1yr_ape_issued_rider,
# MAGIC   aso_agt_sales.1yr_fyc_total,
# MAGIC   aso_agt_sales.1yr_fyc_base,
# MAGIC   aso_agt_sales.1yr_fyc_rider,
# MAGIC   aso_agt_sales.1yr_prod_acc,
# MAGIC   aso_agt_sales.1yr_prod_ci,
# MAGIC   aso_agt_sales.1yr_prod_dis,
# MAGIC   aso_agt_sales.1yr_prod_glh,
# MAGIC   aso_agt_sales.1yr_prod_inv,
# MAGIC   aso_agt_sales.1yr_prod_lp,
# MAGIC   aso_agt_sales.1yr_prod_lts,
# MAGIC   aso_agt_sales.1yr_prod_med,
# MAGIC   aso_agt_sales.1yr_agent_activeness,
# MAGIC   aso_agt_sales.1yr_need_count,
# MAGIC   0 AS 3MO_Persistency,
# MAGIC   aso_agt_service.cus_serv_count,
# MAGIC   aso_agt_service.cus_serv_actv_count,
# MAGIC   aso_agt_service.cus_serv_term_count,
# MAGIC   aso_agt_service.cus_serv_tenure_tot / aso_agt_service.cus_serv_count AS cus_serv_tenure_avg,
# MAGIC   aso_agt_service.cus_serv_actv_tenure_avg,
# MAGIC   aso_agt_service.cus_serv_term_tenure_avg,
# MAGIC   aso_agt_service.cus_existing,
# MAGIC   aso_agt_service.cus_new,
# MAGIC   aso_agt_service.1yr_cus_new,
# MAGIC   aso_agt_service.6mo_cus_new,
# MAGIC   aso_agt_service.3mo_cus_new,
# MAGIC   aso_agt_service.ltd_cus_lapsed,
# MAGIC   aso_agt_service.1yr_cus_lapsed,
# MAGIC   aso_agt_service.3mo_cus_lapsed,
# MAGIC   aso_agt_service.1yr_cus_mature,
# MAGIC   aso_agt_service.6mo_cus_mature,
# MAGIC   aso_agt_service.3mo_cus_mature,
# MAGIC   aso_agt_service.ltd_cus_claim,
# MAGIC   aso_agt_service.1yr_cus_claim,
# MAGIC   aso_agt_service.3mo_cus_claim,
# MAGIC   aso_agt_service.ltd_upsell,
# MAGIC   aso_agt_service.1yr_upsell,
# MAGIC   aso_agt_service.3mo_upsell,
# MAGIC   0 AS 1YR_Persistency,
# MAGIC   aso_agt_winback.winback_cus_count,
# MAGIC   aso_agt_winback.winback_months,
# MAGIC   aso_agt_winback.winback_180days,
# MAGIC   aso_agt_winback.winback_recency_months,
# MAGIC   date_format(
# MAGIC     last_day(add_months(CURRENT_DATE(), -4)),
# MAGIC     'yyyy-MM'
# MAGIC   ) AS monthend_dt
# MAGIC FROM
# MAGIC   aso_agents
# MAGIC   LEFT JOIN aso_agt_sales ON aso_agents.agt_code = aso_agt_sales.writing_agent
# MAGIC   LEFT JOIN aso_agt_service ON aso_agents.agt_code = aso_agt_service.serv_agt
# MAGIC   LEFT JOIN aso_agt_winback ON aso_agents.agt_code = aso_agt_winback.winback_agt;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  *
# MAGIC from    aso_agt_alldata
# MAGIC where   agt_code='RO766'

# COMMAND ----------

'''%sql
set hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE
        IF NOT EXISTS agent_rfm
        (
                eval_dt                  DATE         ,
                agt_code                 VARCHAR(20)  ,
                AGENCY_IND               INT          ,
                BANCA_IND                INT          ,
                agt_nm                   VARCHAR(100) ,
                agt_status               VARCHAR(20)  ,
                agt_join_dt              DATE         ,
                agt_term_dt              DATE         ,
                can_dob                  DATE         ,
                apply_dt                 DATE         ,
                sex_code                 VARCHAR(20)  ,
                mar_stat_cd              VARCHAR(20)  ,
                ins_exp_ind              VARCHAR(20)  ,
                agt_chnl                 VARCHAR(20)  ,
                agt_addr                 STRING,
                agt_age                  VARCHAR(20)  ,
                agt_tenure_mths          INT          ,
                agt_app_mths             INT          ,
                Has_Mailbox              INT          ,
                Has_BusinessPhone        INT          ,
                Has_MobilePhone          INT          ,
                Has_Email                INT          ,
                offer_rank_cd            VARCHAR(20)  ,
                rank_cd                  VARCHAR(20)  ,
                agt_rank_mths            INT          ,
                RANK_JUMP                INT          ,
                comp_prvd_num            VARCHAR(20)  ,
                is_mgr_ind               VARCHAR(20)          ,
                is_rehire_ind            INT          ,
                loc_code                 VARCHAR(20)  ,
                br_code                  VARCHAR(20)  ,
                team_code                VARCHAR(20)  ,
                unit_code                VARCHAR(20)  ,
                agt_sup_cnt              INT          ,
                AGT_SEGMENT              VARCHAR(20)  ,
                EducationCount           INT          ,
                ExamCount                INT          ,
                TrainingCount            INT          ,
                AGT_VIO_COUNT            INT          ,
                ltd_cus_write_count      INT          ,
                ltd_cc_total             INT          ,
                ltd_cc_issued            INT          ,
                ltd_cc_not_taken         INT          ,
                ltd_cc_rejected          INT          ,
                ltd_cc_issued_base       INT          ,
                ltd_cc_issued_rider      INT          ,
                ltd_ape_total            INT          ,
                ltd_ape_issued           INT          ,
                ltd_ape_not_taken        INT          ,
                ltd_ape_rejected         INT          ,
                ltd_ape_issued_base      INT          ,
                ltd_ape_issued_rider     INT          ,
                ltd_fyc_total            INT          ,
                ltd_fyc_base             INT          ,
                ltd_fyc_rider            INT          ,
                ltd_prod_acc             INT          ,
                ltd_prod_ci              INT          ,
                ltd_prod_dis             INT          ,
                ltd_prod_glh             INT          ,
                ltd_prod_inv             INT          ,
                ltd_prod_lp              INT          ,
                ltd_prod_lts             INT          ,
                ltd_prod_med             INT          ,
                ltd_agent_activeness     FLOAT        ,
                ltd_need_count           FLOAT        ,
                3mo_cus_write_count      INT          ,
                3mo_cc_total             INT          ,
                3mo_cc_issued            INT          ,
                3mo_cc_not_taken         INT          ,
                3mo_cc_rejected          INT          ,
                3mo_cc_issued_base       INT          ,
                3mo_cc_issued_rider      INT          ,
                3mo_ape_total            INT          ,
                3mo_ape_issued           INT          ,
                3mo_ape_not_taken        INT          ,
                3mo_ape_rejected         INT          ,
                3mo_ape_issued_base      INT          ,
                3mo_ape_issued_rider     INT          ,
                3mo_fyc_total            INT          ,
                3mo_fyc_base             INT          ,
                3mo_fyc_rider            INT          ,
                3mo_prod_acc             INT          ,
                3mo_prod_ci              INT          ,
                3mo_prod_dis             INT          ,
                3mo_prod_glh             INT          ,
                3mo_prod_inv             INT          ,
                3mo_prod_lp              INT          ,
                3mo_prod_lts             INT          ,
                3mo_prod_med             INT          ,
                3mo_agent_activeness     FLOAT        ,
                3mo_need_count           FLOAT        ,
                1yr_cus_write_count      INT          ,
                1yr_cc_total             INT          ,
                1yr_cc_issued            INT          ,
                1yr_cc_not_taken         INT          ,
                1yr_cc_rejected          INT          ,
                1yr_cc_issued_base       INT          ,
                1yr_cc_issued_rider      INT          ,
                1yr_ape_total            INT          ,
                1yr_ape_issued           INT          ,
                1yr_ape_not_taken        INT          ,
                1yr_ape_rejected         INT          ,
                1yr_ape_issued_base      INT          ,
                1yr_ape_issued_rider     INT          ,
                1yr_fyc_total            INT          ,
                1yr_fyc_base             INT          ,
                1yr_fyc_rider            INT          ,
                1yr_prod_acc             INT          ,
                1yr_prod_ci              INT          ,
                1yr_prod_dis             INT          ,
                1yr_prod_glh             INT          ,
                1yr_prod_inv             INT          ,
                1yr_prod_lp              INT          ,
                1yr_prod_lts             INT          ,
                1yr_prod_med             INT          ,
                1yr_agent_activeness     FLOAT        ,
                1yr_need_count           FLOAT        ,
                3MO_Persistency          FLOAT        ,
                cus_serv_count           INT          ,
                cus_serv_actv_count      INT          ,
                cus_serv_term_count      INT          ,
                cus_serv_tenure_avg      INT          ,
                cus_serv_actv_tenure_avg INT          ,
                cus_serv_term_tenure_avg INT          ,
                cus_existing             INT          ,
                cus_new                  INT          ,
                1yr_cus_new              INT          ,
                6mo_cus_new              INT          ,
                3mo_cus_new              INT          ,
                ltd_cus_lapsed           INT          ,
                1yr_cus_lapsed           INT          ,
                3mo_cus_lapsed           INT          ,
                1yr_cus_mature           INT          ,
                6mo_cus_mature           INT          ,
                3mo_cus_mature           INT          ,
                ltd_cus_claim            INT          ,
                1yr_cus_claim            INT          ,
                3mo_cus_claim            INT          ,
                ltd_upsell               INT          ,
                1yr_upsell               INT          ,
                3mo_upsell               INT          ,
                1YR_Persistency          FLOAT        ,
                winback_cus_count        INT          ,
                winback_months           INT          ,
                winback_180days          INT          ,
                winback_recency_months   FLOAT
        )
        partitioned BY
        (
                monthend_dt string
        )
        stored AS orc;'''

# COMMAND ----------

agent_rfm = spark.sql(f"""
SELECT
        eval_dt                  ,
        agt_code                 ,
        AGENCY_IND               ,
        BANCA_IND                ,
        agt_nm                   ,
        agt_status               ,
        agt_join_dt              ,
        agt_term_dt              ,
        can_dob                  ,
        apply_dt                 ,
        sex_code                 ,
        mar_stat_cd              ,
        ins_exp_ind              ,
        agt_chnl                 ,
        agt_addr                 ,
        agt_age                  ,
        agt_tenure_mths          ,
        agt_app_mths             ,
        Has_Mailbox              ,
        Has_BusinessPhone        ,
        Has_MobilePhone          ,
        Has_Email                ,
        offer_rank_cd            ,
        rank_cd                  ,
        agt_rank_mths            ,
        RANK_JUMP                ,
        comp_prvd_num            ,
        is_mgr_ind               ,
        is_rehire_ind            ,
        loc_code                 ,
        br_code                  ,
        team_code                ,
        unit_code                ,
        agt_sup_cnt              ,
        AGT_SEGMENT              ,
        EducationCount           ,
        ExamCount                ,
        TrainingCount            ,
        AGT_VIO_COUNT            ,
        ltd_cus_write_count      ,
        ltd_cc_total             ,
        ltd_cc_issued            ,
        ltd_cc_not_taken         ,
        ltd_cc_rejected          ,
        ltd_cc_issued_base       ,
        ltd_cc_issued_rider      ,
        ltd_ape_total            ,
        ltd_ape_issued           ,
        ltd_ape_not_taken        ,
        ltd_ape_rejected         ,
        ltd_ape_issued_base      ,
        ltd_ape_issued_rider     ,
        ltd_fyc_total            ,
        ltd_fyc_base             ,
        ltd_fyc_rider            ,
        ltd_prod_acc             ,
        ltd_prod_ci              ,
        ltd_prod_dis             ,
        ltd_prod_glh             ,
        ltd_prod_inv             ,
        ltd_prod_lp              ,
        ltd_prod_lts             ,
        ltd_prod_med             ,
        ltd_agent_activeness     ,
        ltd_need_count           ,
        3mo_cus_write_count      ,
        3mo_cc_total             ,
        3mo_cc_issued            ,
        3mo_cc_not_taken         ,
        3mo_cc_rejected          ,
        3mo_cc_issued_base       ,
        3mo_cc_issued_rider      ,
        3mo_ape_total            ,
        3mo_ape_issued           ,
        3mo_ape_not_taken        ,
        3mo_ape_rejected         ,
        3mo_ape_issued_base      ,
        3mo_ape_issued_rider     ,
        3mo_fyc_total            ,
        3mo_fyc_base             ,
        3mo_fyc_rider            ,
        3mo_prod_acc             ,
        3mo_prod_ci              ,
        3mo_prod_dis             ,
        3mo_prod_glh             ,
        3mo_prod_inv             ,
        3mo_prod_lp              ,
        3mo_prod_lts             ,
        3mo_prod_med             ,
        3mo_agent_activeness     ,
        3mo_need_count           ,
        1yr_cus_write_count      ,
        1yr_cc_total             ,
        1yr_cc_issued            ,
        1yr_cc_not_taken         ,
        1yr_cc_rejected          ,
        1yr_cc_issued_base       ,
        1yr_cc_issued_rider      ,
        1yr_ape_total            ,
        1yr_ape_issued           ,
        1yr_ape_not_taken        ,
        1yr_ape_rejected         ,
        1yr_ape_issued_base      ,
        1yr_ape_issued_rider     ,
        1yr_fyc_total            ,
        1yr_fyc_base             ,
        1yr_fyc_rider            ,
        1yr_prod_acc             ,
        1yr_prod_ci              ,
        1yr_prod_dis             ,
        1yr_prod_glh             ,
        1yr_prod_inv             ,
        1yr_prod_lp              ,
        1yr_prod_lts             ,
        1yr_prod_med             ,
        1yr_agent_activeness     ,
        1yr_need_count           ,
        3MO_Persistency          ,
        cus_serv_count           ,
        cus_serv_actv_count      ,
        cus_serv_term_count      ,
        cus_serv_tenure_avg      ,
        cus_serv_actv_tenure_avg ,
        cus_serv_term_tenure_avg ,
        cus_existing             ,
        cus_new                  ,
        1yr_cus_new              ,
        6mo_cus_new              ,
        3mo_cus_new              ,
        ltd_cus_lapsed           ,
        1yr_cus_lapsed           ,
        3mo_cus_lapsed           ,
        1yr_cus_mature           ,
        6mo_cus_mature           ,
        3mo_cus_mature           ,
        ltd_cus_claim            ,
        1yr_cus_claim            ,
        3mo_cus_claim            ,
        ltd_upsell               ,
        1yr_upsell               ,
        3mo_upsell               ,
        1YR_Persistency          ,
        winback_cus_count        ,
        winback_months           ,
        winback_180days          ,
        winback_recency_months   ,
        `monthend_dt` AS monthend_dt
FROM
        aso_agt_alldata
""")

# COMMAND ----------

import pyspark.sql.functions as F
agent_rfm.filter(F.col("agt_code")=="RO766").display()

# COMMAND ----------

#result_df = spark.sql("""select * from vn_curated_analytics_db.agent_rfm""")

# COMMAND ----------

# df=spark.read.format("parquet").load(f'/mnt/prod/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/AGENT_RFM')
agent_rfm.createOrReplaceTempView('agent_rfm_prod')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agent_rfm_prod where agt_code = 'RO766' and eval_dt ='2024-03-31'

# COMMAND ----------


