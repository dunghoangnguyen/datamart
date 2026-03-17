# Databricks notebook source
# MAGIC %md
# MAGIC Changed to Pure Agency on November Run (Oct Monthend)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
import joblib

import copy 
from copy import deepcopy
import matplotlib.pyplot as plt
import scipy.cluster.hierarchy as shc

from IPython.display import display
from pyspark.sql.functions import col


pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)

# COMMAND ----------

def convert_wandisco2_orc_parquet(df):

    rdd = df.rdd.map(lambda x: x[-1])

    schema_df = rdd.toDF(sampleRatio=1)

    my_schema=list(schema_df.schema)

    null_cols = []

    # iterate over schema list to filter for NullType columns

    for st in my_schema:

        if str(st.dataType) == 'NullType' or  str(st.dataType) == 'NoneType':

            null_cols.append(st)

    for ncol in null_cols:

        mycolname = str(ncol.name)

        schema_df = schema_df.withColumn(mycolname, schema_df[mycolname].cast('string'))

    fileschema = schema_df.schema

    df_final = spark.createDataFrame(rdd,fileschema)

    return df_final

# COMMAND ----------


# Setting up parameters
from datetime import datetime, timedelta
import calendar

# Get the last month-end from current system date
#last_mthend = datetime.strftime(datetime.now().replace(day=1) - timedelta(days=1), '%Y-%m-%d')

x = 5 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
last_mthend = last_day_of_x_months_ago.strftime('%Y-%m-%d')
last_mthend_12 = (current_month - timedelta(days=366)).strftime('%Y-%m-%d')
last_mthend_sht = last_mthend[0:7]
print("Selected last_mthend = ", last_mthend)
print("Selected last_mthend_sht = ", last_mthend_sht)
print("Selected last_mthend_12 = ", last_mthend_12)


# COMMAND ----------

last_mthend


# COMMAND ----------


tagtdm = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_DAILY', header=True)
tagtdm =  tagtdm.filter(F.col('agt_join_dt')<=last_mthend)

#changed in 10/27 to re-align all data to monthen esprcially UCM

agent_tier = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS', header=True)
TAMS_AGENTS = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_AGENTS', header=True)

agent_scorecard = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/AGENT_SCORECARD/', header=True)
banca_banks = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_LOCATIONS', header=True)

tcustdm_daily = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_DAILY', header=True)
tcustdm_daily = tcustdm_daily.filter(F.col('lst_eff_dt')<=last_mthend)

tpolidm_daily = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_DAILY', header=True)
tpolidm_daily = tpolidm_daily.filter(F.col('pol_iss_dt')<=last_mthend)

tclaims_conso = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/TCLAIMS_CONSO_ALL', header=True)


tclient_details = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_DETAILS', header=True)

tams_candidates = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_AMS_DB/TAMS_CANDIDATES', header=True)

tpolicys = spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TPOLICYS/image_date={last_mthend}', header=True)

tcoverages = spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCOVERAGES/image_date={last_mthend}', header=True)

tclient_policy_links = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCLIENT_POLICY_LINKS', header=True)

tfield_values = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TFIELD_VALUES', header=True)
tfield = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TFIELD_VALUES/')


epos = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_POSSTG_DB/TAP_CLIENT_DETAILS', header=True)

#unique per plan code
#select *, row_number() over(PARTITION BY plan_code order by effective_qtr asc) as rown from vn_published_campaign_db.vn_plan_code_map

#select sub.* from (
#select *, row_number() over(PARTITION BY plan_code order by effective_qtr asc) as rown from vn_published_campaign_db.vn_plan_code_map) sub where sub.rown =1

vn_plan_code_map = spark.read.format("csv").load('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/nbv_margins/2023Q1/vn_plan_code_map_unique.csv', header = True)


#nbv margins not in PROD
#unique per plan code and effective qtr
#select sub.* from (
#select *, row_number() over(PARTITION BY plan_code, effective_qtr order by effective_qtr asc) as rown from vn_published_campaign_db.nbv_margin_histories) sub where sub.rown =1

nbv_margin_histories = spark.read.format("csv").load('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/nbv_margins/2023Q1/nbv_margins.csv', header = True)

#tpolidm_mthend

tpolidm_mthend = spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TPOLIDM_MTHEND/image_date={last_mthend}', header=True)

#tagtdm_mthend

tagtdm_mthend= spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TAGTDM_MTHEND/image_date={last_mthend}', header=True)

tcustdm_mthend= spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_MTHEND/image_date={last_mthend}', header=True)


#propensity t

existing_score = spark.read.format("csv").load('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/score/existing_202305_06.csv', header = True) 
existing_score = existing_score.filter(F.col('monthend_dt')==last_mthend_sht)
new_score = spark.read.format("csv").load('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/score/new_202305_06.csv', header = True) 
new_score = new_score.filter(F.col('month')==last_mthend_sht)
#customer lifestage

lifestage  = spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/CUST_LIFESTAGE/monthend_dt={last_mthend_sht}', header=True)
cus_rfm  = spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_ANALYTICS_DB/CUS_RFM/monthend_dt={last_mthend_sht}', header=True)

#lapse (scores is up to Feb-23)
#get rown =1
#select * , row_number() over (PARTITION BY pol_num order by month_sp desc) AS ROWN from vn_lab_project_lapse_model_db.lapse_mthly
lapse_score_next_due = spark.read.format("csv").load('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/score/lapse_scores_next_due.csv', header = True) 


#early lapse
#lapse prediction at UW 
#early lapse score up to from Jan 2021 - March2023 only in Azure

#chnanged in Sep after ADO 380 sign off
early_lapse = spark.read.format("parquet").load(f'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CUSTOMER_ANALYTICS_DB/EARLY_LAPSE_UW_POLICY_SCORE_DM/', header=True)
early_lapse = early_lapse.filter(F.col('pol_eff_dt')<=last_mthend)


#MOVE
muser_flat = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MUSER_FLAT' , header = True)
manulifemember_flat = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MANULIFEMEMBER_FLAT' , header = True)
movekey_flat = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MOVEKEY_FLAT' , header = True)
userstate_flat = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/USERSTATE_FLAT' , header = True)
hit_data = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_ADOBE_MOVE5_DB/HIT_DATA' , header = True)

#CWS
sf_account   = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/ACCOUNT' , header = True)
cws_hit_data = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_ADOBE_PWS_DB/HIT_DATA' , header = True)

#TPOS

tpos = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_REPORTS_DB/TPOS_COLLECTION/', header=True)

#CPM

cpm_sales = spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/TRACKINGM/') \
    .filter(F.col("cmpgn_id").contains("MIL-")) \
    .select("new_pol_cust_id","new_pol_ape","new_pol_nbv") \
    .groupBy("new_pol_cust_id") \
    .agg(F.sum("new_pol_ape").alias("new_pol_ape"), F.sum("new_pol_nbv").alias("new_pol_nbv")) \
    .dropDuplicates()

cpm_cust= spark.read.format("parquet").load('abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Curated/VN/Master/VN_CURATED_CAMPAIGN_DB/TARGETM_DM/') \
    .filter(F.col("cmpgn_id").contains("MIL-")) \
    .select("tgt_cust_id").distinct()



# COMMAND ----------


cli_type = tcustdm_daily.select('cli_num', 'cli_typ')
cli_contact = tcustdm_daily.select('cli_num', 'mobl_phon_num', 'email_addr')

agent_tier2 = agent_tier.filter(F.col('mdrt_desc').isin(['MDRT','TOT','COT']))\
    .select(
        'agt_code',
        'mdrt_ind',
        'mdrt_desc',
        'fc_ind',
        'fc_desc',
        'mba_ind',
        'mba_desc'
    )

agent_scorecard = agent_scorecard.filter(F.col('monthend_dt') == last_mthend)\
    .join(agent_tier2, on='agt_code', how='left')\
    .withColumn('mdrt_flag', F.when(F.col('agent_group')=='MDRT', 1).otherwise(0))\
    .withColumn('mdrt_tot_flag', F.when(F.col('mdrt_desc')=='TOT', 1).otherwise(0))\
    .withColumn('mdrt_cot_flag', F.when(F.col('mdrt_desc')=='COT', 1).otherwise(0))\
    .withColumn('fc_flag', F.when(F.col('agent_tier')=='FC', 1).otherwise(0))\
    .withColumn('mba_flag', F.when(F.col('agent_tier')=='MBA', 1).otherwise(0))\
    .withColumn('active_1m_flag', F.when(F.col('agent_tier')=='1mAA', 1).otherwise(0))\
    .withColumn('active_3m_flag', F.when(F.col('agent_tier')=='3mAA', 1).otherwise(0))\
    .select(
        agent_scorecard['agt_code'],
        'agent_group',
        'agent_tier',
        'agent_cluster',
        'agent_taskforce',
        'mdrt_flag',
        'mdrt_tot_flag',
        'mdrt_cot_flag',
        'fc_flag',
        'mba_flag',
        'active_1m_flag',
        'active_3m_flag',
    )


tclient_policy_links = tclient_policy_links.filter((tclient_policy_links.LINK_TYP == "O") & (tclient_policy_links.REC_STATUS == "A"))

tcoverages_all = tcoverages.select('pol_num','plan_code','vers_num','cvg_eff_dt','xpry_dt','ins_typ', 'dscnt_prem', 'prem_dur', 'face_amt', 'cvg_typ', 'cvg_reasn', 'cvg_stat_cd')\
                            .withColumn('prem_dur_pre', F.least(F.floor(F.datediff(F.lit(last_mthend), F.col('cvg_eff_dt'))/365.25), F.col('prem_dur')))\
                            .withColumn('prem_dur_pre', F.when(F.col('prem_dur_pre')>F.col('prem_dur'),F.col('prem_dur')).otherwise(F.col('prem_dur_pre')).cast('int'))

banca_banks  = banca_banks.filter((banca_banks.CHNL == "BK")).sort('loc_cd').dropDuplicates()

tfield_values = tfield_values.filter(tfield_values.FLD_NM == 'INS_TYP')

tclaims_conso = tclaims_conso.filter((tclaims_conso.claim_status =='A') &(tclaims_conso.claim_approved_date <=last_mthend))
                                     
tclaims_conso = tclaims_conso.groupBy('policy_number')\
    .agg((F.sum('claim_approved_amount')/23.145).alias('claim_amount'))\
    .select(col("policy_number").alias("pol_num"), col("claim_amount").alias("claim_amount"))
    


# Calculate the date of past premium due
tpolicys = tpolicys\
            .withColumn('prev_due', F.when(F.col('pmt_mode') =='12', F.add_months(F.col('pd_to_dt'),-12))
                                     .when(F.col('pmt_mode') =='01',F.add_months(F.col('pd_to_dt'),-1))
                                     .when(F.col('pmt_mode') =='03',F.add_months(F.col('pd_to_dt'),-3))
                                     .when(F.col('pmt_mode') =='06',F.add_months(F.col('pd_to_dt'),-6)))\
            .withColumn('inforce_yr', F.floor(F.datediff(F.coalesce('pol_trmn_dt', F.lit(last_mthend)), 'pol_eff_dt')/365.25))

epos_income = epos.groupBy('cli_num')\
    .agg(
        F.max('avg_mth_income').alias('max_income'))
    


tpos = tpos.filter((tpos.image_date >=last_mthend_12) & (tpos.image_date <= last_mthend))



# COMMAND ----------



tcoverages_dp = tcoverages.toDF(*[col.lower() for col in tcoverages.columns])
tfield_dp = tfield.toDF(*[col.lower() for col in tfield.columns])
tpolidm_daily_dp = tpolidm_daily.toDF(*[col.lower() for col in tpolidm_daily.columns])
tcustdm_daily_dp = tcustdm_daily.toDF(*[col.lower() for col in tcustdm_daily.columns])

fml_tmp = tpolidm_daily_dp.alias("pol") \
    .join(tcoverages_dp.alias("cov"), col("pol.pol_num") == col("cov.pol_num")) \
    .join(tfield_dp.alias("fld"), col("cov.rel_to_insrd") == col("fld.fld_valu")) \
    .where((col("fld.fld_nm") == "REL_TO_INSRD") & (~col("cov.rel_to_insrd").isin(["00", "01"]))) \
    .selectExpr("po_num", "cli_num", "cov.rel_to_insrd", "fld_valu_desc", "fld_valu_desc_eng").dropDuplicates()

#fml_tmp.show()

# COMMAND ----------

from pyspark.sql.functions import lit, when

fml_grandpa = fml_tmp.filter(col('rel_to_insrd') == '05').select('po_num').distinct().withColumn('dpnd_grandpa_ind', lit(1))
fml_parent = fml_tmp.filter(col('rel_to_insrd').isin(['51', '52'])).select('po_num').distinct().withColumn('dpnd_parent_ind', lit(1))
fml_spouse = fml_tmp.filter(col('rel_to_insrd') == '02').select('po_num').distinct().withColumn('dpnd_spouse_ind', lit(1))
fml_child = fml_tmp.filter(col('rel_to_insrd') == '03').select('po_num').distinct().withColumn('dpnd_child_ind', lit(1))
fml_sib = fml_tmp.filter(col('rel_to_insrd') == '04').select('po_num').distinct().withColumn('dpnd_sibling_ind', lit(1))
fml_oth = fml_tmp.filter(col('rel_to_insrd').isin(['10', '31'])).select('po_num').distinct().withColumn('dpnd_oth_ind', lit(1))

fmlDF = tcustdm_daily_dp.select(col('cli_num').alias('po_num')) \
    .join(fml_grandpa, on='po_num', how='left') \
    .join(fml_parent, on='po_num', how='left') \
    .join(fml_spouse, on='po_num', how='left') \
    .join(fml_child, on='po_num', how='left') \
    .join(fml_sib, on='po_num', how='left') \
    .join(fml_oth, on='po_num', how='left') \
    .select(
        'po_num',
        when(col('dpnd_grandpa_ind').isNull(), 0).otherwise(col('dpnd_grandpa_ind')).alias('dpnd_grandpa_ind'),
        when(col('dpnd_parent_ind').isNull(), 0).otherwise(col('dpnd_parent_ind')).alias('dpnd_parent_ind'),
        when(col('dpnd_spouse_ind').isNull(), 0).otherwise(col('dpnd_spouse_ind')).alias('dpnd_spouse_ind'),
        when(col('dpnd_child_ind').isNull(), 0).otherwise(col('dpnd_child_ind')).alias('dpnd_child_ind'),
        when(col('dpnd_sibling_ind').isNull(), 0).otherwise(col('dpnd_sibling_ind')).alias('dpnd_sibling_ind'),
        when(col('dpnd_oth_ind').isNull(), 0).otherwise(col('dpnd_oth_ind')).alias('dpnd_oth_ind')
    ) \
    .where((col('dpnd_grandpa_ind') == 1) |
           (col('dpnd_parent_ind') == 1) |
           (col('dpnd_spouse_ind') == 1) |
           (col('dpnd_child_ind') == 1) |
           (col('dpnd_sibling_ind') == 1) |
           (col('dpnd_oth_ind') == 1)
    )

# COMMAND ----------

# Get insurance type and APE of the first and second (if any) policy
tpoli_first = tpolidm_mthend.select('po_num', 'pol_num', 'plan_code', 'pol_eff_dt', 'tot_ape')\
    .join(tpolicys.select('pol_num', 'ins_typ_base'), on='pol_num')\
    .withColumn('rn', F.row_number().over(Window.partitionBy('po_num').orderBy('pol_eff_dt')))\
    .join(tfield_values.select('fld_valu', 'fld_valu_desc_eng'), on=tpolicys['ins_typ_base'] == tfield_values['fld_valu'], how='left')\
    .groupBy('po_num')\
    .pivot('rn', [1, 2])\
    .agg(
        F.first('plan_code').alias('plan_code'),
        F.first('ins_typ_base').alias('ins_typ_base'),
        F.first('pol_eff_dt').alias('pol_eff_dt'),
        F.first('tot_ape').alias('tot_ape'),
        F.first('fld_valu_desc_eng').alias('ins_typ_desc')
    )\
    .select(
        'po_num',
        F.col('1_plan_code').alias('plan_code_1'),
        F.col('1_ins_typ_base').alias('ins_typ_base_1'),
        F.to_date(F.col('1_pol_eff_dt')).alias('pol_eff_dt_1'),
        F.col('1_tot_ape').cast('int').alias('tot_ape_1'),
        F.col('1_ins_typ_desc').alias('ins_typ_desc_1'),
        F.col('2_plan_code').alias('plan_code_2'),
        F.to_date(F.col('2_pol_eff_dt')).alias('pol_eff_dt_2'),
        F.col('2_tot_ape').cast('int').alias('tot_ape_2'),
        F.col('2_ins_typ_desc').alias('ins_typ_desc_2'),
        (F.floor(F.datediff(F.col('2_pol_eff_dt'), F.col('1_pol_eff_dt'))/365.25)).alias('yr_2nd_prod')
    )
#tpoli_first.display(20)

# COMMAND ----------

#early_lapse_sum = early_lapse.groupBy(F.col('pol_num'))\
#    .agg(F.max(F.col('p_1')).alias('p_1'),
#         F.min(F.col('decile')).alias('decile'),
#         F.max(F.col('pol_eff_dt')).alias('lst_eff_dt'))

lapse_score_next_due = lapse_score_next_due.withColumn('lapse_score', F.col('lapse_score').cast('float'))

# COMMAND ----------

# MAGIC %md 
# MAGIC Create Views

# COMMAND ----------


tagtdm.createOrReplaceTempView("tagtdm")
#agent_tier2.createOrReplaceTempView("agent_tier2")
agent_tier.createOrReplaceTempView("agent_tier") 
TAMS_AGENTS.createOrReplaceTempView("TAMS_AGENTS")
agent_scorecard.createOrReplaceTempView("agent_scorecard")
banca_banks.createOrReplaceTempView("banca_banks")
tcustdm_daily.createOrReplaceTempView("tcustdm_daily")
cli_contact.createOrReplaceTempView("cli_contact")
cli_type.createOrReplaceTempView("cli_type")

tpolicys.createOrReplaceTempView("tpolicys")
tcoverages_all.createOrReplaceTempView("tcoverages_all")
tclient_policy_links.createOrReplaceTempView("tclient_policy_links")
tfield_values.createOrReplaceTempView("tfield_values")
#vn_plan_code_map.createOrReplaceTempView("vn_plan_code_map")
nbv_margin_histories.createOrReplaceTempView("nbv_margin_histories")

tpolidm_mthend.createOrReplaceTempView("tpolidm_mthend")
tagtdm_mthend.createOrReplaceTempView("tagtdm_mthend")
tcustdm_mthend.createOrReplaceTempView("tcustdm_mthend")

# Scoring tables
existing_score.createOrReplaceTempView("existing_score")
new_score.createOrReplaceTempView("new_score")
early_lapse.createOrReplaceTempView("early_lapse")

# MOVE and CWS tables
muser_flat.createOrReplaceTempView("muser_flat")
manulifemember_flat.createOrReplaceTempView("manulifemember_flat")
movekey_flat.createOrReplaceTempView("movekey_flat")
userstate_flat.createOrReplaceTempView("userstate_flat")
hit_data.createOrReplaceTempView("hit_data")
sf_account.createOrReplaceTempView("sf_account")
cws_hit_data.createOrReplaceTempView("cws_hit_data")
tpos.createOrReplaceTempView("tpos")


tclient_details.createOrReplaceTempView("tclient_details")

tams_candidates.createOrReplaceTempView("tams_candidates")
tclaims_conso.createOrReplaceTempView("tclaims_conso")



# COMMAND ----------

# MAGIC %md
# MAGIC Policy and Coverage Base

# COMMAND ----------



policy_base = spark.sql(f"""
    select cov.pol_num
    ,cov.cvg_eff_dt
    ,cov.dscnt_prem
    ,cov.FACE_AMT
    ,cov.plan_code
    ,cov.vers_num
    ,cov.cvg_typ
    ,cov.cvg_reasn
    ,cast(cov.prem_dur as int) prem_dur
    ,cov.prem_dur_pre
    ,cast(cov.prem_dur-cov.prem_dur_pre as int) as prem_dur_post
    ,pol.dist_chnl_cd
    ,pol.pol_stat_cd
    ,case when pol.pol_stat_cd in ('1','2','3','5') then 1 else 0 end as f_inforce_ind
    ,case when pol.pol_stat_cd in ('B') then 1 else 0 end as f_lapse_ind
    ,case when pol.pol_stat_cd in ('E') then 1 else 0 end as f_surr_ind
    ,case when pol.pol_stat_cd in ('F','H','D','M','T') then 1 else 0 end as f_mature_ind
    ,case when pol.pol_stat_cd in ('A') then 1 else 0 end as f_nottaken_ind
    ,case when pol.pol_stat_cd in ('C','L','N','R','X') then 1 else 0 end f_ter_ind
    ,case when pol.pol_stat_cd in ('4','7','9') then 1 else 0 end as f_paid_ind
    ,pol.pmt_mode
    ,pol.bill_mthd
    ,case when pol.agt_code = pol.wa_cd_1 then 1 else 0 end as f_same_agent
    ,tfield.fld_valu as ins_typ
    ,tfield.fld_valu_desc_eng as ins_typ_desc
    ,tclient.cli_num as po_num
    ,contact.mobl_phon_num as cli_mobile
    ,contact.email_addr as cli_email
    ,agt.loc_cd
    ,case when agt.comp_prvd_num not in ('04','05','34','36','97','98') then datediff(coalesce(agt.agt_term_dt, '{last_mthend}'), agt.agt_join_dt)/365.25 else null end as agt_tenure_yrs
    ,agt_scr.agent_group
    ,agt_scr.agent_tier
    ,agt_scr.agent_cluster
    ,agt_scr.agent_taskforce
    ,agt_scr.mdrt_flag
    ,agt_scr.mdrt_tot_flag
    ,agt_scr.mdrt_cot_flag
    ,agt_scr.active_1m_flag
    ,agt_scr.active_3m_flag
    ,ctyp.cli_typ
    ,bnk.office_cd
    ,case when pdm.lst_pmt_mthd = 'Séc' then 1 else 0 end as f_cheque
    ,case when pdm.lst_pmt_mthd = 'Tiền mặt' then 1 else 0 end as f_cash
    ,case when pdm.lst_pmt_mthd = 'Chuyển khoản' then 1 else 0 end as f_auto
    ,pol.inforce_yr
    from tpolicys pol
    left join tcoverages_all cov on pol.pol_num = cov.pol_num
    left join tpolidm_mthend pdm on pol.pol_num = pdm.pol_num
    left join tfield_values tfield on pol.ins_typ_base = tfield.fld_valu
    left join tclient_policy_links tclient on pol.pol_num =tclient.pol_num
    left join cli_contact contact on tclient.cli_num = contact.cli_num
    left join tagtdm agt on pol.agt_code = agt.agt_code
    left join agent_scorecard agt_scr on pol.agt_code = agt_scr.agt_code
    left join banca_banks bnk on agt.loc_cd = bnk.loc_cd
    left join cli_type ctyp on tclient.cli_num =ctyp.cli_num
    left join tagtdm_mthend sa on pdm.sa_code = sa.agt_code
    left join tagtdm_mthend wa on pdm.wa_code = wa.agt_code
    where cov.cvg_stat_cd in ('1','2','3','5','7')
    and sa.channel='Agency'
    and wa.channel='Agency'
--Add tpos
""")
#policy_base.where(F.col('f_inforce_ind')==1).select('pol_num', 'plan_code','prem_dur', 'prem_dur_pre', 'prem_dur_post').show(20)

agent_assignment = spark.sql("""
                             with pol_agt_base as 
                             (
                                 select pol.pol_num
                                ,pol.po_num
                                ,CASE
                                    WHEN sa.trmn_dt IS NOT NULL
                                        AND sa.comp_prvd_num IN ('01','04', '97', '98') THEN 'Orphaned'
                                    WHEN sa.comp_prvd_num = '01'
                                        AND pol.sa_code = pol.wa_code					THEN 'Original Agent'
                                    WHEN sa.comp_prvd_num = '01'                        THEN 'Reassigned Agent'
                                    WHEN sa.comp_prvd_num = '04'                        THEN 'Orphaned-Collector'
                                    WHEN sa.comp_prvd_num IN ('97', '98')               THEN 'Orphaned-Agent is SM'
                                    ELSE 'Unknown'
                                END                                                 AS cus_agt_rltnshp
                            from tpolidm_mthend pol
                            left join TAMS_AGENTS sa on pol.sa_code = sa.agt_code
                            left join TAMS_AGENTS wa on pol.wa_code = wa.agt_code
                                where wa.comp_prvd_num IN ('01','97','98') 
                                and sa.comp_prvd_num IN ('01','04','97','98')
                                and pol.pol_stat_cd in ('1','2','3','5')
                             ),
                             pol_agt_base_ind as 
                             (select a.* 
                                    ,IF(a.cus_agt_rltnshp IN ('Original Agent', 'Reassigned Agent'), 0, 1) as unassigned
                            from pol_agt_base a)

                            select po_num 
                                ,max(unassigned) as unassigned_ind 
                            from pol_agt_base_ind
                            group by po_num
                            
                             """
                             )

#Fully Inactive Customer

po_inactive = spark.sql("""
                      with pol_ind as 
                      (select pol.po_num
                        ,pol.pol_trmn_dt
                        ,case when pol.pol_stat_cd in ('1','2','3','5') then 1 else 0 end as f_inforce_ind
                    from  tpolidm_mthend pol
                      ),
                    pol_tag as (
                      select po_num, max(f_inforce_ind) as f_inforce_ind , max(pol_trmn_dt) as last_termination_date from pol_ind group by po_num
                        )
                    select po_num, f_inforce_ind, last_termination_date from pol_tag where f_inforce_ind = 0 
                      """
                        )


#Customers with Maturity After Monthwns
po_maturity = tpolidm_mthend.filter(F.col('xpry_dt')>= last_mthend).groupby(F.col('po_num')).agg(F.min(F.col('xpry_dt')).alias('min_mat_date'))
#tpolidm_mthend_xpry.createOrReplaceTempView("tpolidm_mthend_xpry")
#po_maturity = spark.sql("""select po_num, min(xpry_dt) as min_mat_date from tpolidm_mthend_xpry group by po_num """)


#Combine Scores
propensity_scores = spark.sql("""
                       with existing as 
                       (select cli_num as po_num , least(decile_inv
                                                    ,decile_ci
                                                    ,decile_lp
                                                    ,decile_lt
                                                    ,decile_acc
                                                    ,decile_med) as min_decile from existing_score),
                       new as 
                      (select po_num, least(ci_decile
                                        ,invst_decile
                                        ,lt_decile) as min_decile from new_score)
                      select po_num, min_decile from existing 
                      union all 
                      select po_num, min_decile from new
""")



move_info = spark.sql("""
                             with move_acc_mthend as
                             (
                                 select
                                mu.`_id` muser_id
                                ,mk.`value` movekey
                                ,to_date(substr(mk.activationdate,1,10)) activation_date
                                ,to_date(substr(urt.lastdatasync,1,10)) last_data_sync_date
                                                from
                                muser_flat mu
                                inner join manulifemember_flat me on (mu.`_id` = me.userid)
                                left join movekey_flat mk on (me.keyid = mk.`_id`)
                                left join userstate_flat urt on (mu.`_id` = urt.userid)
                            where
                                mk.activated = 1
                             ),
                             move_login_transactions as (
                                    select
                                        post_evar1 muser_id
                                        ,to_date(substr(date_time,1,10)) login_dt
                                        ,post_pagename
                                        ,concat(post_visid_high, post_visid_low) visitor_id
                                        ,concat(post_visid_high,post_visid_low,visit_num,visit_start_time_gmt) visit_id
                                        ,visit_page_num
                                        ,visit_num
                                        ,date_time
                                        ,case
                                            when post_mobileosversion like '%Android%' then 'Android'
                                            else 'iOS'
                                        end os
                                        ,row_number() over(partition by post_evar1 order by date_time asc) rw_num
                                    from
                                        hit_data
                                    where
                                        exclude_hit = 0
                                        and hit_source not in ('5', '7', '8', '9')
                                        and concat(post_visid_high, post_visid_low) is not null
                                ),
                                move_login as (
                                select
                                    muser_id
                                    ,max(login_dt) lst_login_dt
                                from
                                    move_login_transactions
                                where
                                    rw_num <> 1
                                    and login_dt <= last_day(add_months(current_date,-1))
                                group by
                                    muser_id
                                ),
                                move_info as (
                                select
                                    substr(a.movekey,2,length(a.movekey)-1) cli_num
                                    ,a.activation_date
                                    ,b.lst_login_dt
                                from
                                    move_acc_mthend a
                                    left join move_login b on (a.muser_id = b.muser_id)
                            )select * from move_info 
                         
                        """)
move_info = move_info.filter((F.col('lst_login_dt')<= last_mthend) & (F.col('activation_date')<= last_mthend))

move_info.createOrReplaceTempView("move_info")

move_information_mthend = spark.sql("""
                                    with rs_dis as (
                                    select
                                        cli_num
                                        ,activation_date
                                        ,lst_login_dt
                                        ,row_number() over(partition by cli_num order by activation_date desc,lst_login_dt desc) rw_num
                                    from
                                        move_info
                                )
                                select * from rs_dis where rw_num = 1
                                """)


cws_information_mthend = spark.sql("""
                                   with cws_acc as (
                                    select
                                        external_id__c cli_num
                                        ,mcf_user_id__pc acc_id
                                    from
                                        sf_account
                                    where
                                        mcf_user_id__pc is not null
                                )
                                ,cws_login_transactions as(
                                    select
                                        hd.post_evar37 as login_id
                                        ,concat(hd.post_visid_high, hd.post_visid_low, hd.visit_num) as visit_id
                                        ,hd.date_time as login_date_time
                                        ,row_number() over(partition by hd.post_evar37 order by hd.date_time asc) rw_num
                                    from
                                        cws_hit_data hd	
                                    where
                                        1=1
                                        and hd.exclude_hit = 0
                                        and hd.hit_source not in ('5', '7', '8', '9')
                                        and concat(hd.post_visid_high, hd.post_visid_low) is not null
                                        and hd.post_evar37 <> ''
                                        and hd.post_evar19 = '/portfolio/policies'
                                        and hd.user_server in ('hopdongcuatoi.manulife.com.vn','hopdong.manulife.com.vn')
                                )
                                ,cws_reg as (
                                    select
                                        login_id
                                        ,login_date_time reg_dt
                                    from
                                        cws_login_transactions
                                    where
                                        rw_num = 1
                                )
                                ,cws_login as (
                                    select
                                        login_id
                                        ,max(login_date_time) lst_login_dt
                                    from
                                        cws_login_transactions
                                    where
                                        rw_num > 1
                                        and login_date_time <= last_day(add_months(current_date,-1))
                                    group by
                                        login_id
                                )
                                ,cws_infor as (
                                    select
                                        a.cli_num
                                        ,b.reg_dt cws_joint_dt
                                        ,c.lst_login_dt
                                    from
                                        cws_acc a
                                        left join cws_reg b on (a.acc_id = b.login_id)
                                        left join cws_login c on (a.acc_id = c.login_id)
                                ) select * from cws_infor 
                                   
                                   """)

cws_information_mthend = cws_information_mthend.filter((F.col('lst_login_dt')<= last_mthend) & (F.col('cws_joint_dt')<= last_mthend))

#Payments Made in Last Year


payment_summary = spark.sql("""
                            select client_number as po_num
                            ,sum(transaction_amount)/23.145 as transaction_usd
                            from tpos
                            group by client_number
                            """)


#Face from datamart

po_face_amt = spark.sql("""
                        select 
                        
                    po_num, max(tot_face_amt)/23.145 as tot_face_amt_usd from tpolidm_mthend group by po_num
                        """)

po_agent = spark.sql("""with temp1 as (select a.po_num , 
                                IF((own.id_num = ams.id_num) OR 
                                (own.cli_nm=agt.agt_nm AND own.birth_dt=agt.birth_dt AND own.sex_code=agt.sex_code AND own.email_addr=agt.email_addr) OR
                                (own.cli_nm=agt.agt_nm AND own.birth_dt=agt.birth_dt AND own.sex_code=agt.sex_code AND own.mobl_phon_num=agt.mobl_phon_num)
                                ,1,0) AS f_owner_is_agent
                              from tpolidm_mthend a 
                              inner join tcustdm_daily own on a.po_num = own.cli_num
                              left join tagtdm agt on   a.wa_code = agt.agt_code
                              left join tams_candidates ams on agt.can_num =ams.can_num
                            )
                            select po_num, max(f_owner_is_agent) as f_owner_is_agent from temp1 group by po_num
                    """)


# COMMAND ----------

# MAGIC %md
# MAGIC Calculate NBV per Coverage

# COMMAND ----------


    
all_coverage_nbv = policy_base.withColumn('effective_qtr', F.when(F.month('cvg_eff_dt')<=3, F.concat(F.year('cvg_eff_dt')-1, F.lit(' Q3')) )
                                            .when(F.month('cvg_eff_dt')<=6, F.concat(F.year('cvg_eff_dt')-1, F.lit(' Q4')))
                                            .when(F.month('cvg_eff_dt')<=9, F.concat(F.year('cvg_eff_dt'), F.lit(' Q1')))
                                            .when(F.month('cvg_eff_dt')<=12, F.concat(F.year('cvg_eff_dt'), F.lit(' Q2'))))\
                                            .join(vn_plan_code_map.select('plan_code',
                                                 'nbv_margin_agency_affinity',
                                                 'nbv_margin_agency',
                                                 'nbv_margin_dmtm',
                                                 'nbv_margin_other_channel_affinity',
                                                 'nbv_margin_other_channel',
                                                 'nbv_margin_banca_other_banks',
                                                 'nbv_margin_banca_scb',
                                                 'nbv_margin_banca_tcb'),
                                                on='plan_code', how='left')\
                                            .join(nbv_margin_histories.select('plan_code','effective_qtr',
                                                 F.col('nbv_margin_agency_affinity').alias('nbv_margin_agency_affinity2'),
                                                 F.col('nbv_margin_agency').alias('nbv_margin_agency2'),
                                                 F.col('nbv_margin_dmtm').alias('nbv_margin_dmtm2'),
                                                 F.col('nbv_margin_other_channel_affinity').alias('nbv_margin_other_channel_affinity2'),
                                                 F.col('nbv_margin_other_channel').alias('nbv_margin_other_channel2'),
                                                 F.col('nbv_margin_banca_other_banks').alias('nbv_margin_banca_other_banks2'),
                                                 F.col('nbv_margin_banca_scb').alias('nbv_margin_banca_scb2'),
                                                 F.col('nbv_margin_banca_tcb').alias('nbv_margin_banca_tcb2')), on=['plan_code', 'effective_qtr'], how='left')\
                                            .withColumn('nbv_margin', F.when(F.col('loc_cd').isNull(),
                                                    F.when(F.col('dist_chnl_cd').isin(['03','10','14','16','17','18','19','22','23','24','25','29','30','31','32','33','39','41','44','47','49','51','52','53']), F.coalesce(F.col('nbv_margin_banca_other_banks'), F.col('nbv_margin_banca_other_banks2')))
                                                     .when(F.col('dist_chnl_cd').isin(['48']), F.coalesce(F.col('nbv_margin_other_channel_affinity'), F.col('nbv_margin_other_channel_affinity2')))
                                                     .when(F.col('dist_chnl_cd').isin(['01', '02', '08', '50', '*']), F.coalesce(F.col('nbv_margin_agency'), F.col('nbv_margin_agency2')))
                                                     .when(F.col('dist_chnl_cd').isin(['05','06','07','34','36']), F.coalesce(F.col('nbv_margin_dmtm'), F.col('nbv_margin_dmtm2')))
                                                     .when(F.col('dist_chnl_cd').isin(['09']), F.lit(-1.34041044648343))
                                                     .otherwise(F.coalesce(F.col('nbv_margin_other_channel'), F.col('nbv_margin_other_channel2'))))
                                            .when(F.col('dist_chnl_cd').isin(['*']), F.coalesce(F.col('nbv_margin_agency'), F.col('nbv_margin_agency2')))
                                            .when(F.col('loc_cd').like('TCB%'), F.coalesce(F.col('nbv_margin_banca_tcb'), F.col('nbv_margin_banca_tcb2')))
                                            .when(F.col('loc_cd').like('SAG%'), F.coalesce(F.col('nbv_margin_banca_scb'), F.col('nbv_margin_banca_scb2')))
                                            .otherwise(F.coalesce(F.col('nbv_margin_other_channel'), F.col('nbv_margin_other_channel2'))))\
                                            .withColumn('plan_nbv', F.col('dscnt_prem')/23.145*F.col('nbv_margin'))\
                                            .withColumn('coverage_ape', F.col('dscnt_prem')*12/(F.col('pmt_mode'))/23.145)\
                                            .withColumn('coverage_fa', F.col('face_amt')/23.145)\
                                            .withColumn('annual_flag', F.when(F.col('pmt_mode')=='12', 1).otherwise(0))\
                                            .withColumn('valid_email', F.when(F.col('cli_email').isNotNull(), 1).otherwise(0))\
                                            .withColumn('valid_mobile', F.when(F.col('cli_mobile').isNotNull(), 1).otherwise(0))\
                                            .withColumn('channel', F.when(F.col('dist_chnl_cd').isin(['01', '02', '08', '50', '*']), 'Agency')
                                                                    .when(F.col('dist_chnl_cd').isin(['05','06','07','34','36']), 'DMTM')
                                                                    .otherwise(F.col('office_cd')))

all_coverage_nbv = all_coverage_nbv.filter(~F.col('pol_stat_cd').isin(['6','8']))                                           
inforce_coverage_nbv = all_coverage_nbv.filter(F.col('f_inforce_ind')==1)

#inforce_coverage_nbv.createOrReplaceTempView("inforce_coverage_nbv")
#print("all_coverage_nbv:", all_coverage_nbv.count())
#print("inforce_coverage_nbv:", inforce_coverage_nbv.count())



# COMMAND ----------

# MAGIC %md
# MAGIC Channel Determination
# MAGIC Update to use same as models - Oct 2023
# MAGIC

# COMMAND ----------


agency_base = spark.sql(""" 
                    with policy_channel as (
                    select pol.pol_num
                    ,pol.po_num 
                    ,sagt.channel 
                    ,case when wagt.channel = 'Agency' and sagt.channel ='Agency' then 0 else 1 end as agency_ind
                        from tpolidm_mthend pol
                    left join tagtdm_mthend wagt on pol.wa_code = wagt.agt_code 
                    left join tagtdm_mthend sagt on pol.sa_code = sagt.agt_code 
                    where pol.pol_stat_cd in ('1','2','3','5','7')
                    ) --select count( distinct po_num), agency_ind from policy_channel group by agency_ind
                    select  po_num, channel, channel as channel_final from policy_channel group by po_num , channel having sum(agency_ind)=0
                    """
)
agency_base.count()

# COMMAND ----------

# MAGIC  %md
# MAGIC   <span style="font-size: 96px;"><strong>Customer Marketing Segmentation</strong></span>

# COMMAND ----------

all_cli_mkt_seg = all_coverage_nbv.groupBy('po_num')\
                    .agg(F.sum(F.col('coverage_ape')).alias('total_ape'),
                         F.countDistinct('pol_num').alias('pol_cnt'),
                         F.countDistinct(F.when(F.col('inforce_yr') >= 10, F.col('pol_num'))).alias('10yr_pol_cnt'))
all_cli_mkt_seg.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Policy and Client LTV

# COMMAND ----------

#LTV = Pre + Post

all_pol_ltv= all_coverage_nbv.groupby('pol_num')\
                            .agg(
                            F.sum('coverage_ape').alias('coverage_ape'),
                            F.sum(F.when(((F.col("cvg_typ") == 'R')), F.col('coverage_ape')).otherwise(0)).alias('rider_ape'),
                            F.count(F.when(((F.col("cvg_typ") == 'R')),  F.col('pol_num'))).alias('rider_cnt'),
                            F.sum('plan_nbv').alias('plan_nbv'),
                            F.min('prem_dur').alias('prem_dur'),
                            F.min('prem_dur_pre').alias('prem_dur_pre'),
                            (F.sum('plan_nbv')/F.sum('coverage_ape')).alias('cli_nbv_margin'),
                            (F.sum('plan_nbv')).alias('pol_ltv'),
                            (F.min('prem_dur')-(F.min('prem_dur_pre'))).alias('prem_dur_post'),
                            ((F.sum('plan_nbv'))*(1-(F.min('prem_dur_post')/(F.min('prem_dur'))))).alias('pol_ltv_post'),
                            ((F.sum('plan_nbv'))/((F.min('prem_dur'))*((F.min('prem_dur')-(F.min('prem_dur_pre')))))).alias('pol_ltv_post_v2'),
                            ((F.sum('plan_nbv'))*(1-((F.min('prem_dur_pre')/(F.min('prem_dur')))))).alias('pol_ltv_post_v3'),
                            ((F.sum('plan_nbv'))*((F.min('prem_dur_pre')/(F.min('prem_dur'))))).alias('pol_ltv_pre'))\
                            .join(tpolidm_mthend, on='pol_num', how='left')\
                            .join(tclaims_conso,on='pol_num', how='left')\
                            .select('pol_num', 
                                    'prem_dur', 
                                    'po_num', 
                                    'coverage_ape', 
                                    'plan_nbv' , 
                                    'cli_nbv_margin', 
                                    'pol_ltv', 
                                    'prem_dur_post', 
                                    'prem_dur_pre', 
                                    'pol_ltv_post',
                                    'pol_ltv_pre',
                                    'rider_ape',
                                    'rider_cnt',
                                    F.coalesce(F.col('pol_ltv_post_v3'), F.lit(0)).alias('pol_ltv_post_v3'),
                                    F.coalesce(F.col('claim_amount'), F.lit(0)).alias('claim_amount'))\
                            .join(early_lapse.select('pol_num', 'decile', F.col('decile').alias('early_lapse_decile'), 'p_1') , on = 'pol_num', how ='left')\
                            .join(lapse_score_next_due.select('pol_num', 'decile', F.col('decile').alias('next_due_lapse_decile'), 'lapse_score'), on = 'pol_num', how ='left')
                           
#all_pol_ltv.select('pol_num').distinct().count()

# COMMAND ----------


# add more stats on Pol ltv
# add lapse score to calculation logic
# new ltv = basic_ltv*(1-lapse_score) - total approved claim per policy
all_pol_ltv = all_pol_ltv.withColumn('mod_pol_ltv', ((F.col('pol_ltv')-F.col('claim_amount'))*(1-F.coalesce('lapse_score', 'p_1', F.lit(0)))))\
    .withColumn('mod_pol_ltv_pre', ((F.col('pol_ltv_pre')-F.col('claim_amount'))*(1-F.coalesce('lapse_score', 'p_1', F.lit(0)))))\
    .withColumn('mod_pol_ltv_post',((F.col('pol_ltv_post_v3')-F.col('claim_amount'))*(1-F.coalesce('lapse_score', 'p_1', F.lit(0)))))

# COMMAND ----------


#Agency Only

all_cli_ltv = all_pol_ltv.groupBy('po_num')\
                        .agg(
                             F.sum('coverage_ape').alias('coverage_ape'),
                               F.sum('rider_ape').alias('rider_ape'),
                               F.sum('rider_cnt').alias('rider_cnt'),
                            F.sum('plan_nbv').alias('plan_nbv'),
                            F.min('prem_dur').alias('min_prem_dur'),
                            (F.sum('plan_nbv')/F.sum('coverage_ape')).alias('cli_nbv_margin'),
                            F.sum('pol_ltv').alias('cli_ltv'),
                            F.sum('mod_pol_ltv').alias('cli_ltv_mod'),
                            F.sum('pol_ltv_pre').alias('cli_ltv_pre'),
                            F.sum('mod_pol_ltv_pre').alias('cli_ltv_pre_mod'),
                            F.sum('pol_ltv_post').alias('cli_ltv_post'),
                            F.sum('mod_pol_ltv_post').alias('cli_ltv_post_mod'),
                            F.sum('claim_amount').alias('claim_amount'),

                            F.min('early_lapse_decile').alias('early_lapse_decile'),
                            F.min('next_due_lapse_decile').cast('int').alias('next_due_lapse_decile')

                        )\
                        .join(agency_base, on='po_num', how='inner')


all_pol_ltv.createOrReplaceTempView("all_pol_ltv")
#all_pol_ltv.count()

all_cli_ltv.createOrReplaceTempView("all_cli_ltv")
#all_cli_ltv.count()

# COMMAND ----------

all_cli_ltv.count()

# COMMAND ----------

# MAGIC %md
# MAGIC Client Level Data

# COMMAND ----------


#Client Level Data
all_client_coverage = all_coverage_nbv.groupby('po_num')\
    .agg(
        F.min('cvg_eff_dt').alias('first_pol_eff_dt'),
       (F.datediff(F.lit(last_mthend), F.min('cvg_eff_dt'))/365.25).alias('client_tenure'),
        F.countDistinct('pol_num').alias('pol_count'),
        F.countDistinct('ins_typ_desc').alias('ins_typ_count'),
        F.countDistinct(F.when(F.col("ins_typ_desc") == "Term Life" ,F.col("pol_num"))).alias('term_pol'),
        F.countDistinct(F.when(F.col("ins_typ_desc") == "Endowment" ,F.col("pol_num"))).alias('endow_pol'),
        F.countDistinct(F.when(F.col("ins_typ_desc") == "Health Indemnity" ,F.col("pol_num"))).alias('health_indem_pol'),
        F.countDistinct(F.when(F.col("ins_typ_desc") == "Whole Life" ,F.col("pol_num"))).alias('whole_pol'),
        F.countDistinct(F.when(F.col("ins_typ_desc") == "Investment" ,F.col("pol_num"))).alias('investment_pol'),
        F.sum(F.when(F.col("ins_typ_desc").isin(["Term Life","Endowment","Whole Life"]),F.col("coverage_fa"))).alias("protection_fa"),
        F.sum(F.col("coverage_fa")).alias("protection_fa_all"),
        F.countDistinct(F.when(F.col("f_inforce_ind") == 1 ,F.col("pol_num"))).alias('inforce_pol'),
        F.countDistinct(F.when(F.col("f_lapse_ind") == 1 ,F.col("pol_num"))).alias('lapsed_pol'),
        F.countDistinct(F.when(F.col("f_surr_ind") == 1 ,F.col("pol_num"))).alias('surrendered_pol'),
        F.countDistinct(F.when(F.col("f_mature_ind") == 1 ,F.col("pol_num"))).alias('matured_pol'),
        F.countDistinct(F.when(F.col("f_nottaken_ind") == 1 ,F.col("pol_num"))).alias('nottaken_pol'),
        F.countDistinct(F.when(F.col("f_ter_ind") == 1 ,F.col("pol_num"))).alias('terminated_pol'),
        F.countDistinct(F.when(F.col("f_paid_ind") == 1 ,F.col("pol_num"))).alias('oth_paid_pol'),
        F.max('f_same_agent').alias('f_same_agent'),
        F.sum('f_cash').alias('f_cash'),
        F.sum('f_cheque').alias('f_cheque'),
        F.sum('f_auto').alias('f_auto'),
        F.mean('agt_tenure_yrs').alias('agt_tenure_yrs'),
        (F.when(F.sum('mdrt_flag')>=1, 1).otherwise(0)).alias('mdrt_flag'),
        (F.when(F.sum('mdrt_tot_flag')>=1, 1).otherwise(0)).alias('mdrt_tot_flag'),
        (F.when(F.sum('mdrt_cot_flag')>=1, 1).otherwise(0)).alias('mdrt_cot_flag'),
        (F.when(F.sum('active_1m_flag')>=1, 1).otherwise(0)).alias('active_1m_flag'),
        (F.when(F.countDistinct('pol_num')>=2, 1).otherwise(0)).alias('multi_prod'),
        (F.when(F.sum('annual_flag')>=1, 1).otherwise(0)).alias('annual_flag'),
        (F.when(F.sum('valid_email')>=1, 1).otherwise(0)).alias('valid_email'),
        (F.when(F.sum('valid_mobile')>=1, 1).otherwise(0)).alias('valid_mobile'),
        F.sum('coverage_ape').alias('coverage_ape'),
        F.sum('plan_nbv').alias('plan_nbv')          
)
all_client_level = all_client_coverage\
                    .withColumn('image_date', F.lit(last_mthend))\
                    .join(all_cli_ltv,on='po_num', how='inner')\
                    .join(propensity_scores, on='po_num', how='left')\
                    .join(tcustdm_daily, on=all_client_coverage['po_num']==tcustdm_daily['cli_num'], how='left')\
                    .withColumn('lst_purchase_mth', (F.datediff(F.lit(last_mthend), 'lst_eff_dt'))/30)\
                    .join(po_inactive, on='po_num', how='left')\
                    .withColumn('lst_termination_mth', (F.datediff(F.lit(last_mthend), po_inactive['last_termination_date']))/30)\
                    .join(po_maturity, on ='po_num', how='left')\
                    .withColumn('next_mat_date_mth', (F.datediff(po_maturity['min_mat_date'], F.lit(last_mthend)))/30)\
                    .withColumn('f_HCM', F.when(F.col('CITY')=='Hồ Chí Minh',1).otherwise(0))\
                    .withColumn('f_HN', F.when(F.col('CITY')=='Hà Nội',1).otherwise(0))\
                    .withColumn('f_DN', F.when(F.col('CITY')=='Đà Nẵng',1).otherwise(0))\
                    .withColumn('f_oth_city',F.when(F.col('CITY').isin(['Hồ Chí Minh','Hà Nội','Đà Nẵng'])==False,1).otherwise(0))\
                    .join(lifestage, on=all_client_coverage['po_num']==lifestage['client_number'], how='left')\
                    .withColumn('f_adult_self_insured', F.when(F.col('customer_segment')=='Adult Self Insured',1).otherwise(0))\
                    .withColumn('f_family', F.when(F.col('customer_segment')=='Family',1).otherwise(0))\
                    .withColumn('f_family_wkids', F.when(F.col('customer_segment')=='Family with Kids',1).otherwise(0))\
                    .withColumn('f_empty_nest', F.when(F.col('customer_segment')=='Empty Nester',1).otherwise(0))\
                    .withColumn('f_undefined_segment', F.when(F.col('customer_segment')=='Undefined Segmentation',1).otherwise(0))\
                    .withColumn('f_male', F.when(F.col('cus_gender')=='Male',1).otherwise(0))\
                    .join(cus_rfm, on='po_num', how='left')\
                    .join(move_information_mthend, on=all_client_coverage['po_num'] == move_information_mthend['cli_num'], how='left')\
                    .withColumn('move_reg', F.when(F.col('activation_date').isNotNull(),1).otherwise(0))\
                    .withColumn('move_tenure_days',  (F.datediff(F.lit(last_mthend), F.col('activation_date'))))\
                    .withColumn('move_last_log_days',  (F.datediff(F.lit(last_mthend), move_information_mthend['lst_login_dt'])))\
                    .join(cws_information_mthend, on=all_client_coverage['po_num'] == cws_information_mthend['cli_num'], how='left')\
                    .withColumn('cws_reg', F.when(F.col('cws_joint_dt').isNotNull(),1).otherwise(0))\
                    .withColumn('cws_tenure_days',  (F.datediff(F.lit(last_mthend), F.col('cws_joint_dt'))))\
                    .withColumn('cws_last_log_days',  (F.datediff(F.lit(last_mthend), cws_information_mthend['lst_login_dt'])))\
                    .join(all_cli_mkt_seg, on=all_client_coverage['po_num']==all_cli_mkt_seg['po_num'], how='left')\
                    .withColumn('f_vip_elite', F.when(F.col('total_ape')>=12961.76,1).otherwise(0))\
                    .withColumn('f_vip_plat', F.when((F.col('total_ape')>6480.88) & (F.col('total_ape')<12961.76),1).otherwise(0))\
                    .withColumn('f_vip_gold', F.when((F.col('total_ape')>=2808.38) & (F.col('total_ape')<6480.88),1).otherwise(0))\
                    .withColumn('f_vip_silver', F.when((F.col('total_ape')>=864.12) & (F.col('total_ape')<2808.38) & (F.col('10yr_pol_cnt')>=1),1).otherwise(0))\
                    .withColumn('existing_vip_seg', F.when(F.col('total_ape')>=12961.76, "f_vip_elite")
                                                     .when((F.col('total_ape')>6480.88) & (F.col('total_ape')<12961.76), "f_vip_plat")
                                                     .when((F.col('total_ape')>=2808.38) & (F.col('total_ape')<6480.88), "f_vip_gold")
                                                     .when((F.col('total_ape')>=864.12) & (F.col('total_ape')<2808.38) & (F.col('10yr_pol_cnt')>=1), "f_vip_silver").otherwise("Others"))\
                    .join(tpoli_first, on='po_num', how='left')\
                    .join(epos_income, on=all_client_coverage['po_num'] == epos_income['cli_num'], how='left')\
                    .withColumn('f_1st_term', F.when(F.col("ins_typ_desc_1") == "Term Life",1).otherwise(0))\
                    .withColumn('f_1st_endow', F.when(F.col("ins_typ_desc_1") == "Endowment",1).otherwise(0))\
                    .withColumn('f_1st_health_indem', F.when(F.col("ins_typ_desc_1") == "Health Indemnity",1).otherwise(0))\
                    .withColumn('f_1st_whole', F.when(F.col("ins_typ_desc_1") == "Whole Life",1).otherwise(0))\
                    .withColumn('f_1st_invest', F.when(F.col("ins_typ_desc_1") == "Investment",1).otherwise(0))\
                    .withColumn('f_2nd_term', F.when(F.col("ins_typ_desc_2") == "Term Life",1).otherwise(0))\
                    .withColumn('f_2nd_endow', F.when(F.col("ins_typ_desc_2") == "Endowment",1).otherwise(0))\
                    .withColumn('f_2nd_health_indem', F.when(F.col("ins_typ_desc_2") == "Health Indemnity",1).otherwise(0))\
                    .withColumn('f_2nd_whole', F.when(F.col("ins_typ_desc_2") == "Whole Life",1).otherwise(0))\
                    .withColumn('f_2nd_invest', F.when(F.col("ins_typ_desc_2") == "Investment",1).otherwise(0))\
                    .join(agent_assignment , on='po_num', how='left')\
                    .join(po_face_amt, on='po_num', how='left')\
                    .join(po_agent, on='po_num', how='left')\
                    .select(all_client_coverage['po_num'],
                             'min_prem_dur',
                            'first_pol_eff_dt',
                            'sex_code',
                            'client_tenure',
                            'tot_face_amt_usd',
                            'pol_count',
                            'ins_typ_count',
                            'term_pol',
                            'endow_pol',
                            'health_indem_pol',
                            'whole_pol',
                            'investment_pol',
                            'inforce_pol',
                            'lapsed_pol',
                            'surrendered_pol',
                            'matured_pol',
                            'nottaken_pol',
                            'terminated_pol',
                            'oth_paid_pol',
                            'f_same_agent',
                            'f_cash',
                            'f_cheque',
                            'f_auto',
                            'f_owner_is_agent',
                            'agt_tenure_yrs',
                            'mdrt_flag',
                            'mdrt_tot_flag',
                            'mdrt_cot_flag',
                            'active_1m_flag',
                            'multi_prod',
                            'annual_flag',
                            'valid_email',
                            'valid_mobile',
                            all_client_coverage['coverage_ape'],
                            all_client_coverage['plan_nbv'],
                            'rider_ape',
                            'rider_cnt',
                            'cli_nbv_margin',
                            'cli_ltv',
                            'cli_ltv_pre',
                            'cli_ltv_post',
                            'cli_ltv_mod',
                            'cli_ltv_pre_mod',
                            'cli_ltv_post_mod',
                            'claim_amount',
                            F.least('next_due_lapse_decile','early_lapse_decile').alias('lapse_decile'),
                            'channel_final',
                            'channel',
                            'min_decile',
                            'cur_age',
                            'FRST_ISS_AGE',
                            'CITY',
                            (F.col('MTHLY_INCM')/23.145).alias('MTHLY_INCM'),
                            F.coalesce(F.col('max_income')/23.145, (F.col('MTHLY_INCM')/23.145)).alias('INCM_2'),
                            # Add adjusted income based on avg. inflation (2.98%/year)
                            ((F.col('MTHLY_INCM')/23.145)*(1+F.col('client_tenure')*2.98/100)).alias('adj_mthly_incm'),
                            'unassigned_ind',
                            cus_rfm['NO_DPND'],
                            'f_HCM',
                            'f_HN',
                            'f_DN',
                            'f_oth_city',
                            'customer_segment',
                            'cus_age_band',
                            'dependent_age_band',
                            'cus_gender',
                            'f_adult_self_insured',
                            'f_family',
                            'f_family_wkids',
                            'f_empty_nest',
                            'f_undefined_segment',
                            'f_male',
                            'k_inf_cvg_acc',
                            'k_inf_cvg_ci',
                            'k_inf_cvg_inv',
                            'k_inf_cvg_lp',
                            'k_inf_cvg_lts',
                            'k_inf_cvg_med',
                            'f_addrchg_1m',
                            'f_addrchg_3m',
                            'f_addrchg_6m',
                            'f_addrchg_12m',
                            'f_occpchg_1m',
                            'f_occpchg_3m',
                            'f_occpchg_6m',
                            'f_occpchg_12m',
                            'activation_date',
                            'move_reg',
                            'cws_reg',
                            'move_tenure_days',
                            'move_last_log_days',
                            'cws_tenure_days',
                            'cws_last_log_days',
                            'pol_cnt',
                            'total_ape',
                            '10yr_pol_cnt',
                            'f_vip_elite',
                            'f_vip_plat',
                            'f_vip_gold',
                            'f_vip_silver',
                            'existing_vip_seg',
                            'f_1st_term',
                            'f_1st_endow',
                            'f_1st_health_indem',
                            'f_1st_whole',
                            'f_1st_invest',
                            'f_2nd_term',
                            'f_2nd_endow',
                            'f_2nd_health_indem',
                            'f_2nd_whole',
                            'f_2nd_invest',
                            'yr_2nd_prod',
                            'lst_termination_mth',
                            'next_mat_date_mth',
                            'lst_purchase_mth',
                            'lst_eff_dt',
                            'last_termination_date',
                            'min_mat_date',
                            'protection_fa',
                            'protection_fa_all',
                            F.when(F.col('term_pol')>0,1).otherwise(0).alias('f_term_pol'),
                            F.when(F.col('endow_pol')>0,1).otherwise(0).alias('f_endow_pol'),
                            F.when(F.col('health_indem_pol')>0,1).otherwise(0).alias('f_health_indem_pol'),
                            F.when(F.col('whole_pol')>0,1).otherwise(0).alias('f_whole_pol'),
                            F.when(F.col('investment_pol')>0,1).otherwise(0).alias('f_investment_pol'),
                            F.when(F.col('inforce_pol')>0, 1).otherwise(0).alias('inforce_ind'),
                            F.when((F.col('lst_termination_mth')>=0) & (F.col('lst_termination_mth') <6), 1).otherwise(0).alias('f_trmn_0_6m'), 
                            F.when((F.col('lst_termination_mth')>=6) & (F.col('lst_termination_mth') <12), 1).otherwise(0).alias('f_trmn_6_12m'), 
                            F.when((F.col('lst_termination_mth')>=12) & (F.col('lst_termination_mth') <18), 1).otherwise(0).alias('f_trmn_12_18m'), 
                            F.when((F.col('lst_purchase_mth')>=0) & (F.col('lst_purchase_mth') <6), 1).otherwise(0).alias('f_purchase_0_6m'), 
                            F.when((F.col('lst_purchase_mth')>=6) & (F.col('lst_purchase_mth') <12), 1).otherwise(0).alias('f_purchase_6_12m'), 
                            F.when((F.col('lst_purchase_mth')>=12) & (F.col('lst_purchase_mth') <18), 1).otherwise(0).alias('f_purchase_12_18m'), 
                            F.when((F.col('next_mat_date_mth')>=0) & (F.col('next_mat_date_mth') <12), 1).otherwise(0).alias('f_mat_0_12m'), 
                            F.when((F.col('next_mat_date_mth')>=12) & (F.col('next_mat_date_mth') <24), 1).otherwise(0).alias('f_mat_12_24m'), 
                            F.when(F.least('next_due_lapse_decile','early_lapse_decile')< 4,1).otherwise(0).alias('top1_3_lapse'),
                            F.when(F.col('rider_cnt')>0, 1).otherwise(0).alias('f_with_rider'),
                            'image_date'
    )
                    
#Fix negative income
#Cap adjusted income to max $20k
all_client_level = all_client_level.withColumn("adj_mthly_incm", 
                                               when(col("adj_mthly_incm") < 0, 864)\
                                                   .otherwise(
                                                       when(col("adj_mthly_incm") > 20000, 20000)\
                                                           .otherwise(col("adj_mthly_incm"))
                                                   )
                                            )

# COMMAND ----------

#all_client_level.count()

# COMMAND ----------

#Po Payment
tpolidm_mthend = tpolidm_mthend\
            .withColumn('prev_due', F.when(F.col('pmt_mode') =='12', F.add_months(F.col('pd_to_dt'),-12))
                                     .when(F.col('pmt_mode') =='01',F.add_months(F.col('pd_to_dt'),-1))
                                     .when(F.col('pmt_mode') =='03',F.add_months(F.col('pd_to_dt'),-3))
                                     .when(F.col('pmt_mode') =='06',F.add_months(F.col('pd_to_dt'),-6)))\
            .withColumn('prev_due_lag2', F.when(F.col('pmt_mode') =='12', F.add_months(F.col('pd_to_dt'),-24))
                                     .when(F.col('pmt_mode') =='01',F.add_months(F.col('pd_to_dt'),-2))
                                     .when(F.col('pmt_mode') =='03',F.add_months(F.col('pd_to_dt'),-6))
                                     .when(F.col('pmt_mode') =='06',F.add_months(F.col('pd_to_dt'),-12)))
            
tpolidm_mthend.createOrReplaceTempView("tpolidm_mthend")
tpos.createOrReplaceTempView("tpos")

base = spark.sql("""
                 select pol.pol_num
                        ,pol.po_num
                        ,pol.pd_to_dt
                        ,pol.prev_due
                       -- ,pos.transaction_date
                        ,min(datediff(transaction_date,prev_due)) as days_diff
                      --  ,case when datediff(transaction_date,prev_due) >= -60 and datediff(transaction_date,prev_due) <-30   then '1. > 30 days before due'
                      --        when datediff(transaction_date,prev_due) >= -30 and datediff(transaction_date,prev_due) <0   then '2. 1-30days before due' 
                      --        when datediff(transaction_date,prev_due) == 0 then '3. Same day due' 
                    ---       when datediff(transaction_date,prev_due) >= 1 and datediff(transaction_date,prev_due) <=30 then '4. 1-30 days after due' 
                     ---         when datediff(transaction_date,prev_due) >= 30 then '5. >30 days after due' 
                      --        else '6. No payment recorded within 60 days' end as payment1
                        --Indicators
                      ---  ,case when datediff(transaction_date,prev_due) >= -60 and datediff(transaction_date,prev_due) <-30   then 1 else 0 end as f_30_days_before_due
                      ---  ,case when datediff(transaction_date,prev_due) >= -30 and datediff(transaction_date,prev_due) <0   then 1 else 0 end as f_l30days_before_due
                      --  ,case when datediff(transaction_date,prev_due) == 0 then 1 else 0 end as f_same_day_due
                      --  ,case when datediff(transaction_date,prev_due) >= 1 and datediff(transaction_date,prev_due) <=30 then 1 else 0 end as f_l30_days_after_due
                      --  ,case when datediff(transaction_date,prev_due) >= 30 then 1 else 0 end as f_30_days_after_due
                       -- ,case when transaction_date is null then 1 else 0 end as f_no_payment_rec_60_days
                        --,add_months(pol.prev_due,-2)
                        --,add_months(pol.prev_due,2)
                from tpolidm_mthend pol left join 
                tpos pos on pol.pol_num = pos.policy_number and pos.transaction_date between add_months(pol.prev_due,-2) and add_months(pol.prev_due,2)
                where  transaction_date is not null 
                group by pol.pol_num
                        ,pol.po_num
                        ,pol.pd_to_dt
                        ,pol.prev_due
                 """)


base.createOrReplaceTempView("base")

base_ind = spark.sql("""
                      select *
                        ,case when days_diff >= -60 and days_diff <-30   then '1. > 30 days before due'
                              when days_diff >= -30 and days_diff <0   then '2. 1-30days before due' 
                              when days_diff == 0 then '3. Same day due' 
                              when days_diff >= 1 and days_diff <=30 then '4. 1-30 days after due' 
                              when days_diff >= 30 then '5. >30 days after due' 
                              else '6. No payment recorded within 60 days' end as payment1
                        --Indicators
                        ,case when days_diff >= -60 and days_diff <-30   then 1 else 0 end as f_30_days_before_due
                        ,case when days_diff >= -30 and days_diff <0   then 1 else 0 end as f_l30days_before_due
                        ,case when days_diff == 0 then 1 else 0 end as f_same_day_due
                        ,case when days_diff >= 1 and days_diff <=30 then 1 else 0 end as f_l30_days_after_due
                        ,case when days_diff >= 30 then 1 else 0 end as f_30_days_after_due
                        ,case when days_diff is null then 1 else 0 end as f_no_payment_rec_60_days from base 
                      """)


#Create Indicators

po_payment = base_ind.groupby('po_num').agg(F.max('f_30_days_before_due').alias('f_30_days_before_due'),
                                        F.max('f_l30days_before_due').alias('f_l30days_before_due'),
                                        F.max('f_same_day_due').alias('f_same_day_due'),
                                        F.max('f_l30_days_after_due').alias('f_l30_days_after_due'),
                                        F.max('f_30_days_after_due').alias('f_30_days_after_due'),
                                        F.max('f_no_payment_rec_60_days').alias('f_no_payment_rec_60_days'),
                                        F.countDistinct(F.when(F.col("payment1") == "f_30_days_before_due" ,F.col("pol_num"))).alias('k_30_days_before_due'),
                                        F.countDistinct(F.when(F.col("payment1") == "f_l30days_before_due" ,F.col("pol_num"))).alias('k_l30days_before_due'),
                                        F.countDistinct(F.when(F.col("payment1") == "f_same_day_due" ,F.col("pol_num"))).alias('k_same_day_due'),
                                        F.countDistinct(F.when(F.col("payment1") == "f_l30_days_after_due" ,F.col("pol_num"))).alias('k_l30_days_after_due'),
                                        F.countDistinct(F.when(F.col("payment1") == "f_30_days_after_due" ,F.col("pol_num"))).alias('k_30_days_after_due'),
                                        F.countDistinct(F.when(F.col("payment1") == "f_no_payment_rec_60_days" ,F.col("pol_num"))).alias('k_no_payment_rec_60_days')
                                        )
                                        
#Total Premium Payments in Last YEar

payment_summary = spark.sql("""
                            select client_number as po_num
                            ,sum(transaction_amount)/23.145 as transaction_usd
                            from tpos
                            group by client_number
                            """)

# COMMAND ----------



all_client_level = all_client_level.join(po_payment, on='po_num', how ='left')\
    .join(F.broadcast(cpm_sales), all_client_level['po_num'] == cpm_sales["new_pol_cust_id"], how='left')\
    .join(F.broadcast(cpm_cust).select('tgt_cust_id', F.col('tgt_cust_id').alias('lead')), all_client_level['po_num'] == cpm_cust['tgt_cust_id'], how='left')

# COMMAND ----------

#joim FML DF
all_client_level = all_client_level.join(fmlDF, on='po_num', how='left')\
                    .withColumn('f_dependent_ind', F.greatest(*["dpnd_grandpa_ind","dpnd_parent_ind","dpnd_spouse_ind","dpnd_child_ind","dpnd_sibling_ind","dpnd_oth_ind"]))\
                    .join(F.broadcast(payment_summary), on='po_num', how='left')
                    
                                      

# COMMAND ----------

# MAGIC %md 
# MAGIC Summary Stats : LTV

# COMMAND ----------

all_client_level.createOrReplaceTempView('all_client_level')

all_client_level_qtl = spark.sql("""select a.* 
                                 , ntile(10) over (order by a.adj_mthly_incm /*a.cli_ltv_mod*/ desc) as income_decile 
                                 , ntile(10) over (order by  a.cli_ltv_mod desc) as ltv_decile 
                                 , case when adj_mthly_incm is not null and adj_mthly_incm <43 then 43
                                        when adj_mthly_incm is not null and MTHLY_INCM >= 20000 then 20000 
                                        else adj_mthly_incm end as MTHLY_INCM_CAPPED
                                -- ,case when MTHLY_INCM is NULL then 0*12 else MTHLY_INCM*12 end as YRLY_INCM_IMP --5808 average income
                                 
                                 from all_client_level a 
                                  where inforce_ind = 1
                                 """)


# COMMAND ----------


all_client_level_qtl.createOrReplaceTempView('all_client_level_qtl')
fmlDF.createOrReplaceTempView('fmlDF')

# COMMAND ----------

#impute mean income capped per decile
all_client_level_qtl.createOrReplaceTempView('all_client_level_qtl')

all_client_level_qtl  = spark.sql("""
                                  select a.* ,
                                   0 as  YRLY_INCM_IMP,
                                    case when protection_fa is null then 0 else protection_fa end as protection_fa_imp,
                                    case when tot_face_amt_usd is null then 0 else tot_face_amt_usd end as protection_fa_all_imp,
                                    case when b.po_num is not null then 1 else NULL end as f_with_dependent
                                    
                                 from all_client_level_qtl a left join fmlDF b on a.po_num = b.po_num
                                  """)


# COMMAND ----------


#Protection Gap - available only to customers with declared income below 2000 and those with > 0 income (<top 1%)
#Income - > 0 and capped at 10k
protection_gap = all_client_level_qtl.filter((F.col('adj_mthly_incm')>0))\
                        .withColumn('protection_gap_v2', F.col('f_with_dependent')*(120*F.least(F.col('adj_mthly_incm'),F.lit(20000)) -  F.col('protection_fa_imp')))\
                        .withColumn('protection_gap_all', F.col('f_with_dependent')*(120*F.least(F.col('adj_mthly_incm'),F.lit(20000)) -  F.col('protection_fa_all_imp')))\
                        .withColumn('wallet_rem', (12*F.least(F.col('adj_mthly_incm'),F.lit(20000)) -  F.col('coverage_ape')))\
                        .withColumn('MTHLY_INCM_99', F.least(F.col('adj_mthly_incm'),F.lit(20000)))\
                        .select('po_num','protection_gap_v2','protection_gap_all','wallet_rem','MTHLY_INCM_99')


#all_client_level_qtl = all_client_level_qtl.withColumn('protection_gap_v2', F.col('f_with_dependent')*(120*F.col('MTHLY_INCM') -  F.col('protection_fa_imp')))\
#                                           .withColumn('protection_gap_all', F.col('f_with_dependent')*(120*F.col('MTHLY_INCM') -  F.col('protection_fa_all_imp')))\
#                                            .withColumn('wallet_rem,', (F.col('YRLY_INCM_IMP') -  F.col('coverage_ape')))

all_client_level_qtl = all_client_level_qtl.join(F.broadcast(protection_gap), on='po_num', how='left')


# COMMAND ----------

all_client_level_qtl.columns

# COMMAND ----------

all_client_level_qtl.persist()
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

all_client_level_qtl.repartition("image_date").write.mode("overwrite").partitionBy("image_date").parquet('/mnt/lab/vn/project/scratch/cseg_cltv/temp3/')

# COMMAND ----------

# all_client_level_qtl.count()

# COMMAND ----------

#all_client_level_qtl = spark.read.format("parquet").load('abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/cseg_cltv/temp3', header=True)
#all_client_level_qtl= all_client_level_qtl.filter(F.col('image_date')=='2023-12-31')

# COMMAND ----------

# all_client_level_qtl.select("adj_mthly_incm"
# ,"f_with_dependent"
# ,'protection_fa_all_imp'
# ,'unassigned_ind'
# ,'f_term_pol'
# ,'f_endow_pol'
# ,'f_health_indem_pol'
# ,'f_whole_pol'
# ,'f_investment_pol'
# ,'pol_cnt'
# ).summary().display()

# COMMAND ----------

#tclaims_conso.select('claim_received_date', 'claim_effective_date', 'payment_date', 'policy_number', 'effective_duration', 'length_issue', 'tat_tier1', 'tat_tier2').limit(10).display()
