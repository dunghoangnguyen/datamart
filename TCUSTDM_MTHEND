# Databricks notebook source
# DBTITLE 1,Step0. Load all parameters
from datetime import datetime, timedelta
import calendar

# Get the last month-end from current system date
#last_mthend = datetime.strftime(datetime.now().replace(day=1) - timedelta(days=1), '%Y-%m-%d')

x = 0 # Change to number of months ago (0: last month-end, 1: last last month-end, ...)
today = datetime.now()
first_day_of_current_month = today.replace(day=1)
current_month = first_day_of_current_month

for i in range(x):
    first_day_of_previous_month = current_month - timedelta(days=1)
    first_day_of_previous_month = first_day_of_previous_month.replace(day=1)
    current_month = first_day_of_previous_month

last_day_of_x_months_ago = current_month - timedelta(days=1)
last_mthend = last_day_of_x_months_ago.strftime('%Y-%m-%d')

print("Selected last_mthend = ", last_mthend)

# COMMAND ----------

# DBTITLE 1,Step1. Load paths leading to all parquets
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql import functions as F
from pyspark.sql.functions import trunc, date_add, current_date, date_format, concat, concat_ws, row_number, col, max, min, monotonically_increasing_id, when
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType
import pandas as pd

tclient_policy_links_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_POLICY_LINKS/'
tclient_addresses_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_ADDRESSES/'
tprovinces_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TPROVINCES/'
tdistricts_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TDISTRICTS/'
twards_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TWARDS/'
tpolicys_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TPOLICYS/'
tfield_values_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TFIELD_VALUES/'
legacy_cws_path = 'abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/legacy_cws.csv'
user_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/USER/'
tcas_cws_user_mappings_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCAS_CWS_USER_MAPPINGS/'
tcas_cws_users_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCAS_CWS_USERS/'
account_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/ACCOUNT/'
cws_hit_data_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_ADOBE_PWS_DB/HIT_DATA/'
movekey_flat_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MOVEKEY_FLAT/'
manulifemember_flat_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MANULIFEMEMBER_FLAT/'
muser_flat_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MUSER_FLAT/'
userstate_flat_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/USERSTATE_FLAT/'
move_hit_data_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_ADOBE_MOVE5_DB/HIT_DATA/'
tji_config_df_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TJI_CONFIG/'
tji_zap_qtn_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TJI_ZAP_QTN/'
tji_app_info_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TJI_APP_INFO/'
tclient_other_details_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_OTHER_DETAILS/'
tclient_details_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_DETAILS/'
tclient_service_registrations_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_SERVICE_REGISTRATIONS/'
toccupation_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TOCCUPATION/'
tsms_clients_path = 'abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CICS_DB/TSMS_CLIENTS/'

# COMMAND ----------

# DBTITLE 1,Step2. Read parquet files into Spark dataframes
# Read parquet files into Dataframes
# Prepare tables needed for tmp_cus_add_ful_mthend
tclient_policy_links = spark.read.format("parquet").load(tclient_policy_links_path)
tclient_addresses = spark.read.format("parquet").load(tclient_addresses_path)
tprovinces = spark.read.format("parquet").load(tprovinces_path)
tdistricts = spark.read.format("parquet").load(tdistricts_path)
twards = spark.read.format("parquet").load(twards_path)

# Prepare tables needed for tmp_lst_trmn_pol_mthend & tmp_lst_inf_pol_mthend & tmp_frt_inf_pol_mthend
tpolicys = spark.read.format("parquet").load(tpolicys_path)
tfield_values = spark.read.format("parquet").load(tfield_values_path)

# Prepare tables needed for cws_legacy_information_mthend
legacy_cws_schema = StructType([
    StructField("userid", StringType(), True),
    StructField("createddate", DateType(), True),
    StructField("eventdate", DateType(), True),
    StructField("reporting_date", StringType(), True)
])
legacy_cws = spark.read.format("csv").schema(legacy_cws_schema).option("header", "true").option("inferSchema", "true").option("mode", "DROPMALFORMED").load(legacy_cws_path)
user = spark.read.format("parquet").load(user_path)
tcas_cws_users = spark.read.format("parquet").load(tcas_cws_users_path)
tcas_cws_user_mappings = spark.read .format("parquet").load(tcas_cws_user_mappings_path)

# Prepare tables needed for cws_information_mthend
account = spark.read.format("parquet").load(account_path)
cws_hit_data = spark.read.format("parquet").load(cws_hit_data_path).select("post_evar37", "post_visid_high", "post_visid_low", "visit_num", "date_time", "exclude_hit", "hit_source", "post_evar19", "user_server")

# Prepare tables needed for move_information_mthend
movekey_flat = spark.read.format("parquet").load(movekey_flat_path)
manulifemember_flat = spark.read.format("parquet").load(manulifemember_flat_path)
muser_flat = spark.read.format("parquet").load(muser_flat_path)
userstate_flat = spark.read.format("parquet").load(userstate_flat_path)
move_hit_data = spark.read.format("parquet").load(move_hit_data_path).select("post_evar1", "date_time", "post_pagename", "post_visid_low", "post_visid_high",
"visit_start_time_gmt", "visit_page_num", "visit_num", "post_mobileosversion", "exclude_hit", "hit_source")

# Prepare tables needed for rs_tinc (Income information)
tji_config = spark.read.format("parquet").load(tji_config_df_path)
tji_zap_qtn = spark.read.format("parquet").load(tji_zap_qtn_path)
tji_app_info = spark.read.format("parquet").load(tji_app_info_path)
tclient_other_details = spark.read.format("parquet").load(tclient_other_details_path)
tclient_details = spark.read.format("parquet").load(tclient_details_path)
tclient_service_registrations = spark.read.format("parquet").load(tclient_service_registrations_path)
toccupation = spark.read.format("parquet").load(toccupation_path)
tsms_clients = spark.read.format("parquet").load(tsms_clients_path)

# Convert all column names into lowercase
tclient_policy_links_df = tclient_policy_links.toDF(*[col.lower() for col in tclient_policy_links.columns])
tclient_addresses_df = tclient_addresses.toDF(*[col.lower() for col in tclient_addresses.columns])
tprovinces_df = tprovinces.toDF(*[col.lower() for col in tprovinces.columns])
tdistricts_df = tdistricts.toDF(*[col.lower() for col in tdistricts.columns])
twards_df = twards.toDF(*[col.lower() for col in twards.columns])

tpolicys_df = tpolicys.toDF(*[col.lower() for col in tpolicys.columns])
tfield_values_df = tfield_values.toDF(*[col.lower() for col in tfield_values.columns])

legacy_cws_df = legacy_cws.toDF(*[col.lower() for col in legacy_cws.columns])
user_df = user.toDF(*[col.lower() for col in user.columns])
tcas_cws_user_mappings_df = tcas_cws_user_mappings.toDF(*[col.lower() for col in tcas_cws_user_mappings.columns])
tcas_cws_users_df = tcas_cws_users.toDF(*[col.lower() for col in tcas_cws_users.columns])
#legacy_cws.show(100)

account_df = account.toDF(*[col.lower() for col in account.columns])
cws_hit_data_df = cws_hit_data.toDF(*[col.lower() for col in cws_hit_data.columns])

movekey_flat_df = movekey_flat.toDF(*[col.lower() for col in movekey_flat.columns])
manulifemember_flat_df = manulifemember_flat.toDF(*[col.lower() for col in manulifemember_flat.columns])
muser_flat_df = muser_flat.toDF(*[col.lower() for col in muser_flat.columns])
userstate_flat_df = userstate_flat.toDF(*[col.lower() for col in userstate_flat.columns])
move_hit_data_df = move_hit_data.toDF(*[col.lower() for col in move_hit_data.columns])

tji_config_df = tji_config.toDF(*[col.lower() for col in tji_config.columns])
tji_zap_qtn_df = tji_zap_qtn.toDF(*[col.lower() for col in tji_zap_qtn.columns])
tji_app_info_df = tji_app_info.toDF(*[col.lower() for col in tji_app_info.columns])
tclient_other_details_df = tclient_other_details.toDF(*[col.lower() for col in tclient_other_details.columns])
tclient_details_df = tclient_details.toDF(*[col.lower() for col in tclient_details.columns])
tclient_service_registrations_df = tclient_service_registrations.toDF(*[col.lower() for col in tclient_service_registrations.columns])
toccupation_df = toccupation.toDF(*[col.lower() for col in toccupation.columns])
tsms_clients_df = tsms_clients.toDF(*[col.lower() for col in tsms_clients.columns])

# Filter partitions to keep data of the reporting month only
tprovinces_df = tprovinces_df.filter(col('image_date') == last_mthend)
twards_df = twards_df.filter(col('image_date') == last_mthend)
tclient_addresses_df = tclient_addresses_df.filter(col('image_date') == last_mthend)
tclient_policy_links_df = tclient_policy_links_df.filter(col('image_date') == last_mthend)
tdistricts_df = tdistricts_df.filter(col('image_date') == last_mthend)
tpolicys_df = tpolicys_df.filter(col('image_date') == last_mthend)
tfield_values_df = tfield_values_df.filter(col('image_date') == last_mthend)
cws_hit_data_df = cws_hit_data_df.filter(
    (cws_hit_data_df.exclude_hit == 0) &
    (~cws_hit_data_df.hit_source.isin(['5', '7', '8', '9'])) &
    (concat(cws_hit_data_df.post_visid_high, cws_hit_data_df.post_visid_low).isNotNull()) &
    (cws_hit_data_df.post_evar37 != '') &
    (cws_hit_data_df.post_evar19 == '/portfolio/policies') &
    (cws_hit_data_df.user_server.isin(['hopdongcuatoi.manulife.com.vn','hopdong.manulife.com.vn']))
)
move_hit_data_df = move_hit_data_df.filter(
    (move_hit_data_df.exclude_hit == 0) &
    (~move_hit_data_df.hit_source.isin(['5', '7', '8', '9'])) &
     (concat(move_hit_data_df.post_visid_high, move_hit_data_df.post_visid_low).isNotNull())
)

tji_config_df = tji_config_df.filter(col('image_date') == last_mthend)
tji_zap_qtn_df = tji_zap_qtn_df.filter(col('image_date') == last_mthend)
tji_app_info_df = tji_app_info_df.filter(col('image_date') == last_mthend)
tclient_other_details_df = tclient_other_details_df.filter(col('image_date') == last_mthend)
tclient_details_df = tclient_details_df.filter(col('image_date') == last_mthend)
tclient_service_registrations_df = tclient_service_registrations_df.filter(col('image_date') == last_mthend)
toccupation_df = toccupation_df.filter(col('image_date') == last_mthend)
#tsms_clients_df = tsms_clients_df.filter(col('image_date') == last_mthend)

# Check number of records
print(f"tprovinces: {tprovinces_df.count()}")
print(f"twards: {twards_df.count()}")
print(f"tclient_addresses: {tclient_addresses_df.count()}")
print(f"tclient_policy_links: {tclient_policy_links_df.filter((col('rec_status') == 'A') & col('link_typ').isin(['O','I','T'])).count()}")
print(f"tdistricts: {tdistricts_df.count()}")
print(f"tpolicys: {tpolicys_df.count()}")
print(f"tfield_values: {tfield_values_df.count()}")
print(f"legacy_cws: {legacy_cws_df.count()}")
print(f"user: {user_df.count()}")
print(f"tcas_cws_user_mappings: {tcas_cws_user_mappings_df.count()}")
print(f"tcas_cws_users: {tcas_cws_users_df.count()}")
print(f"account: {account_df.count()}")
print(f"cws_hit_data: {cws_hit_data_df.count()}")
print(f"movekey_flat: {movekey_flat_df.count()}")
print(f"manulifemember_flat: {manulifemember_flat_df.count()}")
print(f"muser_flat: {muser_flat_df.count()}")
print(f"userstate_flat: {userstate_flat_df.count()}")
print(f"move_hit_data: {move_hit_data_df.count()}")
print(f"tji_config: {tji_config_df.count()}")
print(f"tji_app_info: {tji_app_info_df.count()}")
print(f"tji_zap_qtn: {tji_zap_qtn_df.count()}")
print(f"tclient_other_details: {tclient_other_details_df.count()}")
print(f"tclient_details: {tclient_details_df.count()}")
print(f"tclient_service_registrations: {tclient_service_registrations_df.count()}")
print(f"toccupation: {toccupation_df.count()}")
print(f"tsms_clients: {tsms_clients_df.count()}")

# COMMAND ----------

# DBTITLE 1,Step3. tmp_cus_add_ful_mthend
tmp_cus_add_ful_mthend = tclient_policy_links_df.filter((tclient_policy_links_df.rec_status == 'A') & 
        (tclient_policy_links_df.link_typ.isin(['O', 'I', 'T'])) & 
        (tclient_policy_links_df.image_date == last_mthend)).join(tclient_addresses_df, 
        (tclient_policy_links_df.cli_num == tclient_addresses_df.cli_num) & 
        (tclient_policy_links_df.addr_typ == tclient_addresses_df.addr_typ) & 
        (tclient_policy_links_df.image_date == tclient_addresses_df.image_date), 'inner').join(tprovinces_df, 
          (tclient_addresses_df.zip_code.substr(1, 2) == tprovinces_df.prov_id) & 
          (tclient_addresses_df.image_date == tprovinces_df.image_date), 
          'left').join(tdistricts_df, 
          (tclient_addresses_df.zip_code.substr(3, 2) == tdistricts_df.dist_id) & 
          (tprovinces_df.prov_id == tdistricts_df.prov_id) & 
          (tprovinces_df.image_date == tdistricts_df.image_date), 
          'left').join(twards_df, 
          (tclient_addresses_df.zip_code.substr(5, 2) == twards_df.ward_id) & 
          (tdistricts_df.prov_id == twards_df.prov_id) & 
          (tdistricts_df.dist_id == twards_df.dist_id) & 
          (tdistricts_df.image_date == twards_df.image_date), 
          'left').select(
        tclient_addresses_df.cli_num,
        tclient_addresses_df.addr_1,
        tclient_addresses_df.addr_2,
        tclient_addresses_df.addr_3,
        tclient_addresses_df.addr_4,
        concat_ws(',', tclient_addresses_df.addr_1, tclient_addresses_df.addr_2, tclient_addresses_df.addr_3, tclient_addresses_df.addr_4).alias('full_address'),
        tclient_addresses_df.zip_code,
        tclient_addresses_df.addr_typ,
        tprovinces_df.prov_id,
        tprovinces_df.prov_nm.alias('city'),
        tdistricts_df.dist_id,
        tdistricts_df.dist_nm.alias('district'),
        twards_df.ward_id,
        twards_df.ward_nm.alias('ward'),
        row_number().over(Window.partitionBy(tclient_addresses_df.cli_num).orderBy(tclient_addresses_df.addr_typ.desc())).alias('rn')
    ).filter(col('rn') == 1).select(
        'cli_num',
        'addr_1',
        'addr_2',
        'addr_3',
        'addr_4',
        'full_address',
        'zip_code',
        'addr_typ',
        'prov_id',
        'city',
        'dist_id',
        'district',
        'ward_id',
        'ward'
    )

print("Number of records of tmp_cus_add_ful_mthend: ", tmp_cus_add_ful_mthend.count())
print("Number of columns of tmp_cus_add_ful_mthend: ", len(tmp_cus_add_ful_mthend.columns))
tmp_cus_add_ful_mthend.display(10)

# COMMAND ----------

# DBTITLE 1,Step4. Get list of last terminated policy per customer
rs_a = (
    tpolicys_df.alias("p")
    .join(
        tclient_policy_links_df.alias("l"),
        on=[
            col("p.pol_num") == col("l.pol_num"),
            col("p.image_date") == col("l.image_date"),
        ],
        how="inner",
    )
    .where(
        (col("l.link_typ") == "O")
        & col("p.pol_stat_cd").isin(["A", "B", "C", "D", "E", "F", "H", "L", "M", "T"])
        & (col("p.image_date") == last_mthend)
    )
    .groupBy(col("l.cli_num"))
    .agg(max(col("p.pol_trmn_dt")).alias("pol_trmn_dt"))
)

rs_b = (
    tpolicys_df.alias("p1")
    .join(
        tclient_policy_links_df.alias("l1"),
        on=[
            col("p1.pol_num") == col("l1.pol_num"),
            col("p1.image_date") == col("l1.image_date"),
        ],
        how="inner",
    )
    .where(
        (col("l1.link_typ") == "O")
        & col("p1.pol_stat_cd").isin(["A", "B", "C", "D", "E", "F", "H", "L", "M", "T"])
        & (col("p1.image_date") == last_mthend)
    )
    .select(
        col("l1.cli_num"),
        col("p1.pol_num"),
        col("p1.plan_code_base"),
        col("p1.pol_stat_cd"),
        col("p1.pol_trmn_dt"),
        col("p1.dist_chnl_cd"),
    )
)

rs_status_tbl = (
    tfield_values_df.where(
        (col("fld_nm") == "STAT_CODE")
        & (col("app_cd") == "CAS")
        & (col("image_date") == last_mthend)
    )
    .select(col("fld_valu_desc_eng"), col("fld_valu").alias("pol_stat_cd"))
)

rs_lst_chnl_desc_tbl = (
    tfield_values_df.where(
        (col("fld_nm") == "DIST_CHNL_CD")
        & (col("app_cd") == "CAS")
        & (col("image_date") == last_mthend)
    )
    .select(col("fld_valu_desc_eng"), col("fld_valu").alias("dist_chnl_cd"))
)

tmp_lst_trmn_pol_mthend = (
    rs_a.alias("a")
    .join(rs_b.alias("b"), on="cli_num", how="inner")
    .join(rs_status_tbl.alias("status_tbl"), on="pol_stat_cd", how="inner")
    .join(rs_lst_chnl_desc_tbl.alias("lst_chnl_desc_tbl"), on="dist_chnl_cd", how="inner")
    .where((col("a.pol_trmn_dt").isNotNull()) & (col("a.pol_trmn_dt") == col("b.pol_trmn_dt")))
    .select(
        col("cli_num"),
        col("pol_num"),
        col("plan_code_base"),
        col("b.pol_stat_cd"),
        col("b.pol_trmn_dt"),
        col("status_tbl.fld_valu_desc_eng").alias("status"),
        col("lst_chnl_desc_tbl.fld_valu_desc_eng").alias("lst_chnl_desc"),
    )
)

print("Number of records of tmp_lst_trmn_pol_mthend: ", tmp_lst_trmn_pol_mthend.count())
print("Number of columns of tmp_lst_trmn_pol_mthend: ", len(tmp_lst_trmn_pol_mthend.columns))
tmp_lst_trmn_pol_mthend.display(10)

# COMMAND ----------

# DBTITLE 1,Step5. Get list of last policy issued per customer
rs_a = tpolicys_df.alias("p").join(tclient_policy_links_df.alias("l"), (tpolicys_df.pol_num == tclient_policy_links_df.pol_num) & (tpolicys_df.image_date == tclient_policy_links_df.image_date)) \
    .where((tclient_policy_links_df.link_typ == 'O') & (tpolicys_df.image_date == last_mthend)) \
    .groupBy(tpolicys_df.image_date, tclient_policy_links_df.cli_num) \
    .agg(max(tpolicys_df.pol_eff_dt).alias("pol_eff_dt")) \
    .select(tpolicys_df.image_date, 
            tclient_policy_links_df.cli_num, 
            "pol_eff_dt")
#print("Number of records: ", rs_a.count())
#print("Number of columns: ", len(rs_a.columns))

rs_b = tpolicys_df.alias("p1").join(tclient_policy_links_df.alias("l1"), (tpolicys_df.pol_num == tclient_policy_links_df.pol_num) & (tpolicys_df.image_date == tclient_policy_links_df.image_date)) \
    .where((tclient_policy_links_df.link_typ == 'O') & (tpolicys_df.image_date == last_mthend)) \
    .select(tclient_policy_links_df.cli_num, 
            tpolicys_df.pol_num, 
            tpolicys_df.plan_code_base, 
            tpolicys_df.pol_stat_cd, 
            tpolicys_df.pol_eff_dt, 
            tpolicys_df.agt_code, 
            tpolicys_df.dist_chnl_cd, 
            tpolicys_df.image_date)
#print("Number of records: ", rs_b.count())
#print("Number of columns: ", len(rs_b.columns))

#print("Number of records: ", rs_status_tbl.count())
#print("Number of columns: ", len(rs_status_tbl.columns))
#rs_status_tbl.display(100)

#print("Number of records: ", rs_lst_chnl_desc_tbl.count())
#print("Number of columns: ", len(rs_lst_chnl_desc_tbl.columns))
#rs_lst_chnl_desc_tbl.display(100)

tmp_lst_inf_pol_mthend = rs_a.alias("a").join(rs_b.alias("b"), on=["cli_num", "pol_eff_dt"], how="inner") \
    .join(rs_status_tbl.alias("status_tbl"), on="pol_stat_cd", how="inner") \
    .join(rs_lst_chnl_desc_tbl.alias("lst_chnl_desc_tbl"), on="dist_chnl_cd", how="inner") \
    .where(rs_a.pol_eff_dt.isNotNull()) \
    .select(col("a.cli_num"), 
            col("b.pol_num"), 
            col("b.plan_code_base"), 
            col("b.pol_stat_cd"), 
            col("b.agt_code"), 
            col("b.pol_eff_dt"), 
            col("status_tbl.fld_valu_desc_eng").alias("status"), 
            col("lst_chnl_desc_tbl.fld_valu_desc_eng").alias("lst_chnl_desc"))
    
print("Number of records of tmp_lst_inf_pol_mthend: ", tmp_lst_inf_pol_mthend.count())
print("Number of columns of tmp_lst_inf_pol_mthend: ", len(tmp_lst_inf_pol_mthend.columns))
tmp_lst_inf_pol_mthend.display(10)

# COMMAND ----------

# DBTITLE 1,Step6. Get list of last policy issued per customer
rs_a = tpolicys_df.alias("p").join(tclient_policy_links_df.alias("l"), (tpolicys_df.pol_num == tclient_policy_links_df.pol_num) & (tpolicys_df.image_date == tclient_policy_links_df.image_date)) \
    .where((tclient_policy_links_df.link_typ == 'O') & (tpolicys_df.image_date == last_mthend)) \
    .groupBy(tpolicys_df.image_date, tclient_policy_links_df.cli_num) \
    .agg(min(tpolicys_df.pol_eff_dt).alias("pol_eff_dt")) \
    .select(tpolicys_df.image_date, 
            tclient_policy_links_df.cli_num, 
            "pol_eff_dt")
#print("Number of records: ", rs_a.count())
#print("Number of columns: ", len(rs_a.columns))
#rs_a.display(100)

tmp_frt_inf_pol_mthend = rs_a.alias("a").join(rs_b.alias("b"), on=["cli_num", "pol_eff_dt"], how="inner") \
    .join(rs_status_tbl.alias("status_tbl"), on="pol_stat_cd", how="inner") \
    .join(rs_lst_chnl_desc_tbl.alias("lst_chnl_desc_tbl"), on="dist_chnl_cd", how="inner") \
    .where(rs_a.pol_eff_dt.isNotNull()) \
    .select(col("a.cli_num"), 
            col("b.pol_num"), 
            col("b.plan_code_base"), 
            col("b.pol_stat_cd"), 
            col("b.agt_code"), 
            col("b.pol_eff_dt"), 
            col("status_tbl.fld_valu_desc_eng").alias("status"), 
            col("lst_chnl_desc_tbl.fld_valu_desc_eng").alias("lst_chnl_desc"))
    
print("Number of records of tmp_frt_inf_pol_mthend: ", tmp_frt_inf_pol_mthend.count())
print("Number of columns of tmp_frt_inf_pol_mthend: ", len(tmp_frt_inf_pol_mthend.columns))
tmp_frt_inf_pol_mthend.display(10)

# COMMAND ----------

# DBTITLE 1,Step7. Retrieve legacy CWS account & activities
# Retrieve old CWS accounts
cws_acc = tcas_cws_user_mappings_df.filter(tcas_cws_user_mappings_df["image_date"] == last_mthend)
cws_acc = cws_acc.groupBy("cli_num").agg(max("user_id").alias("user_id"), max("image_date").alias("image_date"))
cws_acc = tcas_cws_user_mappings_df.join(cws_acc, on=["cli_num", "user_id", "image_date"], how="inner")

# Retrieve old CWS login logs
cws_login = legacy_cws_df.join(user_df, legacy_cws_df["userid"] == user_df["id"], how="inner")
cws_login = cws_login.join(tcas_cws_users_df, user_df["email"] == tcas_cws_users_df["sf_email_address"], how="inner")
cws_login = cws_login.join(tcas_cws_user_mappings_df, tcas_cws_users_df["user_id"] == tcas_cws_user_mappings_df["user_id"], how="inner")
cws_login = cws_login.groupBy(tcas_cws_user_mappings_df["cli_num"]).agg(max("eventdate").alias("cws_legacy_lst_login_dt"))

# Merge to retrieve old CWS customers" info
cws_legacy_information_mthend = cws_acc.join(cws_login, on="cli_num", how="left")

print("Number of records of cws_legacy_information_mthend: ", cws_legacy_information_mthend.count())
print("Number of columns of cws_legacy_information_mthend: ", len(cws_legacy_information_mthend.columns))
cws_legacy_information_mthend.display(10)

# COMMAND ----------

# DBTITLE 1,Step8. CWS accounts & activities (new version)
cws_acc = account_df.select(
    account_df.external_id__c.alias('cli_num'),
    account_df.mcf_user_id__pc.alias('acc_id')
).where(account_df.mcf_user_id__pc.isNotNull())

cws_login_transactions = cws_hit_data_df.select(
    cws_hit_data_df.post_evar37.alias('acc_id'),
    concat(cws_hit_data_df.post_visid_high, cws_hit_data_df.post_visid_low, cws_hit_data_df.visit_num).alias('visit_id'),
    cws_hit_data_df.date_time.alias('login_date_time'),
    row_number().over(Window.partitionBy(cws_hit_data_df.post_evar37).orderBy(cws_hit_data_df.date_time.asc())).alias('rw_num')
)

cws_reg = cws_login_transactions.select(
    cws_login_transactions.acc_id,
    cws_login_transactions.login_date_time.alias('reg_dt')
).where(cws_login_transactions.rw_num == 1)

cws_login = cws_login_transactions.where(
    (cws_login_transactions.rw_num > 1) &
    (cws_login_transactions.login_date_time <= last_mthend)
).groupBy(cws_login_transactions.acc_id).agg(
    max(cws_login_transactions.login_date_time).alias('lst_login_dt')
)

cws_information_mthend = cws_acc.join(
    cws_reg,
    on='acc_id',
    how='left'
).join(
    cws_login,
    on='acc_id',
    how='left'
).select(
    cws_acc.cli_num,
    cws_reg.reg_dt.alias('cws_joint_dt'),
    cws_login.lst_login_dt
)

#print("Number of records of cws_information_mthend: ", cws_information_mthend.count())
#print("Number of columns of cws_information_mthend: ", len(cws_information_mthend.columns))
cws_information_mthend.display(10)

# COMMAND ----------

# DBTITLE 1,Step9. Retrieve MOVE activation and login information
move_acc_mthend = muser_flat_df.join(
    manulifemember_flat_df,
    on=muser_flat_df['_id'] == manulifemember_flat_df.userid,
    how='inner'
).join(
    movekey_flat_df,
    on=manulifemember_flat_df.keyid == movekey_flat_df['_id'],
    how='left'
).join(
    userstate_flat_df,
    on=muser_flat_df['_id'] == userstate_flat_df.userid,
    how='left'
).where(movekey_flat_df.activated == 1).select(
    muser_flat_df['_id'].alias('muser_id'),
    movekey_flat_df['value'].alias('movekey'),
    date_format(movekey_flat_df.activationdate, 'yyyy-MM-dd').alias('activation_date'),
    date_format(userstate_flat_df.lastdatasync, 'yyyy-MM-dd').alias('last_data_sync_date')
)

move_login_transactions = move_hit_data_df.select(
    move_hit_data_df.post_evar1.alias('muser_id'),
    date_format(move_hit_data_df.date_time, 'yyyy-MM-dd').alias('login_dt'),
    move_hit_data_df.post_pagename,
    concat(move_hit_data_df.post_visid_high, move_hit_data_df.post_visid_low).alias('visitor_id'),
    concat(move_hit_data_df.post_visid_high, move_hit_data_df.post_visid_low, move_hit_data_df.visit_num, move_hit_data_df.visit_start_time_gmt).alias('visit_id'),
    move_hit_data_df.visit_page_num,
    move_hit_data_df.visit_num,
    move_hit_data_df.date_time,
    when(move_hit_data_df.post_mobileosversion.like('%Android%'), 'Android').otherwise('iOS').alias('os'),
    row_number().over(Window.partitionBy(move_hit_data_df.post_evar1).orderBy(move_hit_data_df.date_time.asc())).alias('rw_num')
)

move_login = move_login_transactions.where(
    (move_login_transactions.rw_num != 1) &
    (move_login_transactions.login_dt <= last_mthend)
).groupBy(move_login_transactions.muser_id).agg(
    max(move_login_transactions.login_dt).alias('lst_login_dt')
)

move_info = move_acc_mthend.join(
    move_login,
    on='muser_id',
    how='left'
).select(
    when(move_acc_mthend.movekey.isNotNull(), move_acc_mthend.movekey.substr(2, int(1e9))).alias('cli_num'),
    move_acc_mthend.activation_date,
    move_login.lst_login_dt
)

rs_dis = move_info.select(
    col('cli_num'),
    col('activation_date'),
    col('lst_login_dt'),
    row_number().over(Window.partitionBy(col('cli_num')).orderBy(col('activation_date').desc(), col('lst_login_dt').desc())).alias('rw_num')
)

move_information_mthend = rs_dis.where(rs_dis.rw_num == 1)

#print("Number of records of move_information_mthend: ", move_information_mthend.count())
#print("Number of columns of move_information_mthend: ", len(move_information_mthend.columns))
move_information_mthend.display(10)

# COMMAND ----------

# DBTITLE 1,Step10. Create additional customer information tables
# Get number of dependants
rs_dpnd = tclient_policy_links_df.alias("l").join(
    tclient_policy_links_df.alias("l1"),
    on=["pol_num", "image_date"],
    how="inner"
).where(
    (col("l.link_typ") == "O") &
    (col("l1.link_typ").isin(["I", "T"])) &
    (col("l.cli_num") != col("l1.cli_num")) &
    (col("l.image_date") == last_mthend)
).groupBy(
    col("l.cli_num")
).agg(
    F.countDistinct(col("l1.cli_num")).alias("no_dpnd")
)

# Get list of unique customers with last purchased policy
rs_tmp_last = tmp_lst_inf_pol_mthend.select(
 "cli_num",
 "status",
 "lst_chnl_desc",
 "pol_eff_dt",
 "pol_num",
 "agt_code",
 "plan_code_base"
).dropDuplicates(["cli_num"])
#print("Number of records: ", rs_tmp_last.count())
#print("Number of columns: ", len(rs_tmp_last.columns))

# Get list of unique customers with first purchased policy
rs_tmp_frst = tmp_frt_inf_pol_mthend.select(
    "cli_num",
    "lst_chnl_desc"
).dropDuplicates(["cli_num"])
#print("Number of records: ", rs_tmp_frst.count())
#print("Number of columns: ", len(rs_tmp_frst.columns))

# Get list of unique customers with latest lapsed policy
rs_tmp_trmn = tmp_lst_trmn_pol_mthend.select(
    "cli_num",
    "pol_num",
    "pol_trmn_dt",
    "pol_stat_cd",
    "status"
).dropDuplicates(["cli_num"])
#print("Number of records: ", rs_tmp_trmn.count())
#print("Number of columns: ", len(rs_tmp_trmn.columns))

# Perform join and aggregation operations to get customers income information
# Income from calculation
rs_inc1 = (
    tji_config_df.join(tji_zap_qtn_df, (tji_config_df.config_id == tji_zap_qtn_df.qtn_cd) & (tji_config_df.image_date == tji_zap_qtn_df.image_date))
    .join(tji_app_info_df.alias('info'), (col("info.fld_nm") == tji_config_df.attrib_minor) & (col("info.image_date") == tji_config_df.image_date))
    .join(tji_app_info_df.alias('ans'), (col("ans.pol_num") == col("info.pol_num")) & (col("ans.app_typ") == col("info.app_typ")) & (col("ans.image_date") == col("info.image_date"))
          & (col("ans.fld_nm") == tji_zap_qtn_df.qtn_cd) & (col("ans.image_date") == tji_zap_qtn_df.image_date))
    .where((tji_config_df.config_typ == "35") & (tji_config_df.config_id.like("%0013__%")) & (col("info.app_typ") == "NB") & 
           (col("ans.fcn_typ") == "Q") & col("ans.fld_valu").isNotNull() & (tji_config_df.image_date == last_mthend))
    .groupBy(col("info.fld_valu").alias("cli_num"))
    .agg(max(col("ans.fld_valu").cast("int")).alias("mthly_incm"))
)
#rs_inc1.display(10)
# Income from input form
rs_inc2 = (
    tclient_other_details_df.where((tclient_other_details_df.mthly_incm.isNotNull()) & (tclient_other_details_df.mthly_incm != 0) &
                           (tclient_other_details_df.image_date == last_mthend))
    .groupBy(tclient_other_details_df.cli_num)
    .agg(max(tclient_other_details_df.mthly_incm.cast("int")).alias("mthly_incm"))
)

# Combine income from two sources
rs_tinc = (
    rs_inc1.union(rs_inc2)
)#.groupBy("cli_num").agg(sum("mthly_incm"))    # need to debug (unsupported operand type(s) for +: 'int' and 'str')
rs_tinc = rs_tinc.groupBy("cli_num").agg(max("mthly_incm").alias("mthly_incm"))
#print("Number of records of rs_tinc: ", rs_tinc.count())

# Generate rs_cust_sum
# Filter rows where link_typ is in ("O', 'I', 'T')
filtered_tclient_policy_links_df = tclient_policy_links_df[tclient_policy_links_df["link_typ"].isin(["O", "I", "T"])]

# Merge the two dataframes on image_date and pol_num
merged_tpolicys_df = tpolicys_df.join( 
                     filtered_tclient_policy_links_df, 
                     on=["image_date", "pol_num"], 
                     how="inner")

# Group by cli_num and aggregate
rs_cust_sum = merged_tpolicys_df.groupBy("cli_num").agg(
    F.concat_ws(",", F.collect_set("link_typ")).alias("link_types"),
    F.coalesce(F.min("frst_iss_dt"), F.min("pol_iss_dt")).alias("frst_iss_dt"),
    F.min("pol_eff_dt").alias("frst_eff_dt"),
    F.max("pol_eff_dt").alias("lst_eff_dt"),
    F.max("restrict_cd_3").alias("restrict_cd_3")
)
print("Number of records of rs_cust_sum: ", rs_cust_sum.count())
print("Number of columns of rs_cust_sum: ", len(rs_cust_sum.columns))
rs_cust_sum.display(0)

# COMMAND ----------

# DBTITLE 1,Step11. Finalize (join all tables together)
unique_tclient_policy_links_df = filtered_tclient_policy_links_df.select("cli_num").dropDuplicates()

tcustdm_mthend = tclient_details_df.alias("tcd") \
    .join(rs_cust_sum.alias("cs"), col("tcd.cli_num") == col("cs.cli_num"), "inner") \
    .join(unique_tclient_policy_links_df.alias("tcpl"), col("tcpl.cli_num") == col("tcd.cli_num"), "inner") \
    .join(cws_legacy_information_mthend.alias("cws_leg"), col("tcd.cli_num") == col("cws_leg.cli_num"), "left_outer") \
    .join(tclient_other_details_df.alias("tcod"), (col("tcod.cli_num") == col("tcd.cli_num")) & (col("tcod.image_date") == col("tcd.image_date")), "left_outer") \
    .join(tclient_service_registrations_df.alias("tsr"), (col("tsr.cli_num") == col("tcd.cli_num")) & (col("tsr.image_date") == col("tcd.image_date")), "left_outer") \
    .join(tfield_values_df.alias("tsr_v"), (col("tsr.method_recv") == col("tsr_v.fld_valu").substr(2, 1)) & (col("tsr.image_date") == col("tsr_v.image_date")) & (col("tsr_v.fld_nm") == "METHOD_RECV"), "left_outer") \
    .join(toccupation_df.alias("tocc"), (col("tocc.occp_code") == col("tcd.occp_code")) & (col("tocc.image_date") == col("tcd.image_date")), "left_outer") \
    .join(tsms_clients_df.alias("tsms"), (col("tsms.cli_num") == col("tcd.cli_num")) & (col("tsms.cli_typ") == "C"), "left_outer") \
    .join(tfield_values_df.alias("tsms_v"), (col("tsms.stat_cd") == col("tsms_v.fld_valu")) & (col("tsms_v.fld_nm") == "SMS_REASN_CD") & (col("tsms_v.image_date") == last_mthend), "left_outer") \
    .join(tmp_cus_add_ful_mthend.alias("fadr"), col("fadr.cli_num") == col("tcd.cli_num"), "left_outer") \
    .join(rs_tinc.alias("tinc"), col("tinc.cli_num") == col("tcd.cli_num"), "left_outer") \
    .join(rs_dpnd.alias("dpnd"), col("dpnd.cli_num") == col("tcd.cli_num"), "left_outer") \
    .join(rs_tmp_last.alias("tmp_last"), col("tmp_last.cli_num") == col("tcd.cli_num"), "left_outer") \
    .join(rs_tmp_frst.alias("tmp_frst"), col("tmp_frst.cli_num") == col("tcd.cli_num"), "left_outer") \
    .join(rs_tmp_trmn.alias("tmp_trmn"), col("tmp_trmn.cli_num" )== col("tcd.cli_num" ), "left_outer" )\
    .join(cws_information_mthend.alias("cws" ), col("cws.cli_num" )== col("tcd.cli_num" ), "left_outer" )\
    .join(move_information_mthend.alias( "move" ), col( "move.cli_num" )== col( "tcd.cli_num" ), "left_outer" )\
    .where(col("tcd.image_date") == last_mthend)\
    .select(
        "tcd.cli_num",
        "tcd.cli_nm",
        "tcd.birth_dt",
        F.floor(F.months_between(current_date(), "tcd.birth_dt" )/ 12).alias( "cur_age"),
        F.floor(F.months_between(col( "cs.frst_iss_dt" ), "tcd.birth_dt" )/ 12).alias( "frst_iss_age"),
        when(col( "cs.frst_iss_dt" ).isNotNull(), col("cs.frst_iss_dt")).otherwise(col( "cs.frst_eff_dt")).alias( "frst_join_dt"),
        F.floor(F.months_between(current_date(), "cs.frst_eff_dt") / 12).alias("no_vin_yr"),
        "tcd.sex_code",
        when(col("cs.restrict_cd_3").isNotNull(), "Y").otherwise("N").alias("blklst_ind"),
        when(col("tcd.sex_code") == "G", "Company").otherwise("Individual").alias("CLI_TYP"),
        when(F.floor(F.months_between(col("cs.lst_eff_dt"), current_date()) / 12) <= 3, "A").otherwise("N").alias("actvnes_stat"),
        when(F.months_between(current_date(), when(col("cs.frst_iss_dt").isNotNull(), col("cs.frst_iss_dt")).otherwise(col("cs.frst_eff_dt"))) < 3, "N").otherwise("E").alias("new_exist_stat"),
        "tcd.nat_code",
        "tcd.id_typ",
        "tcd.id_num",
        "tcd.id_iss_dt",
        "tcd.id_iss_plc",
        "tcd.mobl_phon_num",
        "tcd.prim_phon_num",
        "tcd.othr_phon_num",
        col("fadr.addr_typ").cast("int").alias("addr_typ"),
        "fadr.full_address",
        col("cws_leg.user_id").alias("cws_acct_id"),
        col("cws_leg.create_date").alias("cws_acct_join_dt"),
        col("cws_leg.stat_cd").alias("cws_acct_stat_cd"),
        "tcod.email_addr",
        col("tcpl.cli_num").alias("linked_cli_num"),
        "cs.link_types",
        col("tsr.user_id").alias("service_user_id"),
        col("tsr.stat_cd").alias("service_user_stat_cd"),
        col("tsr.cli_typ").alias("service_user_typ"),
        col("tsr.chng_dt").alias("service_reg_dt"),
        col("tsr.method_recv").alias("service_method_recv"),
        col("tsr_v.fld_valu_desc").alias("service_method_recv_desc"),
        "tocc.occp_code",
        "tocc.occp_desc",
        "tocc.occp_clas",
        when(col("tsms.cli_num").isNull(), "N").otherwise("Y").alias("sms_ind"),
        col("tsms.stat_cd").alias("sms_stat_cd"),
        col("tsms_v.fld_valu_desc").alias("sms_stat_cd_desc"),
        col("tsms.synchn_ind").alias("sms_eservice_ind"),
        "fadr.prov_id",
        "fadr.dist_id",
        "fadr.ward_id",
        "fadr.city",
        "fadr.district",
        "fadr.ward",
        "tinc.mthly_incm",
        "dpnd.no_dpnd",
        col("tmp_frst.lst_chnl_desc").alias("frst_chnl_desc"),
        "tmp_last.lst_chnl_desc",
        col("tmp_last.pol_num").alias("lst_eff_pol"),
        col("tmp_last.pol_eff_dt").alias("lst_eff_dt"),
        col("tmp_trmn.pol_num").alias("lst_trmn_pol"),
        col("tmp_trmn.pol_trmn_dt").alias("lst_trmn_dt"),
        col("tmp_trmn.pol_stat_cd").alias("lst_trmn_stat_cd"),
        col("tmp_trmn.status").alias("lst_trmn_sts"),
        col("cws_leg.cws_legacy_lst_login_dt").alias("cws_lst_login_dt_legacy"),
        when(col("cws.cli_num").isNotNull(), "Y").otherwise("N").alias("cws_ind"),
        "cws.cws_joint_dt",
        col("cws.lst_login_dt").alias("cws_lst_login_dt"),
        when(col("move.cli_num").isNotNull(), "Y").otherwise("N").alias("move_ind"),
        col("move.activation_date").alias("move_joint_dt"),
        col("move.lst_login_dt").alias("move_lst_login_dt"),
        F.lit(last_mthend).alias("image_date")
    )

#print("Number of records of tcustdm_mthend: ", tcustdm_mthend.count())
#print("Number of columns of tcustdm_mthend: ", len(tcustdm_mthend.columns))
tcustdm_mthend.display(100)

# COMMAND ----------

tcustdm_mthend.write.mode("overwrite").partitionBy("image_date").parquet("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/cpm/datamarts/TCUSTDM_MTHEND")

# COMMAND ----------


