# Databricks notebook source
# MAGIC %run /Curation/Utilities/01_Initialize

# COMMAND ----------

# MAGIC %run /Curation/Utilities/02_Functions

# COMMAND ----------

spark.conf.set("spark.sql.session.timeZone","UTC+8")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

control_sch_list = ['VN_PUBLISHED_CICS_DB']
control_tbl_list = ['TSMS_CLIENTS']


#control_sch_list_2 =['VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB']
#control_tbl_list_2 = ['TCLIENT_POLICY_LINKS','TCLIENT_ADDRESSES','TPROVINCES','TDISTRICTS','TWARDS','TPOLICYS','TFIELD_VALUES','TCLIENT_DETAILS','TCAS_CWS_USER_MAPPINGS','TCLIENT_OTHER_DETAILS','TCLIENT_SERVICE_REGISTRATIONS','TOCCUPATION','TJI_CONFIG','TJI_ZAP_QTN','TJI_APP_INFO']

aldsg2_path = 'Published/VN/Master'
file_format = 'PARQUET'

# COMMAND ----------

#create external table with no partition 
create_external_table_tool(container_name,storage_account,control_sch_list,control_tbl_list,aldsg2_path,file_format)

# COMMAND ----------

#create view due to cluster issue
tclient_policy_links_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_POLICY_LINKS")

tclient_addresses_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_ADDRESSES")

tprovinces_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TPROVINCES")

tdistricts_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TDISTRICTS")

twards_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TWARDS")

tpolicys_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TPOLICYS")

tfield_values_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TFIELD_VALUES")

tclient_details_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_DETAILS")

tcas_cws_user_mappings_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCAS_CWS_USER_MAPPINGS")

tclient_other_details_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_OTHER_DETAILS")

tclient_service_registrations_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TCLIENT_SERVICE_REGISTRATIONS")

toccupation_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TOCCUPATION")

tji_config_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TJI_CONFIG")

tji_zap_qtn_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TJI_ZAP_QTN")

tji_app_info_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CASM_CAS_SNAPSHOT_DB/TJI_APP_INFO")

#RITM06759348
from pyspark.sql.functions import trunc, date_add, current_date, date_format, concat, concat_ws, row_number, col, max, min, monotonically_increasing_id, when
from pyspark.sql.types import StructType, StructField, StringType, DateType
# Prepare tables needed for cws_legacy_information_mthend
user_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/USER/")
tcas_cws_users_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_CAS_DB/TCAS_CWS_USERS/")

legacy_cws_schema = StructType([
    StructField("userid", StringType(), True),
    StructField("createddate", DateType(), True),
    StructField("eventdate", DateType(), True),
    StructField("reporting_date", StringType(), True)
])
legacy_cws_df = spark.read.format("csv").schema(legacy_cws_schema).option("header", "true").option("inferSchema", "true").option("mode", "DROPMALFORMED").load("abfss://lab@abcmfcadovnedl01psea.dfs.core.windows.net/vn/project/scratch/legacy_cws.csv")

# Prepare tables needed for cws_information_mthend
account_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_SFDC_EASYCLAIMS_DB/ACCOUNT/")
cws_hit_data_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_ADOBE_PWS_DB/HIT_DATA/").select("post_evar37", "post_visid_high", "post_visid_low", "visit_num", "date_time", "exclude_hit", "hit_source", "post_evar19", "user_server")

# Prepare tables needed for move_information_mthend
movekey_flat_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MOVEKEY_FLAT/")
manulifemember_flat_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MANULIFEMEMBER_FLAT/")
muser_flat_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/MUSER_FLAT/")
userstate_flat_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_MOVE5_MONGO_DB/USERSTATE_FLAT/")
move_hit_data_df = spark.read.parquet("abfss://prod@abcmfcadovnedl01psea.dfs.core.windows.net/Published/VN/Master/VN_PUBLISHED_ADOBE_MOVE5_DB/HIT_DATA/").select("post_evar1", "date_time", "post_pagename", "post_visid_low", "post_visid_high",
"visit_start_time_gmt", "visit_page_num", "visit_num", "post_mobileosversion", "exclude_hit", "hit_source")

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

# COMMAND ----------

tclient_policy_links_df.createOrReplaceTempView('tclient_policy_links')
tclient_addresses_df.createOrReplaceTempView('tclient_addresses')
tprovinces_df.createOrReplaceTempView('tprovinces')
tdistricts_df.createOrReplaceTempView('tdistricts')
twards_df.createOrReplaceTempView('twards')
tpolicys_df.createOrReplaceTempView('tpolicys')
tfield_values_df.createOrReplaceTempView('tfield_values')
tclient_details_df.createOrReplaceTempView('tclient_details')
tcas_cws_user_mappings_df.createOrReplaceTempView('tcas_cws_user_mappings')
tclient_other_details_df.createOrReplaceTempView('tclient_other_details')
tclient_service_registrations_df.createOrReplaceTempView('tclient_service_registrations')
toccupation_df.createOrReplaceTempView('toccupation')
tji_config_df.createOrReplaceTempView('tji_config')
tji_zap_qtn_df.createOrReplaceTempView('tji_zap_qtn')
tji_app_info_df.createOrReplaceTempView('tji_app_info')
#RITM06759348
user_df.createOrReplaceTempView('user')
tcas_cws_users_df.createOrReplaceTempView('tcas_cws_users')
legacy_cws_df.createOrReplaceTempView('legacy_cws')
account_df.createOrReplaceTempView('account')
cws_hit_data_df.createOrReplaceTempView('cws_hit_data')
movekey_flat_df.createOrReplaceTempView('movekey_flat')
manulifemember_flat_df.createOrReplaceTempView('manulifemember_flat')
muser_flat_df.createOrReplaceTempView('muser_flat')
userstate_flat_df.createOrReplaceTempView('userstate_flat')
move_hit_data_df.createOrReplaceTempView('move_hit_data')

# COMMAND ----------

spark.sql("""
CREATE or replace temp view 
        TMP_CUS_ADD_FUL_MTHEND AS
SELECT
        a.cli_num      ,
        a.addr_1       ,
        a.addr_2       ,
        a.addr_3       ,
        a.addr_4       ,
        a.full_address ,
        a.zip_code     ,
        a.addr_typ     ,
        a.prov_id      ,
        a.city         ,
        a.dist_id      ,
        a.district     ,
        a.ward_id      ,
        a.ward
FROM
        (
                SELECT
                        adr.cli_num                                                                 ,
                        adr.addr_1                                                                  ,
                        adr.addr_2                                                                  ,
                        adr.addr_3                                                                  ,
                        adr.addr_4                                                                  ,
                        concat_ws(', ',adr.addr_1,adr.addr_2,adr.addr_3,adr.addr_4) AS FULL_ADDRESS ,
                        adr.zip_code                                                                ,
                        adr.addr_typ                                                                ,
                        pro.prov_id                                                                 ,
                        pro.prov_nm AS CITY                                                         ,
                        dis.dist_id                                                                 ,
                        dis.dist_nm AS DISTRICT                                                     ,
                        wrd.ward_id                                                                 ,
                        wrd.ward_nm AS WARD                                                         ,
                        row_number() over (PARTITION BY
                                           adr.cli_num ORDER BY
                                           adr.addr_typ DESC) AS rn
                FROM
                        TCLIENT_POLICY_LINKS cpl
                INNER JOIN
                        tclient_addresses adr
                ON
                        (
                                cpl.cli_num    = adr.cli_num
                        AND     cpl.addr_typ   = adr.addr_typ
                        AND     cpl.image_date = adr.image_date)
                LEFT JOIN
                        tprovinces pro
                ON
                        (
                                SUBSTR(adr.zip_code,1,2) = pro.prov_id
                        AND     adr.image_date           = pro.image_date)
                LEFT JOIN
                        tdistricts dis
                ON
                        (
                                SUBSTR(adr.zip_code,3,2) = dis.dist_id
                        AND     pro.prov_id              = dis.prov_id
                        AND     pro.image_date           = dis.image_date)
                LEFT JOIN
                        twards wrd
                ON
                        (
                                SUBSTR(adr.zip_code,5,2) = wrd.ward_id
                        AND     dis.prov_id              = wrd.prov_id
                        AND     dis.dist_id              = wrd.dist_id
                        AND     dis.image_date           = wrd.image_date)
                WHERE
                        cpl.rec_status = 'A'
                AND     cpl.link_typ IN ('O',
                                         'I',
                                         'T')
                GROUP BY
                        adr.cli_num ,
                        addr_1      ,
                        addr_2      ,
                        addr_3      ,
                        addr_4      ,
                        zip_code    ,
                        adr.addr_typ,
                        pro.prov_id ,
                        pro.prov_nm ,
                        dis.dist_id ,
                        dis.dist_nm ,
                        wrd.ward_id ,
                        wrd.ward_nm
                ORDER BY
                        adr.cli_num,
                        adr.addr_typ DESC ) a
WHERE
        rn = 1""")

spark.sql("""CREATE or replace temp view 
        TMP_LST_TRMN_POL_MTHEND AS
SELECT
        a.cli_num                              ,
        b.pol_num                              ,
        b.plan_code_base                       ,
        b.pol_stat_cd                          ,
        b.pol_trmn_dt                          ,
        status_tbl.fld_valu_desc_eng        AS STATUS ,
        LST_CHNL_DESC_TBL.fld_valu_desc_eng    LST_CHNL_DESC
FROM
        (
                SELECT
                        l1.cli_num        ,
                        p1.pol_num        ,
                        p1.plan_code_base ,
                        p1.pol_stat_cd    ,
                        p1.pol_trmn_dt    ,
                        p1.dist_chnl_cd
                FROM
                        Tpolicys p1 INNER
                JOIN
                        Tclient_policy_links l1
                ON
                        (
                                p1.pol_num    = l1.pol_num
                        AND     p1.image_date = l1.image_date)
                WHERE
                        l1.link_typ ='O'
                AND     p1.pol_stat_cd IN ('A',
                                           'B',
                                           'C',
                                           'D',
                                           'E',
                                           'F',
                                           'H',
                                           'L',
                                           'M',
                                           'T')
                AND     p1.image_date = last_day(add_months(CURRENT_DATE,-1)) ) b
INNER JOIN
        (
                SELECT
                        l.cli_num ,
                        MAX(p.pol_trmn_dt) pol_trmn_dt
                FROM
                        Tpolicys p
                INNER JOIN
                        Tclient_policy_links l
                ON
                        (
                                p.pol_num    = l.pol_num
                        AND     p.image_date = l.image_date)
                WHERE
                        l.link_typ ='O'
                AND     p.pol_stat_cd IN ('A',
                                          'B',
                                          'C',
                                          'D',
                                          'E',
                                          'F',
                                          'H',
                                          'L',
                                          'M',
                                          'T')
                AND     p.image_date = last_day(add_months(CURRENT_DATE,-1))
                GROUP BY
                        l.cli_num ) a
ON
        (
                a.cli_num = b.cli_num)
LEFT OUTER JOIN
        (
                SELECT
                        fld_valu_desc_eng ,
                        fld_valu
                FROM
                        Tfield_values
                WHERE
                        fld_nm     = 'STAT_CODE'
                AND     app_cd     = 'CAS'
                AND     image_date = last_day(add_months(CURRENT_DATE,-1)) ) status_tbl
ON
        (
                b.pol_stat_cd = status_tbl.fld_valu)
LEFT OUTER JOIN
        (
                SELECT
                        fld_valu_desc_eng ,
                        fld_valu
                FROM
                        Tfield_values
                WHERE
                        fld_nm     = 'DIST_CHNL_CD'
                AND     app_cd     = 'CAS'
                AND     image_date = last_day(add_months(CURRENT_DATE,-1)) ) LST_CHNL_DESC_TBL
ON
        (
                b.dist_chnl_cd = LST_CHNL_DESC_TBL.fld_valu)
WHERE
        a.pol_trmn_dt IS NOT NULL
AND     a.pol_trmn_dt           = b.pol_trmn_dt""")


spark.sql(""" CREATE or replace temp view 
        TMP_LST_INF_POL_MTHEND AS 
SELECT
        A.CLI_NUM                              ,
        B.POL_NUM                              ,
        B.PLAN_CODE_BASE                       ,
        B.POL_STAT_CD                          ,
        B.AGT_CODE                             ,
        B.POL_EFF_DT                           ,
        status_tbl.fld_valu_desc_eng        AS STATUS ,
        LST_CHNL_DESC_TBL.fld_valu_desc_eng AS LST_CHNL_DESC
FROM
        (
                SELECT
                        L1.CLI_NUM        ,
                        P1.POL_NUM        ,
                        P1.PLAN_CODE_BASE ,
                        P1.POL_STAT_CD    ,
                        P1.POL_EFF_DT     ,
                        P1.AGT_CODE       ,
                        P1.DIST_CHNL_CD
                FROM
                        TPOLICYS P1
                INNER JOIN
                        TCLIENT_POLICY_LINKS L1
                ON
                        (
                                P1.POL_NUM    = L1.POL_NUM
                        AND     P1.IMAGE_DATE = L1.IMAGE_DATE)
                WHERE
                        L1.LINK_TYP   ='O'
                AND     P1.IMAGE_DATE = LAST_DAY(ADD_MONTHS(CURRENT_DATE,-1)) ) B
INNER JOIN
        (
                SELECT
                        L.CLI_NUM ,
                        MAX(P.POL_EFF_DT) AS POL_EFF_DT
                FROM
                        TPOLICYS P
                INNER JOIN
                        TCLIENT_POLICY_LINKS L
                ON
                        (
                                P.POL_NUM    = L.POL_NUM
                        AND     P.IMAGE_DATE = L.IMAGE_DATE)
                WHERE
                        L.LINK_TYP   ='O'
                AND     P.IMAGE_DATE = LAST_DAY(ADD_MONTHS(CURRENT_DATE,-1))
                GROUP BY
                        L.CLI_NUM ) A
ON
        (
                A.CLI_NUM = B.CLI_NUM)
LEFT OUTER JOIN
        (
                SELECT
                        fld_valu_desc_eng ,
                        fld_valu
                FROM
                        TFIELD_VALUES
                WHERE
                        fld_nm     = 'STAT_CODE'
                AND     app_cd     = 'CAS'
                AND     image_date = last_day(add_months(CURRENT_DATE,-1)) ) status_tbl
ON
        (
                b.pol_stat_cd = status_tbl.fld_valu)
LEFT OUTER JOIN
        (
                SELECT
                        fld_valu_desc_eng ,
                        fld_valu
                FROM
                        TFIELD_VALUES
                WHERE
                        fld_nm     = 'DIST_CHNL_CD'
                AND     app_cd     = 'CAS'
                AND     image_date = last_day(add_months(CURRENT_DATE,-1)) ) LST_CHNL_DESC_TBL
ON
        (
                b.dist_chnl_cd = LST_CHNL_DESC_TBL.fld_valu)
WHERE
        a.pol_eff_dt IS NOT NULL
AND     a.pol_eff_dt           = b.pol_eff_dt""")

spark.sql("""CREATE or replace temp view
        TMP_FRT_INF_POL_MTHEND AS
SELECT
        a.cli_num                              ,
        b.pol_num                              ,
        b.plan_code_base                       ,
        b.pol_stat_cd                          ,
        b.agt_code                             ,
        b.pol_eff_dt                           ,
        status_tbl.fld_valu_desc_eng        AS STATUS ,
        LST_CHNL_DESC_TBL.fld_valu_desc_eng AS LST_CHNL_DESC
FROM
        (
                SELECT
                        l1.cli_num        ,
                        p1.pol_num        ,
                        p1.plan_code_base ,
                        p1.pol_stat_cd    ,
                        p1.pol_eff_dt     ,
                        p1.agt_code       ,
                        p1.dist_chnl_cd
                FROM
                        Tpolicys p1
                INNER JOIN
                        Tclient_policy_links l1
                ON
                        (
                                p1.pol_num = l1.pol_num)
                WHERE
                        l1.link_typ   ='O'
                AND     p1.image_date = last_day(add_months(CURRENT_DATE,-1)) ) b
INNER JOIN
        (
                SELECT
                        l.cli_num ,
                        MIN(p.pol_eff_dt) pol_eff_dt
                FROM
                        Tpolicys p
                INNER JOIN
                        Tclient_policy_links l
                ON
                        (
                                p.pol_num = l.pol_num)
                WHERE
                        l.link_typ   ='O'
                AND     p.image_date = last_day(add_months(CURRENT_DATE,-1))
                GROUP BY
                        l.cli_num ) a
ON
        (
                a.cli_num = b.cli_num)
LEFT OUTER JOIN
        (
                SELECT
                        fld_valu_desc_eng ,
                        fld_valu
                FROM
                        Tfield_values
                WHERE
                        fld_nm     = 'STAT_CODE'
                AND     app_cd     = 'CAS'
                AND     image_date = last_day(add_months(CURRENT_DATE,-1)) ) status_tbl
ON
        (
                b.pol_stat_cd = status_tbl.fld_valu)
LEFT OUTER JOIN
        (
                SELECT
                        fld_valu_desc_eng ,
                        fld_valu
                FROM
                        Tfield_values
                WHERE
                        fld_nm     = 'DIST_CHNL_CD'
                AND     app_cd     = 'CAS'
                AND     image_date = last_day(add_months(CURRENT_DATE,-1)) ) LST_CHNL_DESC_TBL
ON
        (
                b.dist_chnl_cd = LST_CHNL_DESC_TBL.fld_valu)
WHERE
        a.pol_eff_dt IS NOT NULL
AND     a.pol_eff_dt           = b.pol_eff_dt""")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start RITM06759348

# COMMAND ----------

spark.sql("""CREATE or replace temp view cws_legacy_information_mthend as
-- account
with cws_acc as (
	select
		a.cli_num
		,a.user_id
		,a.stat_cd
		,a.create_date
		,a.image_date
	from
		tcas_cws_user_mappings a
		inner join (
			select
				cli_num
				,max(user_id) user_id
				,max(image_date) image_date
			from
				tcas_cws_user_mappings
			where
				image_date = last_day(add_months(current_date,-1))
			group by
				cli_num
		) b on  (a.cli_num=b.cli_num and a.user_id=b.user_id and a.image_date=b.image_date)
	where
		a.image_date = last_day(add_months(current_date,-1))
)
,cws_login as (
	select
		cwsum.cli_num
		,max(log.eventdate) cws_legacy_lst_login_dt
	from
		legacy_cws log
		inner join `user` us on (log.userid = us.id)
		inner join tcas_cws_users cwsu on (us.`email` = cwsu.sf_email_address)
		inner join tcas_cws_user_mappings cwsum on (cwsu.user_id = cwsum.user_id)
	group by
		cwsum.cli_num
)
,cws_info as (
	select
		a.cli_num
		,a.user_id
		,a.create_date
		,a.stat_cd
		,b.cws_legacy_lst_login_dt
	from
		cws_acc a
		left join cws_login b on (a.cli_num = b.cli_num)
)
select * from cws_info          
""")

# COMMAND ----------

spark.sql("""CREATE or replace temp view cws_information_mthend as
with cws_acc as (
	select
		external_id__c cli_num
		,mcf_user_id__pc acc_id
	from
		`account`
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

# COMMAND ----------

spark.sql("""CREATE or replace temp view move_acc_mthend as
select
	mu.`_id` muser_id
	,mk.`value` movekey
	,to_date(mk.activationdate) activation_date
	,to_date(urt.lastdatasync) last_data_sync_date
from
	muser_flat mu
	inner join manulifemember_flat me on (mu.`_id` = me.userid)
	left join movekey_flat mk on (me.keyid = mk.`_id`)
	left join userstate_flat urt on (mu.`_id` = urt.userid)
where
	mk.activated = 1
""")

spark.sql("""CREATE or replace temp view move_information_mthend_tmp_mthend as
with move_login_transactions as (
	select
		post_evar1 muser_id
		,from_unixtime(unix_timestamp(date_time,"yyyy-MM-dd")) login_dt
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
		move_hit_data
	where
		exclude_hit = 0
		and hit_source not in ('5', '7', '8', '9')
		and concat(post_visid_high, post_visid_low) is not null
)
,move_login as (
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
)
,move_info as (
	select
		substr(a.movekey,2,length(a.movekey)-1) cli_num
		,a.activation_date
		,b.lst_login_dt
	from
		move_acc_mthend a
		left join move_login b on (a.muser_id = b.muser_id)
)select * from move_info
""")

spark.sql("""CREATE or replace temp view move_information_mthend as
with rs_dis as (
	select
		cli_num
		,activation_date
		,lst_login_dt
		,row_number() over(partition by cli_num order by activation_date desc,lst_login_dt desc) rw_num
	from
		move_information_mthend_tmp_mthend
)
select * from rs_dis where rw_num = 1
""")

# COMMAND ----------

spark.sql("""CREATE or replace temp view TCUSTDM_MTHEND as
with rs_dpnd as (
	select
		l.cli_num
		,count(distinct l1.cli_num) no_dpnd
	from
		tclient_policy_links l
		inner join tclient_policy_links l1 on (l.pol_num = l1.pol_num and l.image_date = l1.image_date)
	where
		l.link_typ ='O'
		and l1.link_typ in ('I','T')
		and l1.cli_num <> l.cli_num
		and l.image_date = last_day(add_months(current_date,-1))
	group by
		l.cli_num
)
,rs_tmp_last as (
	select
		x.*
	from
		(
			select
				cli_num
				,status
				,lst_chnl_desc
				,pol_eff_dt
				,pol_num
				,agt_code
				,plan_code_base
				,row_number() over (partition by cli_num order by pol_eff_dt desc) as row_val
			from
				tmp_lst_inf_pol_mthend
		) x
	where x.row_val = 1
)
,rs_tmp_frst as (
	select
		y.*
	from
		(
			select
				cli_num
				,lst_chnl_desc
				,row_number() over (partition by cli_num order by pol_eff_dt) as row_val
			from
				tmp_frt_inf_pol_mthend
		) y
	where y.row_val = 1
)
,rs_tmp_trmn as (
	select
		z.*
	from
		(
			select
				cli_num
				,pol_num
				,pol_trmn_dt
				,pol_stat_cd
				,status
				,row_number() over (partition by cli_num order by pol_trmn_dt desc) as row_val
			from
				tmp_lst_trmn_pol_mthend
		) z
	where
		z.row_val = 1
)
,rs_tinc as (
	select
		a.cli_num,
		sum(a.mthly_incm) as mthly_incm
	from
		(
			select
				info.fld_valu as cli_num
				,max(nvl(cast(ans.fld_valu as int),0)) as mthly_incm
			from 
				tji_config config
				inner join tji_zap_qtn qtn on (config_id=qtn.qtn_cd and config.image_date=qtn.image_date)
				inner join tji_app_info info on (info.fld_nm=config.attrib_minor and info.image_date=config.image_date)
				inner join tji_app_info ans on (ans.pol_num=info.pol_num and ans.app_typ=info.app_typ and ans.image_date=info.image_date
																					and ans.fld_nm=qtn.qtn_cd and ans.image_date=qtn.image_date)
			where
				config_typ='35'
				and config_id like '0013__'
				and info.app_typ='NB'
				and ans.fcn_typ='Q'
				and ans.fld_valu is not null
				and config.image_date = last_day(add_months(current_date,-1))
			group by
				info.fld_valu
			union
			select
				cli_num
				,max(mthly_incm) as mthly_incm
			from
				tclient_other_details
			where
				mthly_incm not in (null,0)
				and image_date = last_day(add_months(current_date,-1))
			group by
				cli_num
		) a
	group by
		a.cli_num
)
,rs_cust_sum as (
	select
		cpl.cli_num
		,concat_ws(',',collect_set(cpl.link_typ)) as link_types
		,nvl(min(pol.frst_iss_dt),min(pol.pol_iss_dt)) frst_iss_dt
		,min(pol.pol_eff_dt) frst_eff_dt
		,max(pol.pol_eff_dt) lst_eff_dt
		,max(pol.restrict_cd_3) restrict_cd_3
	from
		tpolicys pol
		inner join tclient_policy_links cpl on (pol.image_date = cpl.image_date and pol.pol_num = cpl.pol_num)
	where
		link_typ in ('O','I','T')
		and pol.image_date = last_day(add_months(current_date,-1))
	group by
		cpl.cli_num
)
select
	tcd.cli_num
	,tcd.cli_nm
	,tcd.birth_dt
	,floor(months_between(current_date,tcd.birth_dt) / 12) as cur_age
	,floor(months_between(cs.frst_iss_dt,tcd.birth_dt) / 12) as frst_iss_age
	,nvl(cs.frst_iss_dt,cs.frst_eff_dt) as frst_join_dt
	,floor(months_between(current_date,cs.frst_eff_dt) /12) as no_vin_yr
	,tcd.sex_code
	,case when cs.restrict_cd_3 is not null then 'Y' 	else 'N' end as blklst_ind
	,case when tcd.sex_code='G' then 'Company' else 'Individual' end as CLI_TYP
	,case when floor(months_between(cs.lst_eff_dt,current_date) /12) <= 3 then 'A' else 'N' end as actvnes_stat
	,case when months_between(current_date,nvl(cs.frst_iss_dt,cs.frst_eff_dt)) < 3 then 'N' else 'E' end as new_exist_stat
	,tcd.nat_code
	,tcd.id_typ
	,tcd.id_num
	,tcd.id_iss_dt
	,tcd.id_iss_plc
	,tcd.mobl_phon_num
	,tcd.prim_phon_num
	,tcd.othr_phon_num
	,fadr.addr_typ
	,fadr.full_address
	,cws_leg.user_id as cws_acct_id
	,cws_leg.create_date cws_acct_join_dt
	,cws_leg.stat_cd as cws_acct_stat_cd
	,tcod.email_addr
	,tcpl.cli_num as linked_cli_num
	,cs.link_types
	,tsr.user_id service_user_id
	,tsr.stat_cd as service_user_stat_cd
	,tsr.cli_typ as service_user_typ
	,tsr.chng_dt as service_reg_dt
	,tsr.method_recv service_method_recv
	,tsr_v.fld_valu_desc as service_method_recv_desc
	,tocc.occp_code
	,tocc.occp_desc
	,tocc.occp_clas
	,case when tsms.cli_num is null then 'N' else 'Y' end as sms_ind
	,tsms.stat_cd as sms_stat_cd
	,tsms_v.fld_valu_desc as sms_stat_cd_desc
	,tsms.synchn_ind as sms_eservice_ind
	,fadr.prov_id
	,fadr.dist_id
	,fadr.ward_id
	,fadr.city
	,fadr.district
	,fadr.ward
	,tinc.mthly_incm
	,dpnd.no_dpnd
	,tmp_frst.lst_chnl_desc as frst_chnl_desc
	,tmp_last.lst_chnl_desc
	,tmp_last.pol_num as lst_eff_pol
	,tmp_last.pol_eff_dt as lst_eff_dt
	,tmp_trmn.pol_num as lst_trmn_pol
	,tmp_trmn.pol_trmn_dt as lst_trmn_dt
	,tmp_trmn.pol_stat_cd lst_trmn_stat_cd
	,tmp_trmn.status lst_trmn_sts
	,cws_leg.cws_legacy_lst_login_dt cws_lst_login_dt_legacy
	,case when cws.cli_num is not null then 'Y' else 'N' end cws_ind
	,cws.cws_joint_dt
	,cws.lst_login_dt cws_lst_login_dt
	,case when move.cli_num is not null then 'Y' else 'N' end move_ind
	,move.activation_date move_joint_dt
	,move.lst_login_dt move_lst_login_dt
	,last_day(add_months(current_date,-1)) image_date
from
	tclient_details tcd
	inner join rs_cust_sum cs on (tcd.cli_num = cs.cli_num)
	inner join (
		select distinct
			cli_num
		from
			tclient_policy_links
		where
			link_typ in ('O','I','T')
			and image_date = last_day(add_months(current_date,-1))
	) tcpl on (tcpl.cli_num = tcd.cli_num)
	left join cws_legacy_information_mthend cws_leg on (tcd.cli_num = cws_leg.cli_num)
	left join tclient_other_details tcod on (tcod.cli_num=tcd.cli_num and tcod.image_date = tcd.image_date)
	left join tclient_service_registrations tsr on (tsr.cli_num= tcd.cli_num and tsr.image_date = tcd.image_date)
	left join tfield_values tsr_v on (tsr.method_recv=substr(tsr_v.fld_valu,2,1) and tsr.image_date=tsr_v.image_date and tsr_v.fld_nm='METHOD_RECV')
	left join toccupation tocc on (tocc.occp_code=tcd.occp_code and tocc.image_date = tcd.image_date)
	left join vn_published_cics_db.tsms_clients tsms on (tsms.cli_num=tcd.cli_num and tsms.cli_typ='C')
	left join tfield_values tsms_v on (tsms.stat_cd=tsms_v.fld_valu and tsms_v.fld_nm='SMS_REASN_CD' and tsms_v.image_date = last_day(add_months(current_date,-1)))
	left join tmp_cus_add_ful_mthend fadr on (tcd.cli_num=fadr.cli_num)
	left join rs_tinc tinc on (tcd.cli_num = tinc.cli_num)
	left join rs_dpnd dpnd on (tcd.cli_num = dpnd.cli_num)
	left join rs_tmp_last tmp_last on (tcd.cli_num = tmp_last.cli_num)
	left join rs_tmp_frst tmp_frst on (tcd.cli_num = tmp_frst.cli_num)
	left join rs_tmp_trmn tmp_trmn on (tcd.cli_num = tmp_trmn.cli_num)
	left join cws_information_mthend cws on (tcd.cli_num = cws.cli_num)
	left join move_information_mthend move on (tcd.cli_num = move.cli_num)
where
	tcd.image_date = last_day(add_months(current_date,-1))
""")

# COMMAND ----------

result_df = spark.sql("""
SELECT 
*
FROM 
  TCUSTDM_MTHEND;
""")

# COMMAND ----------

#Switch back Spark conf to avoid date adjustment
spark.conf.set("spark.sql.session.timeZone","UTC+0")

#Write result to ADLS gen2   
result_df.write.partitionBy('image_date').mode('overwrite').option("partitionOverwriteMode","dynamic").parquet(f"{mount_path}/Curated/VN/Master/VN_CURATED_DATAMART_DB/TCUSTDM_MTHEND")
