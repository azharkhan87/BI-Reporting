--------------------------------------------------------------------------------Extracting Data of Churners for April 2013------------------------------------



-----------------Subscriber Price Plan at the time of Churning----------------
sel COUNT(accs_meth_id ) from dp_tmp_rep.AK_Churn_Dormancy_PP_Apr 
drop table dp_tmp_rep.AK_Churn_Dormancy_PP_Apr ;
CREATE TABLE dp_tmp_rep.AK_Churn_Dormancy_PP_Apr 
AS
(SEL a.Accs_meth_id, COALESCE(Offering_Id,'3') AS Offering_Id
FROM dp_vew.ACCS_METH_OFFR_STAT_HIST a
LEFT JOIN 
(SEL Accs_Meth_ID, Dormancy_Date, Days_Since_Last_Activity 
FROM dp_mdm_vew.subscriber_dormancy_view
WHERE dormancy_date BETWEEN '2013-04-01' and '2013-04-30'
QUALIFY RANK () OVER (PARTITION BY  Accs_meth_id, dormancy_date ORDER BY days_since_last_activity ASC) = 1
AND days_since_last_activity = '90'
) b
ON a.accs_meth_id = b.accs_meth_id
WHERE CAST(Accs_Meth_Offer_Start_Dt AS DATE) <= b.Dormancy_Date
QUALIFY RANK() OVER (PARTITION BY a.Accs_Meth_ID ORDER BY Accs_Meth_Offer_Start_Dt DESC) = 1
)
WITH DATA PRIMARY INDEX (accs_meth_id);

sel COUNT(distinct accs_meth_id)
FROM dp_mdm_vew.subscriber_dormancy_view
where dormancy_date between '2013-04-01' and '2013-04-30'
and days_since_last_Activity = 90
-----------------------Prepaid Churners Count for April 2013----------------


CREATE TABLE dp_tmp_rep.AK_Churn_OS_Apr AS
(SEL a.Accs_Meth_ID, CAST (dormancy_date AS TIMESTAMP (0)) AS Dormancy_Date, Days_Since_Last_Activity, b. Offering_Id, c.Offering_Name
FROM 
(SEL Accs_Meth_ID, Dormancy_Date, Days_Since_Last_Activity 
FROM dp_mdm_vew.subscriber_dormancy_view
WHERE dormancy_date BETWEEN '2013-04-01' and '2013-04-30'
QUALIFY RANK () OVER (PARTITION BY  Accs_meth_id, dormancy_date ORDER BY days_since_last_activity ASC) = 1
AND days_since_last_activity = '90'
) a
INNER JOIN
(SEL * FROM dp_tmp_rep.AK_Churn_Dormancy_PP_Apr) b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN 
dp_vew.offr 
c
ON b.Offering_id = c.Offering_id
WHERE b.Offering_Id IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
)WITH DATA PRIMARY INDEX (accs_meth_id);

SEL  Offering_Name, COUNT(accs_meth_id) FROM dp_tmp_rep.AK_Churn_OS_Apr
GROUP  BY 1


---------------------Calculating ARPU of Subscribers for 3 Months, starting 6 Months prior to Churn Month------------------------

-----------Outgoing Revenue------------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev1_Oct
AS
(
SEL ACCS_METH_ID ,SUM(Traffic_Rev) AS Rev1_Apr_Oct
FROM
(
SEL accs_meth_id, call_start_dt, COALESCE(SUM(call_gross_Revenue_amt),0) AS Traffic_Rev
FROM dp_vew.call_hist
WHERE call_start_dt BETWEEN '2012-10-01' and '2012-10-31'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr) 
GROUP BY 1,2
) a
GROUP BY 1
)WITH DATA
Primary INDEX(ACCS_METH_ID);


CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev1_Nov
AS
(
SEL ACCS_METH_ID ,SUM(Traffic_Rev) AS Rev1_Apr_Nov
FROM
(
SEL accs_meth_id, call_start_dt, COALESCE(SUM(call_gross_Revenue_amt),0) AS Traffic_Rev
FROM dp_vew.call_hist
WHERE call_start_dt BETWEEN  '2012-11-01' and '2012-11-30'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr) 
GROUP BY 1,2
) a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

drop table dp_tmp_rep.AK_Churn_OS_Rev1_Dec
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev1_Dec
AS
(
SEL ACCS_METH_ID ,SUM(Traffic_Rev) AS Rev1_Apr_Dec
FROM
(
SEL accs_meth_id, call_start_dt, COALESCE(SUM(call_gross_Revenue_amt),0) AS Traffic_Rev
FROM dp_vew.call_hist
WHERE call_start_dt BETWEEN '2012-12-01' and '2012-12-31'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr) 
GROUP BY 1,2
) a
GROUP BY 1
)WITH DATA
Primary INDEX(ACCS_METH_ID);
--------------------------------------------
--------------------------------------------
DROP TABLE dp_tmp_rep.AK_Churn_OS_Rev1
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev1
AS
(SEL a.Accs_Meth_ID, Rev1_Apr_Oct/1.195 AS Rev1_Apr_Oct ,Rev1_Apr_Nov/1.195 AS Rev1_Apr_Nov,Rev1_Apr_Dec/1.195 AS Rev1_Apr_Dec
FROM dp_tmp_rep.AK_Churn_OS_Apr a
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev1_Oct b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev1_Nov c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev1_Dec d
ON a.accs_meth_id = d.accs_meth_id
)
WITH DATA PRIMARY INDEX(accs_meth_id);

SEL * FROM dp_tmp_rep.AK_Churn_OS_Rev1

--------------Incoming Revenue-------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev2_Oct
AS
(SEL ACCS_METH_ID, SUM(In_Traffic_Rev) AS Rev2_Apr_Oct
FROM
(
SEL accs_meth_id, call_Start_dt, (COALESCE(SUM(call_network_volume/60),0))*0.9 AS In_Traffic_Rev
FROM dp_vew.msc_call_hist_vw
WHERE call_start_dt BETWEEN  '2012-10-01' and '2012-10-31'
AND  call_type_cd = '1'
AND orig_oper_name_cd <> '1'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr)
GROUP BY 1,2
)
a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);


DROP TABLE dp_tmp_rep.AK_Churn_OS_Rev2_Nov
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev2_Nov
AS
(SEL ACCS_METH_ID, SUM(In_Traffic_Rev) AS Rev2_Apr_Nov
FROM
(
SEL accs_meth_id, call_Start_dt, (COALESCE(SUM(call_network_volume/60),0))*0.9 AS In_Traffic_Rev
FROM dp_vew.msc_call_hist_vw
WHERE call_start_dt BETWEEN '2012-11-01' and '2012-11-30'
AND  call_type_cd = '1'
AND orig_oper_name_cd <> '1'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr)
GROUP BY 1,2
)
a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev2_Dec
AS
(SEL ACCS_METH_ID, SUM(In_Traffic_Rev) AS Rev2_Apr_Dec
FROM
(
SEL accs_meth_id, call_Start_dt, (COALESCE(SUM(call_network_volume/60),0))*0.9 AS In_Traffic_Rev
FROM dp_vew.msc_call_hist_vw
WHERE call_start_dt BETWEEN '2012-12-01' and '2012-12-31'
AND  call_type_cd = '1'
AND orig_oper_name_cd <> '1'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr)
GROUP BY 1,2
)
a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);
------------------------------------------------
------------------------------------------------
DROP TABLE dp_tmp_rep.AK_Churn_OS_Rev2;
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev2
AS
(SEL a.Accs_Meth_ID, Rev2_Apr_Oct AS Rev2_Apr_Oct,Rev2_Apr_Nov AS Rev2_Apr_Nov,Rev2_Apr_Dec AS Rev2_Apr_Dec
FROM dp_tmp_rep.AK_Churn_OS_Apr a
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev2_Oct b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev2_Nov c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev2_Dec d
ON a.accs_meth_id = d.accs_meth_id
)
WITH DATA;

SEL * FROM dp_tmp_rep.AK_Churn_OS_Rev2

------------Subscriptions Revenue----------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev3_Oct
AS
(
                                SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Apr_Oct
                                FROM 
                                (
                                SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
                                FROM dp_vew.sip_snapshot
                                WHERE action = 'add' AND status = 'success' 
                                AND created_date BETWEEN  '2012-10-01' and '2012-10-31'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr GROUP BY 1)
                                AND unit_cost >0
                                GROUP BY 1
								UNION 
  								SEL accs_meth_id,  SUM(amount) AS subscription_rev
                                FROM dp_vew.mediated_confrmtn_event_vew
                                WHERE confrmtn_event_start_dt BETWEEN  '2012-10-01' and '2012-10-31'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr GROUP BY 1)
                                AND amount >0
                                GROUP BY 1
                                ) a 
GROUP BY 1
)
WITH DATA PRIMARY INDEX(accs_meth_id);


DROP TABLE dp_tmp_rep.AK_Churn_OS_Rev3_Nov;
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev3_Nov
AS
(
                                SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Apr_Nov
                                FROM 
                                (
                                SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
                                FROM dp_vew.sip_snapshot
                                WHERE action = 'add' AND status = 'success' 
                                AND created_date BETWEEN '2012-11-01' and '2012-11-30'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr GROUP BY 1)
                                AND unit_cost >0
                                GROUP BY 1
								UNION 
  								SEL accs_meth_id,  SUM(amount) AS subscription_rev
                                FROM dp_vew.mediated_confrmtn_event_vew
                                WHERE confrmtn_event_start_dt BETWEEN  '2012-11-01' and '2012-11-30'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr GROUP BY 1)
                                AND amount >0
                                GROUP BY 1
                                ) a 
GROUP BY 1
)
WITH DATA PRIMARY INDEX(accs_meth_id);

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev3_Dec
AS
(
                                SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Apr_Dec
                                FROM 
                                (
                                SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
                                FROM dp_vew.sip_snapshot
                                WHERE action = 'add' AND status = 'success' 
                                AND created_date BETWEEN '2012-12-01' and '2012-12-31'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr GROUP BY 1)
                                AND unit_cost >0
                                GROUP BY 1
								UNION 
  								SEL accs_meth_id,  SUM(amount) AS subscription_rev
                                FROM dp_vew.mediated_confrmtn_event_vew
                                WHERE confrmtn_event_start_dt BETWEEN  '2012-12-01' and '2012-12-31'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr GROUP BY 1)
                                AND amount >0
                                GROUP BY 1
                                ) a 
GROUP BY 1
)
WITH DATA Primary INDEX(accs_meth_id);

DROP TABLE dp_tmp_rep.AK_Churn_OS_Rev3 ;
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev3 AS
(
SEL a.accs_meth_id, Rev3_Apr_Oct/1.195 AS Rev3_Apr_Oct ,Rev3_Apr_Nov/1.195 AS Rev3_Apr_Nov, Rev3_Apr_Dec/1.195 AS Rev3_Apr_Dec
FROM dp_tmp_rep.AK_Churn_OS_Apr a
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_Rev3_Oct b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_Rev3_Nov c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_Rev3_Dec d
ON a.accs_meth_id = d.accs_meth_id
)
WITH DATA PRIMARY INDEX(accs_meth_id);

SEL * FROM dp_tmp_rep.AK_Churn_OS_Rev3

-------------Surcharge Revenue---------------------
DROP TABLE dp_tmp_rep.AK_Churn_OS_Rev4_Oct;
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev4_Oct
AS
(SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.07*1.195),0) AS Rev4_Apr_Oct
FROM dp_mdm_vew.subscriber_dly_recharge
WHERE recharge_dt BETWEEN  '2012-10-01' and '2012-10-31'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr)
GROUP BY 1
)
WITH DATA
Primary INDEX(accs_meth_id);

DROP TABLE dp_tmp_rep.AK_Churn_OS_Rev4_Nov;
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev4_Nov
AS
(SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.07*1.195),0) AS Rev4_Apr_Nov
FROM dp_mdm_vew.subscriber_dly_recharge
WHERE recharge_dt BETWEEN '2012-11-01' and '2012-11-30'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr)
GROUP BY 1
)
WITH DATA
PRIMARY INDEX(accs_meth_id);

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev4_Dec
AS
(SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*.07*1.195),0) AS Rev4_Apr_Dec
FROM dp_mdm_vew.subscriber_dly_recharge
WHERE recharge_dt BETWEEN '2012-12-01' and '2012-12-31'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Apr)
GROUP BY 1
)
WITH DATA
PRIMARY INDEX(accs_meth_id);


CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev4 AS
(
SEL a.accs_meth_id, Rev4_Apr_Oct/1.195 AS Rev4_Apr_Oct,Rev4_Apr_Nov/1.195 AS Rev4_Apr_Nov, Rev4_Apr_Dec/1.195 AS Rev4_Apr_Dec

FROM dp_tmp_rep.AK_Churn_OS_Apr a
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_Rev4_Oct b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_Rev4_Nov c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_Rev4_Dec d
ON a.accs_meth_id = d.accs_meth_id

)WITH DATA;

SEL * FROM dp_tmp_rep.AK_Churn_OS_Rev4

----------------------------------------------------------------
drop table dp_tmp_rep.AK_Churn_OS_Rev_Apr;
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev_Apr
AS
(
SEL a.accs_meth_id, Offering_Name, (COALESCE(Rev1_Apr_Oct,0)+COALESCE(Rev2_Apr_Oct,0)+COALESCE(Rev3_Apr_Oct,0)+COALESCE(Rev4_Apr_Oct,0)) AS Rev_Apr_Oct,
(COALESCE(Rev1_Apr_Nov,0)+COALESCE(Rev2_Apr_Nov,0)+COALESCE(Rev3_Apr_Nov,0)+COALESCE(Rev4_Apr_Nov,0)) AS Rev_Apr_Nov,
(COALESCE(Rev1_Apr_Dec,0)+COALESCE(Rev2_Apr_Dec,0)+COALESCE(Rev3_Apr_Dec,0)+COALESCE(Rev4_Apr_Dec,0)) AS Rev_Apr_Dec
FROM dp_tmp_rep.AK_Churn_OS_Apr a
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev1 b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev2 c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev3 d
ON a.accs_meth_id = d.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_Rev4 e
ON a.accs_meth_id = e.accs_meth_id
GROUP BY 1,2,3,4,5
WHERE Rev1_Apr_Oct IS NOT NULL
OR Rev2_Apr_Oct IS NOT NULL
OR Rev3_Apr_Oct IS NOT NULL
OR Rev4_Apr_Oct IS NOT NULL
OR Rev1_Apr_Nov IS NOT NULL
OR Rev2_Apr_Nov IS NOT NULL
OR Rev3_Apr_Nov IS NOT NULL
OR Rev4_Apr_Nov IS NOT NULL
OR Rev1_Apr_Dec IS NOT NULL
OR Rev2_Apr_Dec IS NOT NULL
OR Rev3_Apr_Dec IS NOT NULL
OR Rev4_Apr_Dec IS NOT NULL
)
WITH DATA PRIMARY INDEX (accs_meth_id);

--------------------------Creating Monthly Revenue Bands for Subscribers----------------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev_Bands_Apr AS
(SEL Accs_meth_id, Offering_Name, SUM(Rev_Apr_Oct+Rev_Apr_Nov+Rev_Apr_Dec) AS Tot_Rev,
(CASE WHEN Tot_Rev/3 <= 0 THEN 'A: =0'
WHEN Tot_Rev/3 > 0 AND Tot_Rev/3 <= 5 THEN 'B: 1 - 5'
WHEN Tot_Rev/3 > 5 AND Tot_Rev/3 <= 10 THEN 'C: 6 - 10'
WHEN Tot_Rev/3 > 10 AND Tot_Rev/3 <= 15 THEN 'D: 11 - 15'
WHEN Tot_Rev/3 > 15 AND Tot_Rev/3 <= 20 THEN 'E: 16 - 20'
WHEN Tot_Rev/3 > 20 AND Tot_Rev/3 <= 50 THEN 'F: 21 - 50'
WHEN Tot_Rev/3 > 50 AND Tot_Rev/3 <= 100 THEN 'G: 51 - 100'
WHEN Tot_Rev/3 > 100 AND Tot_Rev/3 <= 200 THEN 'H: 101 - 200'
WHEN Tot_Rev/3 > 200 AND Tot_Rev/3 <= 500 THEN 'I: 201 - 500'
WHEN Tot_Rev/3 > 500 AND Tot_Rev/3 <= 1000 THEN 'J: 501 - 1000'
WHEN Tot_Rev/3 > 1000 THEN 'K: 1000+' END) AS Rev_Bands
FROM dp_tmp_rep.AK_Churn_OS_Rev_Apr
GROUP BY 1,2
) WITH DATA PRIMARY INDEX (accs_meth_id);

SEL * FROM dp_tmp_rep.AK_Churn_OS_Rev_Bands_Apr

--------------Voice Duration Banding----------------------------------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev_Mins_Apr AS
(SEL a.*, Tot_Mins, (CASE WHEN Tot_Mins >= 0 AND Tot_Mins <= 20 THEN 'A: 0 - 20'
WHEN Tot_Mins > 20 AND Tot_Mins <= 50 THEN 'B: 21 - 50'
WHEN Tot_Mins > 50 AND Tot_Mins <= 100 THEN 'C: 51 - 100'
WHEN Tot_Mins > 100 AND Tot_Mins <= 200 THEN 'D: 101 - 200'
WHEN Tot_Mins > 200 AND Tot_Mins <= 500 THEN 'E: 201 - 500'
WHEN Tot_Mins > 500 AND Tot_Mins <= 1000 THEN 'F: 501 - 1000'
WHEN Tot_Mins > 1000 THEN 'G: > - 1000' END) AS VC_Dur_Bands
FROM dp_tmp_rep.AK_Churn_OS_Rev_Bands_Apr a
LEFT JOIN
(SEL accs_meth_id, SUM(Call_Rated_Vol) AS Tot_Mins
FROM dp_vew.call_hist
WHERE accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Rev_Bands_apr)
AND CAST(call_Start_dt AS DATE) BETWEEN '2012-10-01' AND '2012-12-31'
GROUP BY 1
) b
ON a.accs_meth_id = b.accs_meth_id
) WITH DATA Primary INDEX (accs_meth_id);

----------Most Used Cell Sites of Subscribers-------------------

-----------------All Available CellSites--------------------

CREATE TABLE dp_tmp_rep.AA_CELL_SITE
AS
(
SEL CELL_SITE_NAME,A.DISTRICT,B.SITE_ID,CELL_SITE_ID,LAC
FROM dp_vew.mkt_geo_hier AS A
LEFT JOIN DP_vew.HLP_SITE AS B
ON A.CELL_SITE_NAME=B.SITE_NAME
LEFT JOIN CELL_SITE_HIST AS C
ON B.SITE_ID=C.SITE_ID
WHERE cell_site_end_dt IS NULL
)WITH DATA
PRIMARY INDEX(CELL_SITE_NAME,CELL_SITE_ID,LAC)

-----------------------Monthly Usage CellSites--------------------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Citywise_Apr_Oct
AS
(
SEL ACCS_METH_ID,OFFERING_ID,MNTH,DISTRICT
,SUM(CNT) AS TOTAL_COUNT

FROM
(
SEL  ACCS_METH_ID,OFFERING_ID,EXTRACT(MONTH FROM CALL_START_DT) AS MNTH,CELL_SITE_ID,LAC,CALL_SVC_TYPE_CD
,SUM(CALL_RATED_VOL) AS CALL_RATED_VOL
,SUM(CALL_NETWORK_VOL) AS CALL_NETWORK_VOL
,SUM(CALL_GROSS_REVENUE_AMT) AS CALL_GROSS_REVENUE_AMT
,COUNT(*) AS CNT
FROM dp_vew.CALL_HIST AS A
WHERE CALL_START_DT BETWEEN '2012-10-01' and '2012-10-31'
AND OFFERING_ID IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Rev_Mins_Apr)
GROUP BY 1,2,3,4,5,6
) AS A
LEFT JOIN dp_tmp_rep.AA_CELL_SITE AS B
ON A.CELL_SITE_ID=B.CELL_SITE_ID
AND A.LAC=B.LAC
GROUP BY 1,2,3,4
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Citywise_Apr_Nov
AS
(
SEL ACCS_METH_ID,OFFERING_ID,MNTH,DISTRICT
,SUM(CNT) AS TOTAL_COUNT

FROM
(
SEL  ACCS_METH_ID,OFFERING_ID,EXTRACT(MONTH FROM CALL_START_DT) AS MNTH,CELL_SITE_ID,LAC,CALL_SVC_TYPE_CD
,SUM(CALL_RATED_VOL) AS CALL_RATED_VOL
,SUM(CALL_NETWORK_VOL) AS CALL_NETWORK_VOL
,SUM(CALL_GROSS_REVENUE_AMT) AS CALL_GROSS_REVENUE_AMT
,COUNT(*) AS CNT
FROM dp_vew.CALL_HIST AS A
WHERE CALL_START_DT BETWEEN '2012-11-01' and '2012-11-30'
AND OFFERING_ID IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Rev_Mins_Apr)
GROUP BY 1,2,3,4,5,6
) AS A
LEFT JOIN dp_tmp_rep.AA_CELL_SITE AS B
ON A.CELL_SITE_ID=B.CELL_SITE_ID
AND A.LAC=B.LAC
GROUP BY 1,2,3,4
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Churn_OS_Citywise_Apr_Dec
AS
(
SEL ACCS_METH_ID,OFFERING_ID,MNTH,DISTRICT
,SUM(CNT) AS TOTAL_COUNT

FROM
(
SEL  ACCS_METH_ID,OFFERING_ID,EXTRACT(MONTH FROM CALL_START_DT) AS MNTH,CELL_SITE_ID,LAC,CALL_SVC_TYPE_CD
,SUM(CALL_RATED_VOL) AS CALL_RATED_VOL
,SUM(CALL_NETWORK_VOL) AS CALL_NETWORK_VOL
,SUM(CALL_GROSS_REVENUE_AMT) AS CALL_GROSS_REVENUE_AMT
,COUNT(*) AS CNT
FROM dp_vew.CALL_HIST AS A
WHERE CALL_START_DT BETWEEN '2012-12-01' and '2012-12-31'
AND OFFERING_ID IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_Rev_Mins_Apr)
GROUP BY 1,2,3,4,5,6
) AS A
LEFT JOIN dp_tmp_rep.AA_CELL_SITE AS B
ON A.CELL_SITE_ID=B.CELL_SITE_ID
AND A.LAC=B.LAC
GROUP BY 1,2,3,4
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

------------------------------------------------------------------
------------------------------------------------------------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_CW_Combine_Apr
AS
(
SEL * FROM dp_tmp_rep.AK_Churn_OS_Citywise_Apr_Oct
UNION ALL
SEL * FROM dp_tmp_rep.AK_Churn_OS_Citywise_Apr_Nov
UNION ALL
SEL * FROM dp_tmp_rep.AK_Churn_OS_Citywise_Apr_Dec
)
WITH DATA;

----------------Most Used Cell Site by April Churners------------------------

CREATE  TABLE dp_tmp_rep.AK_Churn_OS_CW_Subs_Apr
AS
(
SEL a.*
FROM
(
SEL Accs_Meth_Id ,District,SUM(TOTAL_COUNT) AS TOTAL_COUNT 
FROM dp_tmp_rep.AK_Churn_OS_CW_Combine_Apr
WHERE district IS NOT NULL
GROUP BY 1,2
) AS a
QUALIFY ROW_NUMBER() OVER(PARTITION BY Accs_Meth_Id  ORDER BY  TOTAL_COUNT  DESC)=1
)WITH DATA
Primary INDEX(ACCS_METH_ID,DISTRICT);

CREATE  TABLE dp_tmp_rep.AK_Churn_OS_CW_Apr
AS
(
SEL A.ACCS_METH_ID,COALESCE(B.DISTRICT,'UNKNOWN') AS DISTRICT
FROM  dp_tmp_rep.AK_Churn_OS_Rev_Mins_Apr AS A
LEFT JOIN dp_tmp_rep.AK_Churn_OS_CW_Subs_Apr AS b
ON a.accs_meth_id=b.accs_meth_id
GROUP BY 1,2
)WITH DATA
PRIMARY INDEX(accs_meth_id,district)

SEL * FROM dp_tmp_rep.AK_Churn_OS_CW_Apr

-------------------Checking Subs Ending Balance------------------
drop table dp_tmp_rep.AK_Churn_OS_End_Bal_Apr 
CREATE TABLE dp_tmp_rep.AK_Churn_OS_End_Bal_Apr AS
(SEL * FROM dp_vew.sub_bal
WHERE accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_CW_Apr)
AND Ins_Date < '2012-12-01'
QUALIFY RANK() OVER (PARTITION BY Accs_Meth_ID ORDER BY Ins_Date DESC) = 1
)WITH DATA PRIMARY INDEX(Accs_Meth_ID);

------------------------Subs FCA Date-------------------------------------

CREATE TABLE dp_tmp_rep.AK_Churn_OS_FCA_Apr AS
(
SEL a.*, Age_1 AS FCA_Date, (CAST(Dormancy_Date AS DATE) - FCA_Date) AS Tenure
FROM dp_tmp_rep.AK_Churn_OS_Apr a
INNER JOIN
(SEL Accs_meth_id, Age_1 
FROM dp_vew.Subscriber_Hist
QUALIFY RANK () OVER (PARTITION BY Accs_Meth_Id ORDER BY Age_1 DESC) = 1
) b 
ON a.accs_meth_id = b.accs_meth_id
)
WITH DATA;

--------------------Final Churners Dataset for Apr 2013-----------
drop table dp_tmp_rep.AK_Churn_OS_All_Info_Apr 
CREATE TABLE dp_tmp_rep.AK_Churn_OS_All_Info_Apr AS
(SEL a.Accs_Meth_Id, a.Offering_Name, Tot_Rev, Rev_Bands,Tot_Mins, VC_Dur_Bands, District, COALESCE(Accs_Meth_Bal_Amt,0) AS Ending_Bal, 
(CASE WHEN Ending_Bal <= 0 THEN 'A: <= - 0'
WHEN Ending_Bal > 0 AND Ending_Bal <= 5 THEN 'B: 1 - 5'
WHEN Ending_Bal > 5 AND Ending_Bal <= 10 THEN 'C: 6 - 10'
WHEN Ending_Bal > 10 AND Ending_Bal <= 20 THEN 'D: 11 - 20'
WHEN Ending_Bal > 20 AND Ending_Bal <= 30 THEN 'E: 21 - 30'
WHEN Ending_Bal > 30 AND Ending_Bal <= 40 THEN 'F: 31 - 40'
WHEN Ending_Bal > 40 AND Ending_Bal <= 50 THEN 'G: 41 - 50'
WHEN Ending_Bal > 50 THEN 'H: > - 50' END) AS End_Bal_Bands, COALESCE((CASE WHEN Tenure >= 0 AND  Tenure <= 90 THEN 'A: 0 - 90'
WHEN Tenure > 90 AND Tenure <= 180 THEN 'B: 91 - 180'
WHEN Tenure > 180 AND Tenure <= 270 THEN 'C: 181 - 270'
WHEN Tenure > 270 AND Tenure <= 360 THEN 'D: 271 - 360'
WHEN Tenure > 360 AND Tenure <= 540 THEN 'E: 361 - 540'
WHEN Tenure > 540 AND Tenure <= 720 THEN 'F: 541 - 720'
WHEN Tenure > 720 AND Tenure <= 1080 THEN 'G: 721 - 1080'
WHEN Tenure > 1080 AND Tenure <= 1440 THEN 'H: 1081 - 1440'
WHEN Tenure > 1440 AND Tenure <= 1800 THEN 'I: 1441 - 1800'
WHEN Tenure >1800 THEN 'J: 1800 +' END),'Unknwon') AS Age_Bands
FROM dp_tmp_rep.AK_Churn_OS_Rev_Apr a
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_Rev_Mins_Apr b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Churn_OS_CW_Apr c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_End_Bal_Apr d
ON a.accs_meth_id = d.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Churn_OS_FCA_Apr e
ON a.accs_meth_id = e.accs_meth_id
)
WITH DATA PRIMARY INDEX(accs_meth_id);

-- final queries for end result

sel offering_name, COUNT (distinct accs_meth_id) distinct_subs from DP_TMP_REP.AK_churn_os_all_info_Apr GROUP BY  1

sel Offering_name, district, COUNT (distinct Accs_Meth_Id)subs from DP_TMP_REP.AK_churn_os_all_info_Apr GROUP BY  1,2 order by 1,2

sel district, COUNT (distinct Accs_Meth_Id)subs from DP_TMP_REP.AK_churn_os_all_info_Apr GROUP BY  1 order by 1

SEL rev_bands, offering_name,COUNT  (DISTINCT accs_meth_id), SUM(tot_rev)tot_rev, SUM(tot_mins) tot_min FROM DP_TMP_REP.AK_churn_os_all_info_Apr GROUP BY 1,2
ORDER BY 1,2

sel end_bal_bands, offering_name,COUNT(distinct accs_meth_id) dist_subs, SUM(Ending_Bal)total_ending_bal from  DP_TMP_REP.AK_churn_os_all_info_Apr GROUP BY  1,2 ORDER BY 1,2 

SEL Age_bands, offering_name, COUNT (DISTINCT accs_meth_id) FROM DP_TMP_REP.AK_churn_os_all_info_Apr GROUP BY  1,2 ORDER BY 1,2 


----------------------------Other Requests------------------------------------------

-------------Churners Age Bands vs Rev and End Bal Bands Cross Tabbing-----------------------

SEL Age_Bands, COUNT(CASE WHEN Rev_Bands = 'A: =0' THEN accs_meth_id END) AS "A: =0",
COUNT(CASE WHEN Rev_Bands = 'B: 1 - 5' THEN accs_meth_id END) AS "B: 1 - 5",
COUNT(CASE WHEN Rev_Bands = 'C: 6 - 10' THEN accs_meth_id END) AS "C: 6 - 10",
COUNT(CASE WHEN Rev_Bands = 'D: 11 - 15' THEN accs_meth_id END) AS "D: 11 - 15",
COUNT(CASE WHEN Rev_Bands = 'E: 16 - 20' THEN accs_meth_id END) AS "E: 16 - 20",
COUNT(CASE WHEN Rev_Bands = 'F: 21 - 50' THEN accs_meth_id END) AS "F: 21 - 50",
COUNT(CASE WHEN Rev_Bands = 'G: 51 - 100' THEN accs_meth_id END) AS "G: 51 - 100",
COUNT(CASE WHEN Rev_Bands = 'H: 101 - 200' THEN accs_meth_id END) AS "H: 101 - 200",
COUNT(CASE WHEN Rev_Bands = 'I: 201 - 500' THEN accs_meth_id END) AS "I: 201 - 500",
COUNT(CASE WHEN Rev_Bands = 'J: 501 - 1000' THEN accs_meth_id END) AS "J: 501 - 1000",
COUNT(CASE WHEN Rev_Bands = 'K: 1000+' THEN accs_meth_id END) AS "K: > - 1000"
FROM dp_tmp_rep.AK_churn_os_all_info_Apr
GROUP BY 1
ORDER BY 1

SEL Age_Bands, COUNT(CASE WHEN End_Bal_Bands = 'A: <= - 0' THEN accs_meth_id END) AS "A: <= - 0",
COUNT(CASE WHEN End_Bal_Bands = 'B: 1 - 5' THEN accs_meth_id END) AS "B: 1 - 5",
COUNT(CASE WHEN End_Bal_Bands = 'C: 6 - 10' THEN accs_meth_id END) AS "C: 6 - 10",
COUNT(CASE WHEN End_Bal_Bands = 'D: 11 - 20' THEN accs_meth_id END) AS "D: 11 - 20",
COUNT(CASE WHEN End_Bal_Bands = 'E: 21 - 30' THEN accs_meth_id END) AS "E: 21 - 30",
COUNT(CASE WHEN End_Bal_Bands = 'F: 31 - 40' THEN accs_meth_id END) AS "F: 31 - 40",
COUNT(CASE WHEN End_Bal_Bands = 'G: 41 - 50' THEN accs_meth_id END) AS "G: 41 - 50",
COUNT(CASE WHEN End_Bal_Bands = 'H: > - 50' THEN accs_meth_id END) AS "H: > - 50"
FROM dp_tmp_rep.AK_churn_os_all_info_Apr
GROUP BY 1
ORDER BY 1




------------------------90 Days Active Base --------------

Create table dp_tmp_rep.AA_Entire_Active_Base_Apr as
(
sel a.Accs_Meth_ID, Dormancy_Date, Days_Since_Last_Activity, Coalesce(d.Offering_Id,'3') as Offering_Id, Offering_Name
from 
(
sel Accs_Meth_ID, Dormancy_Date, Days_Since_Last_Activity
from dp_mdm_vew.subscriber_dormancy_view
where Dormancy_Date = '2013-04-30'
and Days_Since_Last_Activity < 90
Qualify Rank () Over (Partition by Accs_Meth_Id, Dormancy_Date order by Days_Since_Last_Activity Asc) = 1
) a
left join
(
sel accs_meth_id, accs_meth_svc_type_cd, end_dt_tm
from dp_vew.subscriber_hist
where cast(end_dt_tm as date) = '9999-12-31'
and accs_meth_svC_type_cd = 0
) c
on a.accs_meth_id = c.accs_meth_id
left join 
dp_vew.ACCS_METH_OFFR_STAT_HIST 
d
on a.accs_meth_id = d.accs_meth_id
left join 
dp_vew.offr 
e
on d.Offering_id = e.Offering_id
where d.Offering_Id in (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
and cast(Accs_Meth_Offer_Start_Dt as Date) <= Dormancy_Date
Qualify Rank() Over (Partition by a.Accs_Meth_ID order by Accs_Meth_Offer_Start_Dt Desc) = 1
)
with data primary index (Accs_Meth_Id);

sel Offering_Name as Price_Plan, Count(accs_meth_id) Distinct_Subs
from dp_tmp_rep.AA_Entire_Active_Base_Apr
group by 1

------------------------Base FCA and Tenure----------

Create table dp_tmp_rep.AA_Entire_Active_Base_Apr_Ten as
(
sel a.*, Age_1 as FCA_Date, (cast(Dormancy_Date as date) - FCA_Date) as Tenure
from dp_tmp_rep.AA_Entire_Active_Base_Apr a
inner join
(sel Accs_meth_id, Age_1, Row_Type_Cd, End_Dt_Tm
from dp_vew.Subscriber_Hist
Qualify Rank () over (Partition by Accs_Meth_Id order by Age_1 Desc) = 1
) b 
on a.accs_meth_id = b.accs_meth_id
)
with data;

----------------Base Age Bands----------------

Create table dp_tmp_rep.AA_Active_Base_Tenure_Apr as
(sel a.*, Coalesce((Case when Tenure >= 0 and  Tenure <= 90 then 'A: 0 - 90'
when Tenure > 90 and Tenure <= 180 then 'B: 91 - 180'
when Tenure > 180 and Tenure <= 270 then 'C: 181 - 270'
when Tenure > 270 and Tenure <= 360 then 'D: 271 - 360'
when Tenure > 360 and Tenure <= 540 then 'E: 361 - 540'
when Tenure > 540 and Tenure <= 720 then 'F: 541 - 720'
when Tenure > 720 and Tenure <= 1080 then 'G: 721 - 1080'
when Tenure > 1080 and Tenure <= 1440 then 'H: 1081 - 1440'
when Tenure > 1440 and Tenure <= 1800 then 'I: 1441 - 1800'
when Tenure >1800 then 'J: 1800 +' end),'Unknwon') as Age_Bands
from dp_tmp_rep.AA_Entire_Active_Base_Apr_Ten a
)
with data primary index(accs_meth_id);

sel Age_Bands, Count(accs_meth_id) as Subscribers
from dp_tmp_rep.AA_Active_Base_Tenure_Apr
group by 1
order by 1






































----------Not Updated----------------------------


------------------------------------------Check for Poora Pakistan Offer Subscriptions for Dece Churners-------------------
SEL COUNT (DISTINCT accs_meth_id) FROM dp_tmp_rep.AK_Churn_OS_Rev3_Jan_NovDec
CREATE TABLE dp_tmp_rep.AK_Churn_OS_Rev3_Jan_NovDec
AS
(
                                SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Jan_NovDec
                                FROM 
                                ( 
   								SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
								FROM dp_vew.sip_snapshot
								WHERE action = 'add' AND status = 'success' 
								AND created_date BETWEEN '2012-01-01' AND '2012-12-31'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_All_Info_Dec GROUP BY 1)
                                AND unit_cost >0
                                AND prod_id = 806
                                GROUP BY 1
                                UNION 
								SEL accs_meth_id,  SUM(amount) AS subscription_rev
                                FROM dp_vew.mediated_confrmtn_event_vew
                                WHERE confrmtn_event_start_dt BETWEEN '2012-01-01' AND '2012-12-31'
                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Churn_OS_All_Info_Dec GROUP BY 1)
                                AND amount >0
                                AND usage_type = 'at_5_nonrecur'
                                GROUP BY 1
								) a 
GROUP BY 1
)
WITH DATA PRIMARY INDEX(accs_meth_id);