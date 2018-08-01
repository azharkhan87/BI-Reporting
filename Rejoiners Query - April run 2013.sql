

-- Apr 2013 Run 

-------------------- Feb 2013 Rejoiners--------------------------

SEL * FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_Feb
CREATE TABLE dp_tmp_rep.AK_Rejoiner_Spin_Down_Feb
AS
(
SEL * FROM dp_mdm_vew.subscriber_dormancy_view
WHERE Dormancy_Date BETWEEN '2013-02-01' and '2013-02-28'
AND Days_Since_Last_Activity >=90
QUALIFY RANK() OVER (PARTITION BY Party_Id, Accs_Meth_Id ORDER BY Dormancy_Date ASC, Days_Since_Last_Activity ASC) = 1
)
WITH DATA PRIMARY INDEX(accs_meth_id);



SEL count(distinct Accs_Meth_Id) FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb
CREATE TABLE dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb
AS
(
SEL a.party_id, a.accs_meth_id, b.Dormancy_Date, b.Days_Since_Last_Activity
FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_Feb a
INNER JOIN
(SEL * FROM dp_mdm_vew.subscriber_dormancy_view
WHERE Dormancy_Date BETWEEN '2013-02-01' and '2013-02-28'
AND Days_Since_Last_Activity = 0
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_Feb)
QUALIFY RANK() OVER (PARTITION BY Party_Id, accs_meth_id ORDER BY Dormancy_Date ASC, Days_Since_Last_Activity ASC) = 1
) b
ON a.accs_meth_id = b.accs_meth_id
AND a.party_id = b.party_id
)
WITH DATA PRIMARY INDEX(accs_meth_id);


--------------------------------------Dormancy Bands of Rejoiners-----------------------

drop table dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb;
CREATE TABLE dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb
AS
(
SEL a.Accs_Meth_Id, a.Dormancy_Date AS Rejoin_Date,b.Dormancy_Date, b.Days_Since_Last_Activity
FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb a
LEFT JOIN 
(
SEL * FROM dp_mdm_vew.subscriber_dormancy_view
WHERE Dormancy_Date BETWEEN '2013-01-15' AND '2013-03-15'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb)
) b
ON a.accs_meth_id = b.accs_meth_id
AND a.party_id = b.party_id
WHERE b.Dormancy_Date = a.Dormancy_Date - 1
)
WITH DATA PRIMARY INDEX(accs_meth_id);

SEL (CASE WHEN Days_Since_Last_Activity >= 90 AND Days_Since_Last_Activity <= 120 THEN 'A: 91-120'
WHEN Days_Since_Last_Activity > 120 AND Days_Since_Last_Activity <= 150 THEN 'B: 121-150'
WHEN Days_Since_Last_Activity > 150 AND Days_Since_Last_Activity <= 180 THEN 'C: 151-180'
WHEN Days_Since_Last_Activity > 180 AND Days_Since_Last_Activity <= 270 THEN 'D: 181-270'
WHEN Days_Since_Last_Activity > 270 AND Days_Since_Last_Activity <= 360 THEN 'E: 271-360'
WHEN Days_Since_Last_Activity > 360 AND Days_Since_Last_Activity <= 540 THEN 'F: 361-540'
WHEN Days_Since_Last_Activity > 540 AND Days_Since_Last_Activity <= 720 THEN 'G: 541-720'
WHEN Days_Since_Last_Activity > 720 THEN 'H: 720+' END) AS Dormancy_Bands,  COUNT(accs_meth_id) AS Rejoiners_Count_Feb
FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb
WHERE Days_Since_Last_Activity >= 90
GROUP BY 1


------------Rejoiners Price Plans Checking - Prepaid-------------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb_PP AS
(SEL a.*, COALESCE(b.Offering_Id,'3') AS Offering_Id, Offering_Name
FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb a
LEFT JOIN 
dp_vew.ACCS_METH_OFFR_STAT_HIST
b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN 
dp_vew.offr c
ON b.Offering_id = c.Offering_id
WHERE b.Offering_Id IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
AND CAST(Accs_Meth_Offer_Start_Dt AS DATE) <= Rejoin_Date
QUALIFY RANK() OVER (PARTITION BY a.Accs_Meth_ID ORDER BY Accs_Meth_Offer_Start_Dt DESC) = 1
)
WITH DATA PRIMARY INDEX (accs_meth_id);

--------------------------------Rejoiners ARPU Calculation--------------------------Feb 2013----------------

-----------Outgoing Revenue------------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev1_Feb_Feb
AS
(
SEL ACCS_METH_ID ,SUM(Traffic_Rev) AS Rev1_Feb_Feb
FROM
(
SEL accs_meth_id, call_start_dt, COALESCE(SUM(call_gross_Revenue_amt),0) AS Traffic_Rev
FROM dp_vew.call_hist
WHERE call_start_dt BETWEEN '2013-02-01' and '2013-02-28'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb) 
GROUP BY 1,2
) a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev1_Feb_Mar
AS
(
SEL ACCS_METH_ID ,SUM(Traffic_Rev) AS Rev1_Feb_Mar
FROM
(
SEL accs_meth_id, call_start_dt, COALESCE(SUM(call_gross_Revenue_amt),0) AS Traffic_Rev
FROM dp_vew.call_hist
WHERE call_start_dt BETWEEN '2013-03-01' and '2013-03-31'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb) 
GROUP BY 1,2
) a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev1_Feb_Apr
AS
(
SEL ACCS_METH_ID ,SUM(Traffic_Rev) AS Rev1_Feb_Apr
FROM
(
SEL accs_meth_id, call_start_dt, COALESCE(SUM(call_gross_Revenue_amt),0) AS Traffic_Rev
FROM dp_vew.call_hist
WHERE call_start_dt BETWEEN '2013-04-01' and '2013-04-30'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb) 
GROUP BY 1,2
) a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

-------------------------------------------
-------------------------------------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev1_Feb
AS
(SEL a.Accs_Meth_ID, Rev1_Feb_Feb/1.195 AS Rev1_Feb_Feb ,Rev1_Feb_Mar/1.195 AS Rev1_Feb_Mar,Rev1_Feb_Apr/1.195 AS Rev1_Feb_Apr
FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb a
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev1_Feb_Feb b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev1_Feb_Mar c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev1_Feb_Apr d
ON a.accs_meth_id = d.accs_meth_id
)
WITH DATA PRIMARY INDEX(accs_meth_id);
--------------------------------------------
--------------------------------------------

--------------Incoming Revenue-------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev2_Feb_Feb
AS
(SEL ACCS_METH_ID, SUM(In_Traffic_Rev) AS Rev2_Feb_Feb
FROM
(
SEL accs_meth_id, call_Start_dt, (COALESCE(SUM(call_network_volume/60),0))*0.9 AS In_Traffic_Rev
FROM dp_vew.msc_call_hist_vw
WHERE call_start_dt BETWEEN '2013-02-01' and '2013-02-28'
AND  call_type_cd = '1'
AND orig_oper_name_cd <> '1'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb)
GROUP BY 1,2
)
a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev2_Feb_Mar
AS
(SEL ACCS_METH_ID, SUM(In_Traffic_Rev) AS Rev2_Feb_Mar
FROM
(
SEL accs_meth_id, call_Start_dt, (COALESCE(SUM(call_network_volume/60),0))*0.9 AS In_Traffic_Rev
FROM dp_vew.msc_call_hist_vw
WHERE call_start_dt BETWEEN '2013-03-01' and '2013-03-31'
AND  call_type_cd = '1'
AND orig_oper_name_cd <> '1'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb)
GROUP BY 1,2
)
a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev2_Feb_Apr
AS
(SEL ACCS_METH_ID, SUM(In_Traffic_Rev) AS Rev2_Feb_Apr
FROM
(
SEL accs_meth_id, call_Start_dt, (COALESCE(SUM(call_network_volume/60),0))*0.9 AS In_Traffic_Rev
FROM dp_vew.msc_call_hist_vw
WHERE call_start_dt BETWEEN '2013-04-01' and '2013-04-30'
AND  call_type_cd = '1'
AND orig_oper_name_cd <> '1'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb)
GROUP BY 1,2
)
a
GROUP BY 1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

------------------------------------------------
------------------------------------------------
CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev2_Feb
AS
(SEL a.Accs_Meth_ID, Rev2_Feb_Feb, Rev2_Feb_Mar, Rev2_Feb_Apr
FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb a
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev2_Feb_Feb b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev2_Feb_Mar c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev2_Feb_Apr d
ON a.accs_meth_id = d.accs_meth_id
)
WITH DATA;
------------------------------------------------
------------------------------------------------


------------Subscriptions Revenue----------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev3_Feb_Feb
AS
(
                                SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Feb_Feb
                                FROM 
                                ( 
                                                
                                                SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
                                                FROM dp_vew.SIP_SNAPSHOT
                                                WHERE action = 'Add' AND Status = 'Success' 
                                                AND CREATED_DATE BETWEEN '2013-02-01' and '2013-02-28'
                                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb GROUP BY 1)
                                                AND unit_cost >0
                                                
                                                GROUP BY 1

                                                UNION 

                                                SEL accs_meth_id,  SUM(amount) AS subscription_rev
                                                FROM dp_vew.MEDIATED_CONFRMTN_EVENT_VEW
                                                WHERE confrmtn_event_start_dt BETWEEN '2013-02-01' and '2013-02-28'
                                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb GROUP BY 1)
                                                AND amount >0
                                                GROUP BY 1

                                ) a 
GROUP BY 1
)WITH DATA PRIMARY INDEX(accs_meth_id);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev3_Feb_Mar
AS
(
                                SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Feb_Mar
                                FROM 
                                ( 
                                                
                                                SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
                                                FROM dp_vew.SIP_SNAPSHOT
                                                WHERE action = 'Add' AND Status = 'Success' 
                                                AND CREATED_DATE BETWEEN  '2013-03-01' and '2013-03-31'
                                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb GROUP BY 1)
                                                AND unit_cost >0
                                                
                                                GROUP BY 1

                                                UNION 

                                                SEL accs_meth_id,  SUM(amount) AS subscription_rev
                                                FROM dp_vew.MEDIATED_CONFRMTN_EVENT_VEW
                                                WHERE confrmtn_event_start_dt BETWEEN   '2013-03-01' and '2013-03-31'
                                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb GROUP BY 1)
                                                AND amount >0
                                                GROUP BY 1

                                ) a 
GROUP BY 1
)WITH DATA PRIMARY INDEX(accs_meth_id);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev3_Feb_Apr
AS
(
                                SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Feb_Apr
                                FROM 
                                ( 
                                                
                                                SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
                                                FROM dp_vew.SIP_SNAPSHOT
                                                WHERE action = 'Add' AND Status = 'Success' 
                                                AND CREATED_DATE BETWEEN  '2013-04-01' and '2013-04-30'
                                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb GROUP BY 1)
                                                AND unit_cost >0
                                                
                                                GROUP BY 1

                                                UNION 

                                                SEL accs_meth_id,  SUM(amount) AS subscription_rev
                                                FROM dp_vew.MEDIATED_CONFRMTN_EVENT_VEW
                                                WHERE confrmtn_event_start_dt BETWEEN  '2013-04-01' and '2013-04-30'
                                                AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb GROUP BY 1)
                                                AND amount >0
                                                GROUP BY 1

                                ) a 
GROUP BY 1
)WITH DATA PRIMARY INDEX(accs_meth_id);

-------------------------------------------------
-------------------------------------------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev3_Feb AS
(
SEL a.accs_meth_id, Rev3_Feb_Feb/1.195 AS Rev3_Feb_Feb, Rev3_Feb_Mar/1.195 AS Rev3_Feb_Mar, Rev3_Feb_Apr/1.195 AS Rev3_Feb_Apr
FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb a
LEFT JOIN
dp_tmp_rep.AK_Rejoiner_Rev3_Feb_Feb b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Rejoiner_Rev3_Feb_Mar c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Rejoiner_Rev3_Feb_Apr d
ON a.accs_meth_id = d.accs_meth_id
)
WITH DATA PRIMARY INDEX(accs_meth_id);

--------------------------------------------------
--------------------------------------------------


-------------Surcharge Revenue---------------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev4_Feb_Feb
AS
(SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.07*1.195),0) AS Rev4_Feb_Feb
FROM dp_mdm_vew.subscriber_dly_recharge
WHERE recharge_dt BETWEEN  '2013-02-01' and '2013-02-28'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb)
GROUP BY 1
)
WITH DATA
PRIMARY INDEX(accs_meth_id);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev4_Feb_Mar
AS
(SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.07*1.195),0) AS Rev4_Feb_Mar
FROM dp_mdm_vew.subscriber_dly_recharge
WHERE recharge_dt BETWEEN '2013-03-01' and '2013-03-31'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb)
GROUP BY 1
)
WITH DATA
PRIMARY INDEX(accs_meth_id);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev4_Feb_Apr
AS
(SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.07*1.195),0) AS Rev4_Feb_Apr
FROM dp_mdm_vew.subscriber_dly_recharge
WHERE recharge_dt BETWEEN '2013-04-01' and '2013-04-30'
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb)
GROUP BY 1
)
WITH DATA
PRIMARY INDEX(accs_meth_id);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev4_Feb AS
(
SEL a.accs_meth_id, Rev4_Feb_Feb/1.195 AS Rev4_Feb_Feb,Rev4_Feb_Mar/1.195 AS Rev4_Feb_Mar, Rev4_Feb_Apr/1.195 AS Rev4_Feb_Apr
FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb a
LEFT JOIN
dp_tmp_rep.AK_Rejoiner_Rev4_Feb_Feb b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Rejoiner_Rev4_Feb_Mar c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN
dp_tmp_rep.AK_Rejoiner_Rev4_Feb_Apr d
ON a.accs_meth_id = d.accs_meth_id
)WITH DATA;

--------------Rejoiners ARPU-----------------------------------
drop table dp_tmp_rep.AK_Rejoiner_Rev_Feb;
CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev_Feb
AS
(
SEL a.accs_meth_id, Offering_Name, (COALESCE(Rev1_Feb_Feb,0)+COALESCE(Rev2_Feb_Feb,0)+COALESCE(Rev3_Feb_Feb,0)+COALESCE(Rev4_Feb_Feb,0)) AS Rev_Feb_Feb,
(COALESCE(Rev1_Feb_Mar,0)+COALESCE(Rev2_Feb_Mar,0)+COALESCE(Rev3_Feb_Mar,0)+COALESCE(Rev4_Feb_Mar,0)) AS Rev_Feb_Mar,
(COALESCE(Rev1_Feb_Apr,0)+COALESCE(Rev2_Feb_Apr,0)+COALESCE(Rev3_Feb_Apr,0)+COALESCE(Rev4_Feb_Apr,0)) AS Rev_Feb_Apr
FROM dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb a
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev1_Feb b
ON a.accs_meth_id = b.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev2_Feb c
ON a.accs_meth_id = c.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev3_Feb d
ON a.accs_meth_id = d.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Rev4_Feb e
ON a.accs_meth_id = e.accs_meth_id
LEFT JOIN dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb_PP f
ON a.accs_meth_id = f.accs_meth_id
GROUP BY 1,2,3,4,5
WHERE Rev1_Feb_Feb IS NOT NULL
OR Rev2_Feb_Feb IS NOT NULL
OR Rev3_Feb_Feb  IS NOT NULL
OR Rev4_Feb_Feb  IS NOT NULL
OR Rev1_Feb_Mar  IS NOT NULL
OR Rev2_Feb_Mar IS NOT NULL
OR Rev3_Feb_Mar IS NOT NULL
OR Rev4_Feb_Mar IS NOT NULL
OR Rev1_Feb_Apr IS NOT NULL
OR Rev2_Feb_Apr IS NOT NULL
OR Rev3_Feb_Apr IS NOT NULL
OR Rev4_Feb_Apr IS NOT NULL
)
WITH DATA PRIMARY INDEX (accs_meth_id);

----------------Rejoiners Monthly Revenue Bands---------------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Rev_Feb_Bands AS
(SEL Accs_meth_id, Offering_Name, SUM(Rev_Feb_Feb+Rev_Feb_Mar+Rev_Feb_Apr) AS Tot_Rev,
(CASE WHEN Tot_Rev/3 <= 0 THEN 'A: =0'
WHEN Tot_Rev/3 > 0 AND Tot_Rev/3 <= 5 THEN 'B: 1 - 5'
WHEN Tot_Rev/3 > 5 AND Tot_Rev/3 <= 10 THEN 'B: 6 - 10'
WHEN Tot_Rev/3 > 10 AND Tot_Rev/3 <= 15 THEN 'B: 11 - 15'
WHEN Tot_Rev/3 > 15 AND Tot_Rev/3 <= 20 THEN 'B: 16 - 20'
WHEN Tot_Rev/3 > 20 AND Tot_Rev/3 <= 50 THEN 'C: 21 - 50'
WHEN Tot_Rev/3 > 50 AND Tot_Rev/3 <= 100 THEN 'D: 51 - 100'
WHEN Tot_Rev/3 > 100 AND Tot_Rev/3 <= 200 THEN 'E: 101 - 200'
WHEN Tot_Rev/3 > 200 AND Tot_Rev/3 <= 500 THEN 'F: 201 - 500'
WHEN Tot_Rev/3 > 500 AND Tot_Rev/3 <= 1000 THEN 'G: 501 - 1000'
WHEN Tot_Rev/3 > 1000 THEN 'H: 1000+' END) AS Rev_Bands
FROM dp_tmp_rep.AK_Rejoiner_Rev_Feb
GROUP BY 1,2
) WITH DATA PRIMARY INDEX (accs_meth_id);

--============================================================================================================================================
-- final run query

SEL  offering_name AS Price_plan, Rev_bands ,COUNT(accs_meth_id) R, SUM(tot_rev) rev
FROM dp_tmp_rep.AK_Rejoiner_Rev_Feb_Bands
GROUP BY 1,2
ORDER BY 1,2

SEL  offering_name AS Price_plan, COUNT(accs_meth_id) R
FROM dp_tmp_rep.AK_Rejoiner_Rev_Feb_Bands
GROUP BY 1

SEL  Rev_bands, SUM(tot_rev) rev, COUNT(accs_meth_id) R
FROM dp_tmp_rep.AK_Rejoiner_Rev_Feb_Bands
GROUP BY 1


--============================================================================================================================================

--------------------Checking Rejoiners Cell Site Details--------Jan 2013-------------------------------------------------------------------------

DROP TABLE dp_tmp_rep.AK_Rejoiner_Citywise_Feb_Feb;
CREATE TABLE dp_tmp_rep.AK_Rejoiner_Citywise_Feb_Feb
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
WHERE CALL_START_DT BETWEEN '2013-02-01' and '2013-02-28'
AND OFFERING_ID IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb_PP)
GROUP BY 1,2,3,4,5,6
) AS A
LEFT JOIN dp_tmp_rep.AA_CELL_SITE AS B
ON A.CELL_SITE_ID=B.CELL_SITE_ID
AND A.LAC=B.LAC
GROUP BY 1,2,3,4
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Citywise_Feb_Mar
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
WHERE CALL_START_DT BETWEEN  '2013-03-01' and '2013-03-31'
AND OFFERING_ID IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb_PP)
GROUP BY 1,2,3,4,5,6
) AS A
LEFT JOIN dp_tmp_rep.AA_CELL_SITE AS B
ON A.CELL_SITE_ID=B.CELL_SITE_ID
AND A.LAC=B.LAC
GROUP BY 1,2,3,4
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_Citywise_Feb_Apr
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
WHERE CALL_START_DT BETWEEN '2013-04-01' and '2013-04-30'
AND OFFERING_ID IN (5,15,43,69,97,109,147,159,8,67,110,137,142,169)
AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb_PP)
GROUP BY 1,2,3,4,5,6
) AS A
LEFT JOIN dp_tmp_rep.AA_CELL_SITE AS B
ON A.CELL_SITE_ID=B.CELL_SITE_ID
AND A.LAC=B.LAC
GROUP BY 1,2,3,4
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID);

CREATE TABLE dp_tmp_rep.AK_Rejoiner_CW_Combine_Feb
AS
(
SEL * FROM dp_tmp_rep.AK_Rejoiner_Citywise_Feb_Feb
UNION ALL
SEL * FROM dp_tmp_rep.AK_Rejoiner_Citywise_Feb_Mar
UNION ALL
SEL * FROM dp_tmp_rep.AK_Rejoiner_Citywise_Feb_Apr
)
WITH DATA;

CREATE  TABLE dp_tmp_rep.AK_Rejoiner_CW_Subs_Feb
AS
(
SEL a.*
FROM
(
SEL Accs_Meth_Id ,District,SUM(TOTAL_COUNT) AS TOTAL_COUNT 
FROM dp_tmp_rep.AK_Rejoiner_CW_Combine_Feb
WHERE district IS NOT NULL
GROUP BY 1,2
) AS a
QUALIFY ROW_NUMBER() OVER(PARTITION BY Accs_Meth_Id  ORDER BY  TOTAL_COUNT  DESC)=1
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID,DISTRICT);

CREATE  TABLE dp_tmp_rep.AK_Rejoiner_CW_Feb
AS
(
SEL A.ACCS_METH_ID,COALESCE(B.DISTRICT,'UNKNOWN') AS DISTRICT
FROM  dp_tmp_rep.AK_Rejoiner_Spin_Down_2_Feb_PP AS A
LEFT JOIN dp_tmp_rep.AK_Rejoiner_CW_Subs_Feb AS B
ON A.ACCS_METH_ID=B.ACCS_METH_ID
GROUP BY 1,2
)WITH DATA
PRIMARY INDEX(ACCS_METH_ID,DISTRICT)


-- district wise final query
SEL district, COUNT (accs_meth_id) FROM dp_tmp_rep.AK_Rejoiner_CW_Feb
GROUP BY 1

-----------------------------------Other Requests-------------------------------------------

------------------------------------------Check for Poora Pakistan Offer Subscriptions for Rejoiners-------------------

CREATE TABLE dp_tmp_rep.AK_Rejoiner_PooraPak_Offer_Feb
AS
(
       SEL accs_meth_id, SUM(subscription_rev) AS Rev3_Feb
       FROM 
       ( 

                       
                       SEL  accs_meth_id,  SUM(unit_cost) AS subscription_rev
                       FROM dp_vew.SIP_SNAPSHOT
                       WHERE action = 'Add' AND Status = 'Success' 
                       AND CREATED_DATE BETWEEN '2012-01-01' AND '2013-04-30'
                       AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoiner_Rev_feb_Bands GROUP BY 1)
                       AND unit_cost >0
                       AND prod_id = 806
                       GROUP BY 1

                       UNION 

                       SEL accs_meth_id,  SUM(amount) AS subscription_rev
                       FROM dp_vew.MEDIATED_CONFRMTN_EVENT_VEW
                       WHERE confrmtn_event_start_dt BETWEEN '2012-01-01' AND '2013-04-30'
                       AND accs_meth_id IN (SELECT accs_meth_id FROM dp_tmp_rep.AK_Rejoiner_Rev_Feb_Bands GROUP BY 1)
                       AND amount >0
                       AND usage_type = 'AT_5_NonRecur'
                       GROUP BY 1

       ) a 
GROUP BY 1
)WITH DATA PRIMARY INDEX(accs_meth_id);

SEL COUNT(*) FROM dp_tmp_rep.AK_Rejoiner_PooraPak_Offer_Feb

------------------------------Rejoiners - Dormancy and Revenue Bands Cross Tabbing (CT)-----------------------
drop table dp_tmp_rep.AK_Rejoiners_Rev_Dorm_CT_Jan;
CREATE TABLE dp_tmp_rep.AK_Rejoiners_Rev_Dorm_CT_Feb
AS
(
SEL a.*, (CASE WHEN Days_Since_Last_Activity >= 90 AND Days_Since_Last_Activity <= 120 THEN 'A: 91-120'
WHEN Days_Since_Last_Activity > 120 AND Days_Since_Last_Activity <= 150 THEN 'B: 121-150'
WHEN Days_Since_Last_Activity > 150 AND Days_Since_Last_Activity <= 180 THEN 'C: 151-180'
WHEN Days_Since_Last_Activity > 180 AND Days_Since_Last_Activity <= 270 THEN 'D: 181-270'
WHEN Days_Since_Last_Activity > 270 AND Days_Since_Last_Activity <= 360 THEN 'E: 271-360'
WHEN Days_Since_Last_Activity > 360 AND Days_Since_Last_Activity <= 540 THEN 'F: 361-540'
WHEN Days_Since_Last_Activity > 540 AND Days_Since_Last_Activity <= 720 THEN 'G: 541-720'
WHEN Days_Since_Last_Activity > 720 THEN 'H: 720+' END) AS Dormancy_Bands
FROM dp_tmp_rep.AK_Rejoiner_Rev_Feb_Bands a
LEFT JOIN
dp_tmp_rep.AK_Rejoin_Dormancy_Band_Feb b
ON a.accs_meth_id = b.accs_meth_id
WHERE Days_Since_Last_Activity >= 90
)
WITH DATA;

SEL Dormancy_Bands, COUNT(CASE WHEN Rev_Bands = 'A: =0' THEN accs_meth_id END) AS "A: =0",
COUNT(CASE WHEN Rev_Bands = 'B: 1 - 5' THEN accs_meth_id END) AS "B: 1 - 5",
COUNT(CASE WHEN Rev_Bands = 'B: 6 - 10' THEN accs_meth_id END) AS "C: 6 - 10",
COUNT(CASE WHEN Rev_Bands = 'B: 11 - 15' THEN accs_meth_id END) AS "D: 11 - 15",
COUNT(CASE WHEN Rev_Bands = 'B: 16 - 20' THEN accs_meth_id END) AS "E: 16- 20",
COUNT(CASE WHEN Rev_Bands = 'C: 21 - 50' THEN accs_meth_id END) AS "F: 21 - 50",
COUNT(CASE WHEN Rev_Bands = 'D: 51 - 100' THEN accs_meth_id END) AS "G: 51 - 100",
COUNT(CASE WHEN Rev_Bands = 'E: 101 - 200' THEN accs_meth_id END) AS "H: 101 - 200",
COUNT(CASE WHEN Rev_Bands = 'F: 201 - 500' THEN accs_meth_id END) AS "I: 201 - 500",
COUNT(CASE WHEN Rev_Bands = 'G: 501 - 1000' THEN accs_meth_id END) AS "J: 501 - 1000",
COUNT(CASE WHEN Rev_Bands = 'H: 1000+' THEN accs_meth_id END) AS "K: 1000+"
FROM dp_tmp_rep.AK_Rejoiners_Rev_Dorm_CT_Feb
GROUP BY 1
ORDER BY 1

