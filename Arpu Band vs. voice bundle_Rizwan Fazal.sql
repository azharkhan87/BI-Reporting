
-- Arpu Band vs. voice bundle_Rizwan Fazal


sel * from dp_tmp_rep.aak_arpu_base
sel COUNT(distinct accs_meth_id) from dp_tmp_rep.aak_arpu_base
create table dp_tmp_rep.aak_arpu_base
as
(
	sel accs_meth_id,
	min(days_since_last_activity) DSLA
	
	from DP_VEW.subscriber_dormancy
	where DORMANCY_DATE = '2012-10-31'
	and DAYS_SINCE_LAST_ACTIVITY < 30
	
	group by 1
)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_arpu_oct_traf where accs_meth_id = 17407002
create table dp_tmp_rep.aak_arpu_oct_traf
as
(
	sel accs_meth_id,
	sum(Call_Gross_Revenue_Amt) rev
 
	from DP_VEW.call_hist_blc
	where Call_Start_Dt between '2012-10-01' and '2012-10-31'
	and ACCS_METH_ID in (sel Accs_Meth_Id from dp_tmp_Rep.aak_arpu_base group by 1)
	
	group by 1
)with data primary index(accs_meth_id)	

sel * from dp_tmp_REp.aak_arpu_oct_recharges  WHERE accs_meth_id = 17407002
create table dp_tmp_REp.aak_arpu_oct_recharges
as
(
	SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.07*1.195),0) as surcharge_rev
	FROM dp_mdm_vew.subscriber_dly_recharge
	WHERE recharge_dt BETWEEN '2012-10-01' AND '2012-10-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_base group by 1)
	GROUP BY 1

)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_arpu_oct_subscription where accs_meth_id = 17407002
drop table dp_tmp_rep.aak_arpu_oct_subscription;
create table dp_tmp_rep.aak_arpu_oct_subscription
as
(
	sel customer_Accs_Meth_Id as accs_meth_id,
	sum(Cost_Amt) subscription_rev
	
	from DP_MDM_VEW.subscriber_sip 
	where Event_Start_Dt between  '2012-10-01' AND '2012-10-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_base group by 1)
	GROUP BY 1

	union 
	
	sel Accs_Meth_Id,
	sum(amount) subscription_rev
	
	from DP_VEW.MEDIATED_CONFRMTN_EVENT_VEW
	where cast(Confrmtn_Event_Start_Dt  as date) between    '2012-10-01' AND '2012-10-31'
	and ACCS_METH_ID in (sel accs_meth_id from  DP_TMP_REP.aak_arpu_base group by 1)
	and usage_type <> 'siebel'
	group by 1
	
)with data primary index(Accs_meth_id)



sel * from dp_tmp_rep.aak_oct_voicebundle;
create table dp_tmp_rep.aak_oct_voicebundle
as
(

	sel Accs_Meth_Id,
	sum(voice_bundle_rev)voice_bundle_rev,
	sum(counts) voice_bundle_counts
	from
	(
	sel customer_Accs_Meth_Id as accs_meth_id,
	sum(Cost_Amt) voice_bundle_rev,
	count(*) counts
	
	from DP_MDM_VEW.subscriber_sip 
	where Event_Start_Dt between  '2012-10-01' AND '2012-10-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_base group by 1)
	AND PRODUCT_NAME in ('AN Voice Bundle 1',	'AN Voice Bundle 2',	'AN Voice Bundle 3',	'AN Voice Bundle 4',	'AN Voice Bundle 5',	'AN Voice Bundle 6',	'AN Voice Bundle 7',	'AT_5_NONRECUR',	'Buy Voice Offnet',	'Buy Voice Offnet (Offnet Bundle)',	'Buy Voice, offnet onnet',	'Buy Voice, offnet onnet_1',	'Buy Voice, offnet onnet_2',	'Buy Voice, offnet onnet_3',	'DJ_DIN_RAAT',	'DJ_DIN_RAAT',	'FIDC_TS24_LBC',	'FIDC_TS63',	'Flat Night 11',	'Flat Night 2-4-7-5-10',	'Flat Night 2-4-7-5-10 (10 Paisa Offer + Free Call Offer)',	'Flat Night 3',	'Flat Night 9',	'Free Intervals During Call',	'Free Intervals During Call (Boltey Jao Offer)',	'Free Intervals During Call2',	'Free Intervals During Call2 (30 Paisa Offer)',	'Free Intervals During Call2-3',	'Free Intervals During Call2-3 (30 Paisa Offer)',	'Karobar_Voice_Bundle1',	'Karobar_Voice_Bundle2',	'Karobar_Voice_Bundle3',	'Variable FN (5 Paisa Offer)',	'VFN Bonus',	'VFN Bonus (5 Paisa Offer)',	'Voice',	'Voice Bundle 1',	'Voice Bundle 2',	'Voice Bundle 3',	'Voice Bundle A',	'Voice Bundle B',	'Voice Bundle C',	'Voice Bundle1',	'Voice Bundle2',	'Voice Bundle3')
	GROUP BY 1

	union 
	
	sel Accs_Meth_Id,
	sum(amount) voice_bundle_rev,
	count(*) counts	
	from DP_VEW.MEDIATED_CONFRMTN_EVENT_VEW
	where cast(Confrmtn_Event_Start_Dt  as date) between    '2012-10-01' AND '2012-10-31'
	and ACCS_METH_ID in (sel accs_meth_id from  DP_TMP_REP.aak_arpu_base group by 1)
	and usage_type in ('AT_5_NONRECUR', 'BUY_MORE_VOICE_SUBS', 'Flat Night', 'Flat Night 10', 'Flat Night 11', 'Flat Night 2', 'Flat Night 3', 'Flat Night 4', 'Flat Night 5', 'Flat Night 9', 'Flat Night Campaign 7', 'FLAT_NIGHT_SUBS', 'Free Interval During Call', 'Free Intervals During Cal', 'Variable FN', 'Variable FN 1', 'Variable FN 2', 'Variable FN 3', 'Variable FN 4', 'Variable FN 5', 'IDD Flatnight', 'IDD Voice Bundle', 'Buy Voice Offnet', 'Buy Voice, 15offnet 15onn', 'Buy Voice, offnet onnet')
	and usage_type <> 'siebel'
	group by 1
	) a
	group by 1
)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_arpu_join where arpu = 0
drop table dp_tmp_rep.aak_arpu_join;
create table dp_tmp_rep.aak_arpu_join
as
(
	sel a.Accs_Meth_Id,
	coalesce(SUM(	rev), 0) Traffic_rev,
	coalesce(SUM(	surcharge_rev ),0) surcharge_rev,
	coalesce(SUM(	subscription_rev), 0) subscription_rev,
	SUM(	coalesce(	rev, 0)+ coalesce(	surcharge_rev , 0) + coalesce(subscription_rev, 0) ) ARPU
	
	from DP_TMP_REP.aak_arpu_base a
	left join DP_TMP_REP.aak_arpu_oct_traf b
	on a.accs_meth_id = b.Accs_Meth_Id
	
	left join dp_tmp_rep.aak_arpu_oct_recharges c
	on a.accs_meth_id = c.Accs_Meth_Id
		
	left join dp_tmp_rep.aak_arpu_oct_subscription d
	on a.accs_meth_id = d.Accs_Meth_Id
	
	group by 1
	
)with data primary index(accs_meth_id)

sel * from dp_tmp_Rep.aak_arpu_final
drop table dp_tmp_Rep.aak_arpu_final;
create table dp_tmp_Rep.aak_arpu_final
as
(
	sel 
	CASE when arpu = 0 then 'A: = 0'
	when arpu > 0 and arpu <=10 then 'B: 0-10'
	when arpu > 10 and arpu <= 30 then 'C: 10-30'
	when arpu > 30 and arpu <= 50 then 'D: 30-50'
	when arpu > 50 and arpu <= 70 then 'E: 50-70'
	when arpu > 70 and arpu <=100 then 'F: 70-100'
	when arpu >  100 and arpu <=150 then 'G: 100-150'
	when arpu > 150 and arpu <= 200 then 'H: 150-200'
	when arpu > 200 and arpu <= 250 then 'I: 200-250'
	when arpu > 250 and arpu <= 300 then 'J: 250-300'
	when arpu > 300 and arpu <= 350 then 'K: 300-350'
	when arpu > 350 and arpu <= 400 then 'L: 350-400'
	when arpu > 400 and arpu <= 450 then 'M: 400-450'
	when arpu > 450 and arpu <= 500 then 'N: 450-500'
	when arpu > 500 and arpu <= 600 then 'O: 500-600'
	when arpu > 600 and arpu <= 700 then 'P: 600-700'
	when arpu > 700 and arpu <= 800 then 'Q: 700-800'
	when arpu > 800 and arpu <= 900 then 'R: 800-900'
	when arpu > 900 and arpu <= 1000 then 'S: 900-1000'
	when arpu > 1000 then 'T: > 1000'
	END arpu_bands ,
	count(distinct A.accs_meth_id) dist_subs,
	count(distinct b.Accs_Meth_Id) voice_bundle_subs,
	sum (voice_bundle_counts) voice_bundle_counts
	
	from DP_TMP_REP.aak_arpu_join a
	left join DP_TMP_REP.aak_oct_voicebundle b
	on a.accs_meth_id = b.Accs_Meth_Id
	group by 1

)with data 

sel arpu_bandS, COUNT(distinct Accs_Meth_Id) subs from dp_tmp_Rep.aak_arpu_final2 group by 1
sel * from dp_tmp_Rep.aak_arpu_final2
create table dp_tmp_Rep.aak_arpu_final2
as
(
	sel a.Accs_Meth_Id, 
	CASE when arpu = 0 then 'A: = 0'
	when arpu > 0 and arpu <=10 then 'B: 0-10'
	when arpu > 10 and arpu <= 30 then 'C: 10-30'
	when arpu > 30 and arpu <= 50 then 'D: 30-50'
	when arpu > 50 and arpu <= 70 then 'E: 50-70'
	when arpu > 70 and arpu <=100 then 'F: 70-100'
	when arpu >  100 and arpu <=150 then 'G: 100-150'
	when arpu > 150 and arpu <= 200 then 'H: 150-200'
	when arpu > 200 and arpu <= 250 then 'I: 200-250'
	when arpu > 250 and arpu <= 300 then 'J: 250-300'
	when arpu > 300 and arpu <= 350 then 'K: 300-350'
	when arpu > 350 and arpu <= 400 then 'L: 350-400'
	when arpu > 400 and arpu <= 450 then 'M: 400-450'
	when arpu > 450 and arpu <= 500 then 'N: 450-500'
	when arpu > 500 and arpu <= 600 then 'O: 500-600'
	when arpu > 600 and arpu <= 700 then 'P: 600-700'
	when arpu > 700 and arpu <= 800 then 'Q: 700-800'
	when arpu > 800 and arpu <= 900 then 'R: 800-900'
	when arpu > 900 and arpu <= 1000 then 'S: 900-1000'
	when arpu > 1000 then 'T: > 1000'
	END arpu_bands ,
	count(distinct A.accs_meth_id) dist_subs	
	from DP_TMP_REP.aak_arpu_join a
	left join DP_TMP_REP.aak_oct_voicebundle b
	on a.accs_meth_id = b.Accs_Meth_Id
	group by 1,2

)with data primary index(accs_meth_id)


--\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


create table dp_tmp_rep.aak_arpu_july_traf
as
(
	sel accs_meth_id,
	sum(Call_Gross_Revenue_Amt) rev
 
	from DP_VEW.call_hist
	where Call_Start_Dt between '2013-07-01' and '2013-07-31'
	and ACCS_METH_ID in (sel Accs_Meth_Id from dp_tmp_Rep.aak_arpu_base group by 1)
	
	group by 1
)with data primary index(accs_meth_id)	

sel * from dp_tmp_REp.aak_arpu_july_recharges  WHERE accs_meth_id = 17407002
create table dp_tmp_REp.aak_arpu_july_recharges
as
(
	SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.07*1.195),0) as surcharge_rev
	FROM dp_mdm_vew.subscriber_dly_recharge
	WHERE recharge_dt BETWEEN  '2013-07-01' and '2013-07-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_base group by 1)
	GROUP BY 1

)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_arpu_july_subscription where accs_meth_id = 17407002
drop table dp_tmp_rep.aak_arpu_july_subscription;
create table dp_tmp_rep.aak_arpu_july_subscription
as
(
	sel customer_Accs_Meth_Id as accs_meth_id,
	sum(Cost_Amt) subscription_rev
	
	from DP_MDM_VEW.subscriber_sip 
	where Event_Start_Dt between  '2013-07-01' and '2013-07-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_base group by 1)
	GROUP BY 1

	union 
	
	sel Accs_Meth_Id,
	sum(amount) subscription_rev
	
	from DP_VEW.MEDIATED_CONFRMTN_EVENT_VEW
	where cast(Confrmtn_Event_Start_Dt  as date) between    '2013-07-01' and '2013-07-31'
	and ACCS_METH_ID in (sel accs_meth_id from  DP_TMP_REP.aak_arpu_base group by 1)
	and usage_type <> 'siebel'
	group by 1
	
)with data primary index(Accs_meth_id)



sel * from dp_tmp_rep.aak_july_voicebundle;
create table dp_tmp_rep.aak_july_voicebundle
as
(

	sel Accs_Meth_Id,
	sum(voice_bundle_rev)voice_bundle_rev,
	sum(counts) voice_bundle_counts
	from
	(
	sel customer_Accs_Meth_Id as accs_meth_id,
	sum(Cost_Amt) voice_bundle_rev,
	count(*) counts
	
	from DP_MDM_VEW.subscriber_sip 
	where Event_Start_Dt between   '2013-07-01' and '2013-07-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_base group by 1)
	AND PRODUCT_NAME in ('AN Voice Bundle 1',	'AN Voice Bundle 2',	'AN Voice Bundle 3',	'AN Voice Bundle 4',	'AN Voice Bundle 5',	'AN Voice Bundle 6',	'AN Voice Bundle 7',	'AT_5_NONRECUR',	'Buy Voice Offnet',	'Buy Voice Offnet (Offnet Bundle)',	'Buy Voice, offnet onnet',	'Buy Voice, offnet onnet_1',	'Buy Voice, offnet onnet_2',	'Buy Voice, offnet onnet_3',	'DJ_DIN_RAAT',	'DJ_DIN_RAAT',	'FIDC_TS24_LBC',	'FIDC_TS63',	'Flat Night 11',	'Flat Night 2-4-7-5-10',	'Flat Night 2-4-7-5-10 (10 Paisa Offer + Free Call Offer)',	'Flat Night 3',	'Flat Night 9',	'Free Intervals During Call',	'Free Intervals During Call (Boltey Jao Offer)',	'Free Intervals During Call2',	'Free Intervals During Call2 (30 Paisa Offer)',	'Free Intervals During Call2-3',	'Free Intervals During Call2-3 (30 Paisa Offer)',	'Karobar_Voice_Bundle1',	'Karobar_Voice_Bundle2',	'Karobar_Voice_Bundle3',	'Variable FN (5 Paisa Offer)',	'VFN Bonus',	'VFN Bonus (5 Paisa Offer)',	'Voice',	'Voice Bundle 1',	'Voice Bundle 2',	'Voice Bundle 3',	'Voice Bundle A',	'Voice Bundle B',	'Voice Bundle C',	'Voice Bundle1',	'Voice Bundle2',	'Voice Bundle3')
	GROUP BY 1

	union 
	
	sel Accs_Meth_Id,
	sum(amount) voice_bundle_rev,
	count(*) counts	
	from DP_VEW.MEDIATED_CONFRMTN_EVENT_VEW
	where cast(Confrmtn_Event_Start_Dt  as date) between   '2013-07-01' and '2013-07-31'
	and ACCS_METH_ID in (sel accs_meth_id from  DP_TMP_REP.aak_arpu_base group by 1)
	and usage_type in ('AT_5_NONRECUR', 'BUY_MORE_VOICE_SUBS', 'Flat Night', 'Flat Night 10', 'Flat Night 11', 'Flat Night 2', 'Flat Night 3', 'Flat Night 4', 'Flat Night 5', 'Flat Night 9', 'Flat Night Campaign 7', 'FLAT_NIGHT_SUBS', 'Free Interval During Call', 'Free Intervals During Cal', 'Variable FN', 'Variable FN 1', 'Variable FN 2', 'Variable FN 3', 'Variable FN 4', 'Variable FN 5', 'IDD Flatnight', 'IDD Voice Bundle', 'Buy Voice Offnet', 'Buy Voice, 15offnet 15onn', 'Buy Voice, offnet onnet')
	and usage_type <> 'siebel'
	group by 1
	) a
	group by 1
)with data primary index(accs_meth_id)


create table dp_tmp_rep.aak_arpu_july_union
as
(
	sel Accs_Meth_Id
	from DP_TMP_REP.aak_arpu_july_traf 
	group by 1	
	union
	sel Accs_Meth_Id
	from dp_tmp_rep.aak_arpu_july_recharges
	group by 1
	union
	sel Accs_Meth_Id	
	from dp_tmp_rep.aak_arpu_july_subscription
group by 1

)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_arpu_july_join
drop table dp_tmp_rep.aak_arpu_july_join;
create table dp_tmp_rep.aak_arpu_july_join
as
(
	sel a.Accs_Meth_Id,
	coalesce(SUM(	rev), 0) Traffic_rev,
	coalesce(SUM(	surcharge_rev ),0) surcharge_rev,
	coalesce(SUM(	subscription_rev), 0) subscription_rev,
	coalesce(SUM(	rev+ surcharge_rev + subscription_rev ) , 0) ARPU
	
	from DP_TMP_REP.aak_arpu_july_union a
	left join DP_TMP_REP.aak_arpu_july_traf b
	on a.accs_meth_id = b.Accs_Meth_Id
	
	left join dp_tmp_rep.aak_arpu_july_recharges c
	on a.accs_meth_id = c.Accs_Meth_Id
		
	left join dp_tmp_rep.aak_arpu_july_subscription d
	on a.accs_meth_id = d.Accs_Meth_Id	
	group by 1
	
)with data primary index(accs_meth_id)

/*sel * from dp_tmp_Rep.aak_arpu_july_join2
create table dp_tmp_Rep.aak_arpu_july_join2
as
(
	sel Accs_Meth_Id,
	CASE when arpu = 0 then 'A: = 0'
	when arpu > 0 and arpu <=10 then 'B: 0-10'
	when arpu > 10 and arpu <= 30 then 'C: 10-30'
	when arpu > 30 and arpu <= 50 then 'D: 30-50'
	when arpu > 50 and arpu <= 70 then 'E: 50-70'
	when arpu > 70 and arpu <=100 then 'F: 70-100'
	when arpu >  100 and arpu <=150 then 'G: 100-150'
	when arpu > 150 and arpu <= 200 then 'H: 150-200'
	when arpu > 200 and arpu <= 250 then 'I: 200-250'
	when arpu > 250 and arpu <= 300 then 'J: 250-300'
	when arpu > 300 and arpu <= 350 then 'K: 300-350'
	when arpu > 350 and arpu <= 400 then 'L: 350-400'
	when arpu > 400 and arpu <= 450 then 'M: 400-450'
	when arpu > 450 and arpu <= 500 then 'N: 450-500'
	when arpu > 500 and arpu <= 600 then 'O: 500-600'
	when arpu > 600 and arpu <= 700 then 'P: 600-700'
	when arpu > 700 and arpu <= 800 then 'Q: 700-800'
	when arpu > 800 and arpu <= 900 then 'R: 800-900'
	when arpu > 900 and arpu <= 1000 then 'S: 900-1000'
	when arpu > 1000 then 'T: > 1000'
	END arpu_bands_july 
	
	from DP_TMP_REP.aak_arpu_july_join
	group by 1,2

)with data primary index(accs_meth_id)
*/

SEL * FROM dp_tmp_Rep.aak_arpu_july_final
drop table dp_tmp_Rep.aak_arpu_july_final;
create table dp_tmp_Rep.aak_arpu_july_final
as
(
	sel 
	arpu_bands,
	CASE when arpu = 0 then 'A: = 0'
	when arpu > 0 and arpu <=10 then 'B: 0-10'
	when arpu > 10 and arpu <= 30 then 'C: 10-30'
	when arpu > 30 and arpu <= 50 then 'D: 30-50'
	when arpu > 50 and arpu <= 70 then 'E: 50-70'
	when arpu > 70 and arpu <=100 then 'F: 70-100'
	when arpu >  100 and arpu <=150 then 'G: 100-150'
	when arpu > 150 and arpu <= 200 then 'H: 150-200'
	when arpu > 200 and arpu <= 250 then 'I: 200-250'
	when arpu > 250 and arpu <= 300 then 'J: 250-300'
	when arpu > 300 and arpu <= 350 then 'K: 300-350'
	when arpu > 350 and arpu <= 400 then 'L: 350-400'
	when arpu > 400 and arpu <= 450 then 'M: 400-450'
	when arpu > 450 and arpu <= 500 then 'N: 450-500'
	when arpu > 500 and arpu <= 600 then 'O: 500-600'
	when arpu > 600 and arpu <= 700 then 'P: 600-700'
	when arpu > 700 and arpu <= 800 then 'Q: 700-800'
	when arpu > 800 and arpu <= 900 then 'R: 800-900'
	when arpu > 900 and arpu <= 1000 then 'S: 900-1000'
	when arpu > 1000 then 'T: > 1000'
	END arpu_bands_july ,
	
	
	count(distinct b.accs_meth_id) dist_subs
	
	from DP_TMP_REP.aak_arpu_final2 a
	left join DP_TMP_REP.aak_arpu_july_join b
	on a.accs_meth_id = b.Accs_Meth_Id
	group by 1,2

)with data 



--\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\



sel count(distinct Accs_Meth_Id) from dp_tmp_rep.aak_arpu_july2_base 
create table dp_tmp_rep.aak_arpu_july2_base
as
(
	sel accs_meth_id,
	min(days_since_last_activity) DSLA
	
	from DP_VEW.subscriber_dormancy
	where DORMANCY_DATE = '2013-07-31'
	and DAYS_SINCE_LAST_ACTIVITY < 30
	
	group by 1
)with data primary index(accs_meth_id)


sel * from dp_tmp_rep.aak_arpu_july2_traf where accs_meth_id = 17407002
drop table dp_tmp_rep.aak_arpu_july2_traf;
create table dp_tmp_rep.aak_arpu_july2_traf
as
(
	sel accs_meth_id,
	sum(Call_Gross_Revenue_Amt) rev
 
	from DP_VEW.call_hist_blc
	where Call_Start_Dt between  '2013-07-01' and '2013-07-31'
	and ACCS_METH_ID in (sel Accs_Meth_Id from dp_tmp_Rep.aak_arpu_july2_base group by 1)
	
	group by 1
)with data primary index(accs_meth_id)	

sel * from dp_tmp_REp.aak_arpu_july2_recharges  WHERE accs_meth_id = 17407002
drop table dp_tmp_REp.aak_arpu_july2_recharges;

create table dp_tmp_REp.aak_arpu_july2_recharges
as
(
	SEL accs_meth_id, COALESCE(SUM(dly_recharge_amt*0.10*1.195),0) as surcharge_rev
	FROM dp_mdm_vew.subscriber_dly_recharge
	WHERE recharge_dt BETWEEN  '2013-07-01' and '2013-07-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_july2_base group by 1)
	GROUP BY 1

)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_arpu_july2_subscription where accs_meth_id = 17407002
drop table dp_tmp_rep.aak_arpu_july2_subscription;
create table dp_tmp_rep.aak_arpu_july2_subscription
as
(
	sel customer_Accs_Meth_Id as accs_meth_id,
	sum(Cost_Amt) subscription_rev
	
	from DP_MDM_VEW.subscriber_sip 
	where Event_Start_Dt between   '2013-07-01' and '2013-07-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_july2_base group by 1)
	GROUP BY 1

	union 
	
	sel Accs_Meth_Id,
	sum(amount) subscription_rev
	
	from DP_VEW.MEDIATED_CONFRMTN_EVENT_VEW
	where cast(Confrmtn_Event_Start_Dt  as date) between     '2013-07-01' and '2013-07-31'
	and ACCS_METH_ID in (sel accs_meth_id from  DP_TMP_REP.aak_arpu_july2_base group by 1)
	and usage_type <> 'siebel'
	group by 1
	
)with data primary index(Accs_meth_id)


sel * from dp_tmp_rep.aak_july2_voicebundle;
create table dp_tmp_rep.aak_july2_voicebundle
as
(

	sel Accs_Meth_Id,
	sum(voice_bundle_rev)voice_bundle_rev,
	sum(counts) voice_bundle_counts
	from
	(
	sel customer_Accs_Meth_Id as accs_meth_id,
	sum(Cost_Amt) voice_bundle_rev,
	count(*) counts
	
	from DP_MDM_VEW.subscriber_sip 
	where Event_Start_Dt between   '2013-07-01' and '2013-07-31'
	AND accs_meth_id IN (SEL accs_meth_id FROM dp_tmp_rep.aak_arpu_july2_base group by 1)
	AND PRODUCT_NAME in ('AN Voice Bundle 1',	'AN Voice Bundle 2',	'AN Voice Bundle 3',	'AN Voice Bundle 4',	'AN Voice Bundle 5',	'AN Voice Bundle 6',	'AN Voice Bundle 7',	'AT_5_NONRECUR',	'Buy Voice Offnet',	'Buy Voice Offnet (Offnet Bundle)',	'Buy Voice, offnet onnet',	'Buy Voice, offnet onnet_1',	'Buy Voice, offnet onnet_2',	'Buy Voice, offnet onnet_3',	'DJ_DIN_RAAT',	'DJ_DIN_RAAT',	'FIDC_TS24_LBC',	'FIDC_TS63',	'Flat Night 11',	'Flat Night 2-4-7-5-10',	'Flat Night 2-4-7-5-10 (10 Paisa Offer + Free Call Offer)',	'Flat Night 3',	'Flat Night 9',	'Free Intervals During Call',	'Free Intervals During Call (Boltey Jao Offer)',	'Free Intervals During Call2',	'Free Intervals During Call2 (30 Paisa Offer)',	'Free Intervals During Call2-3',	'Free Intervals During Call2-3 (30 Paisa Offer)',	'Karobar_Voice_Bundle1',	'Karobar_Voice_Bundle2',	'Karobar_Voice_Bundle3',	'Variable FN (5 Paisa Offer)',	'VFN Bonus',	'VFN Bonus (5 Paisa Offer)',	'Voice',	'Voice Bundle 1',	'Voice Bundle 2',	'Voice Bundle 3',	'Voice Bundle A',	'Voice Bundle B',	'Voice Bundle C',	'Voice Bundle1',	'Voice Bundle2',	'Voice Bundle3')
	GROUP BY 1

	union 
	
	sel Accs_Meth_Id,
	sum(amount) voice_bundle_rev,
	count(*) counts	
	from DP_VEW.MEDIATED_CONFRMTN_EVENT_VEW
	where cast(Confrmtn_Event_Start_Dt  as date) between    '2013-07-01' and '2013-07-31'
	and ACCS_METH_ID in (sel accs_meth_id from  DP_TMP_REP.aak_arpu_july2_base group by 1)
	and usage_type in ('AT_5_NONRECUR', 'BUY_MORE_VOICE_SUBS', 'Flat Night', 'Flat Night 10', 'Flat Night 11', 'Flat Night 2', 'Flat Night 3', 'Flat Night 4', 'Flat Night 5', 'Flat Night 9', 'Flat Night Campaign 7', 'FLAT_NIGHT_SUBS', 'Free Interval During Call', 'Free Intervals During Cal', 'Variable FN', 'Variable FN 1', 'Variable FN 2', 'Variable FN 3', 'Variable FN 4', 'Variable FN 5', 'IDD Flatnight', 'IDD Voice Bundle', 'Buy Voice Offnet', 'Buy Voice, 15offnet 15onn', 'Buy Voice, offnet onnet')
	and usage_type <> 'siebel'
	group by 1
	) a
	group by 1
)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_arpu_july2_join where arpu = 0
drop table dp_tmp_rep.aak_arpu_july2_join;
create table dp_tmp_rep.aak_arpu_july2_join
as
(
	sel a.Accs_Meth_Id,
	coalesce(SUM(	rev), 0) Traffic_rev,
	coalesce(SUM(	surcharge_rev ),0) surcharge_rev,
	coalesce(SUM(	subscription_rev), 0) subscription_rev,
	SUM(	coalesce(	rev, 0)+ coalesce(	surcharge_rev , 0) + coalesce(subscription_rev, 0) ) ARPU
	
	from DP_TMP_REP.aak_arpu_july2_base a
	left join DP_TMP_REP.aak_arpu_july2_traf b
	on a.accs_meth_id = b.Accs_Meth_Id
	
	left join dp_tmp_rep.aak_arpu_july2_recharges c
	on a.accs_meth_id = c.Accs_Meth_Id
		
	left join dp_tmp_rep.aak_arpu_july2_subscription d
	on a.accs_meth_id = d.Accs_Meth_Id
	
	group by 1
	
)with data primary index(accs_meth_id)

sel * from dp_tmp_Rep.aak_arpu_july2_final
drop table dp_tmp_Rep.aak_arpu_july2_final;
create table dp_tmp_Rep.aak_arpu_july2_final
as
(
	sel 
	CASE when arpu = 0 then 'A: = 0'
	when arpu > 0 and arpu <=10 then 'B: 0-10'
	when arpu > 10 and arpu <= 30 then 'C: 10-30'
	when arpu > 30 and arpu <= 50 then 'D: 30-50'
	when arpu > 50 and arpu <= 70 then 'E: 50-70'
	when arpu > 70 and arpu <=100 then 'F: 70-100'
	when arpu >  100 and arpu <=150 then 'G: 100-150'
	when arpu > 150 and arpu <= 200 then 'H: 150-200'
	when arpu > 200 and arpu <= 250 then 'I: 200-250'
	when arpu > 250 and arpu <= 300 then 'J: 250-300'
	when arpu > 300 and arpu <= 350 then 'K: 300-350'
	when arpu > 350 and arpu <= 400 then 'L: 350-400'
	when arpu > 400 and arpu <= 450 then 'M: 400-450'
	when arpu > 450 and arpu <= 500 then 'N: 450-500'
	when arpu > 500 and arpu <= 600 then 'O: 500-600'
	when arpu > 600 and arpu <= 700 then 'P: 600-700'
	when arpu > 700 and arpu <= 800 then 'Q: 700-800'
	when arpu > 800 and arpu <= 900 then 'R: 800-900'
	when arpu > 900 and arpu <= 1000 then 'S: 900-1000'
	when arpu > 1000 then 'T: > 1000'
	END arpu_bands ,
	count(distinct A.accs_meth_id) dist_subs,
	count(distinct b.Accs_Meth_Id) voice_bundle_subs,
	sum (voice_bundle_counts) voice_bundle_counts
	
	from DP_TMP_REP.aak_arpu_july2_join a
	left join DP_TMP_REP.aak_july2_voicebundle b
	on a.accs_meth_id = b.Accs_Meth_Id
	group by 1

)with data 















	




sel top 10 * from DP_MDM_VEW.subscriber_sip where Event_Start_Dt = CURRENT_DATE - 1







	
	