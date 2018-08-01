

-- MOST USED CELL SITE LAT LONG FOR EPMA SUBS_OMAR ZEBIT

-- RETAILERS PART
drop table dp_tmp_rep.aak_epma_retailerMSISDN;
create multiset table dp_tmp_rep.aak_epma_retailerMSISDN
(
seller_cd varchar(100),
msisdn varchar (100)
)


insert INTO dp_tmp_rep.aak_epma_retailermsisdn values (?,?)


sel * from dp_tmp_rep.aak_epma_retailermsisdn

sel COUNT(distinct msisdn) from dp_tmp_rep.aak_epma_retailerMSISDN
sel COUNT(distinct accs_meth_id) from dp_tmp_rep.aak_epma_retailerBase
sel * from dp_tmp_rep.aak_epma_retailerBase
create table dp_tmp_rep.aak_epma_retailerBase
as
(

	sel b.accs_meth_id,
	msisdn,
	seller_cd
	
	from dp_tmp_rep.aak_epma_retailermsisdn a
	left join dp_vew.HLP_ACCESS_METHOD b
	on a.msisdn = b.ACCS_METH_NUM
	group by 1,2,3
)with data primary index(accs_meth_id)


-- most used cell site of all retailer numbers


sel * from dp_tmp_rep.aak_epma_cid_lac
drop table dp_tmp_rep.aak_epma_cid_lac;
create table dp_tmp_rep.aak_epma_cid_lac
as
(

sel Accs_Meth_Id,
cell_site_id,
lac,
Call_Network_Vol

from 
	(
	sel ACCS_METH_ID,
	cell_site_id,
	lac,
	sum(call_network_vol)call_network_vol
	from dp_vew.CALL_HIST
	where Accs_Meth_Id in (sel Accs_Meth_Id from dp_tmp_rep.aak_epma_retailerBase group by 1)
	and call_start_dt between '2013-01-01' and '2013-01-31'
	and Cell_Site_Id is not null and Lac is not null
	group by 1,2,3
	)a
	qualify ROW_NUMBER() OVER (partition by Accs_Meth_Id order by Call_Network_Vol desc)=1

)with data primary index(Accs_meth_id)

-- unique retailer base
sel  * from dp_tmp_rep.aak_epma_uniqueRetailer where Call_Network_Vol is not null
drop table dp_tmp_rep.aak_epma_uniqueRetailer;
create table dp_tmp_rep.aak_epma_uniqueRetailer
as
(

	sel a.accs_meth_id,
	msisdn,
	seller_cd,
	cell_site_id,
	lac,
	Call_Network_Vol
	from DP_tmp_rep.aak_epma_retailerbase  a
	left join dp_tmp_rep.aak_epma_lat_long b
	on a.accs_meth_id = b.ACCS_METH_ID
	
	where a.ACCS_METH_ID is not null
	qualify ROW_NUMBER() OVER (partition by seller_cd order by msisdn asc)=1

)with data primary index(accs_meth_id)


sel * from dp_tmp_rep.aak_epma_lat_long where cell_site_id is not null and latitude_rad is null
drop table dp_tmp_rep.aak_epma_lat_long;
create table dp_tmp_rep.aak_epma_lat_long
as
(
	sel a.accs_meth_id,
	msisdn,
	seller_cd,
	a.cell_site_id,
	a.lac,
	site_id,
	District,
	Call_Network_Vol,
	(lat*0.0174) as latitude_rad,
	(lng*0.0174) as longitude_rad
	
	
	from  dp_tmp_rep.aak_epma_uniqueRetailer a
	left join DP_VEW.CELL_SITE_HIST b
	on A.cell_site_id = b.Cell_Site_Id
	and A.lac = b.Lac
	and Cell_Site_End_Dt is null
	and lat is not null
	and lng is not null
	group by 1,2,3,4,5,6,7,8,9,10
	
)with data primary index(accs_meth_id)

sel * from dp_tmp_rep.aak_epma_cart_coord
create table dp_tmp_rep.aak_epma_cart_coord
as
(
	sel accs_meth_id,
	msisdn,
	seller_cd,
	cell_site_id,
	lac,
	site_id,
	District,
	Call_Network_Vol,
	latitude_rad,
	longitude_rad,
	(6371* COS(latitude_rad)*COS(Longitude_rad))	as X1,
	(6371* COS(latitude_rad)*SIN(Longitude_rad))	as Y1,
	(6371* SIN(latitude_rad))	as Z1
	
	from dp_tmp_rep.aak_epma_lat_long
	where latitude_rad is not null
	and longitude_rad is not null
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)with data primary index(accs_meth_id)

--================================================================================================================================

--SUBSCRIBERS PART

create multiset table dp_tmp_rep.aak_epma_subsMsisdn
(
msisdn varchar (100)

)

insert INTO dp_tmp_rep.aak_epma_subsMsisdn values (?)

sel * from dp_tmp_rep.aak_epma_Subsbase
create table dp_tmp_rep.aak_epma_Subsbase
as
(

	sel b.accs_meth_id,
	msisdn
	from dp_tmp_rep.aak_epma_subsMsisdn a
	left join dp_vew.HLP_ACCESS_METHOD b
	on a.msisdn = b.ACCS_METH_NUM
	group by 1,2
)with data primary index(accs_meth_id)


-- most used cell site of all retailer numbers


sel * from dp_tmp_rep.aak_epma_cid_lac2 where cell_site_id is null
drop table dp_tmp_rep.aak_epma_cid_lac2;
create table dp_tmp_rep.aak_epma_cid_lac2
as
(

sel Accs_Meth_Id,
cell_site_id,
lac,
Call_Network_Vol

from 
	(
	sel ACCS_METH_ID,
	cell_site_id,
	lac,
	sum(call_network_vol)call_network_vol
	from dp_vew.CALL_HIST
	where Accs_Meth_Id in (sel Accs_Meth_Id from dp_tmp_rep.aak_epma_Subsbase group by 1)
	and call_start_dt between '2013-01-01' and '2013-01-31'
	and Cell_Site_Id is not null and Lac is not null
	group by 1,2,3
	)a
	qualify ROW_NUMBER() OVER (partition by Accs_Meth_Id order by Call_Network_Vol desc)=1

)with data primary index(Accs_meth_id)




sel * from dp_tmp_rep.aak_epma_lat_long2 where cell_site_id is not null and latitude_rad is null
drop table dp_tmp_rep.aak_epma_lat_long2;
create table dp_tmp_rep.aak_epma_lat_long2
as
(
	sel a.accs_meth_id,
	a.cell_site_id,
	a.lac,
	site_id,
	District,
	Call_Network_Vol,
	(lat*0.0174) as latitude_rad,
	(lng*0.0174) as longitude_rad
	
	
	from  dp_tmp_rep.aak_epma_cid_lac2 a
	left join DP_VEW.CELL_SITE_HIST b
	on A.cell_site_id = b.Cell_Site_Id
	and A.lac = b.Lac
	and Cell_Site_End_Dt is null
	and lat is not null
	and lng is not null
	group by 1,2,3,4,5,6,7,8
	
)with data primary index(accs_meth_id)


drop table dp_tmp_rep.aak_epma_cart_coord2;
create table dp_tmp_rep.aak_epma_cart_coord2
as
(
	sel accs_meth_id,
	cell_site_id,
	lac,
	site_id,
	District,
	Call_Network_Vol,
	latitude_rad,
	longitude_rad,
	(6371* COS(latitude_rad)*COS(Longitude_rad))	as X2,
	(6371* COS(latitude_rad)*SIN(Longitude_rad))	as Y2,
	(6371* SIN(latitude_rad))	as Z2
	
	from dp_tmp_rep.aak_epma_lat_long2
	where latitude_rad is not null
	and longitude_rad is not null
	group by 1,2,3,4,5,6,7,8,9,10,11
)with data primary index(accs_meth_id)


--==============================================================================================================================


-- calculating distance between the points
sel * from dp_tmp_rep.aak_epma_distance
create table dp_tmp_rep.aak_epma_distance
as
(
	sel a.accs_meth_id as sub_accs_meth_id,
	b.accs_meth_id as retailer_accs_meth_id,
	b.msisdn as retailer_msisdn,
	b.seller_cd as retailer_seller_cd,
	
	a.cell_site_id as subs_cell_site_id,
	a.lac as subs_lac,
	a.site_id as subs_site_id,
	
	b.cell_site_id as retailer_cell_site_id,
	b.lac as retailer_lac,
	b.site_id as retailer_site_id,
	
	a.District as subs_district,
	b.District as retailer_district,
	
	a.Call_Network_Vol as subs_network_vol,
	b.Call_Network_Vol as retailer_network_vol,
	
	a.latitude_rad as subs_latitude,
	a.longitude_rad as subs_longitude,
	
	b.latitude_rad as retailer_latitude,
	b.longitude_rad as retailer_longitude,
	
	X2,
	Y2,
	Z2,
	X1,
	Y1,
	Z1,
	
	sqrt(((x1-x2)**2) + ((y1-y2)**2) + ((z1-z2)**2)) as distance
	
	from dp_tmp_rep.aak_epma_cart_coord2 a
	left join dp_tmp_rep.aak_epma_cart_coord b
	on 1=1
	group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
	
)with data primary index(sub_accs_meth_id)

sel * from dp_tmp_rep.aak_epma_min_dist
create table dp_tmp_rep.aak_epma_min_dist
as
(
	sel * 
	from dp_tmp_rep.aak_epma_distance
	qualify ROW_NUMBER() OVER (partition by sub_accs_meth_id order by distance asc)=1
	
)with data 

sel * from dp_tmp_Rep.aak_epma_final
create table dp_tmp_Rep.aak_epma_final
as
(
sel  b.accs_meth_num as subscriber_msisdn,
retailer_msisdn,
retailer_seller_cd,
subs_district,
retailer_district,
c.site_name as subscriber_site_name,
d.site_name as retailer_site_name

from dp_tmp_rep.aak_epma_min_dist a
left join DP_VEW.HLP_ACCESS_METHOD b
on A.sub_accs_meth_id = b.ACCS_METH_ID
left join dp_vew.HLP_SITE c
on a.subs_site_id = c.site_id

left join DP_VEW.HLP_SITE d
on a.retailer_site_id = d.Site_ID

group by 1,2,3,4,5,6,7
)with data primary index(subscriber_msisdn)


sel top 10 * from hlp_site




x = R * cos(lat) * cos(lon)

y = R * cos(lat) * sin(lon)

z = R *sin(lat)


sel  * from cell_site_hist where Cell_Site_End_Dt is null and Lat is not null and lng is not null


sel  * from DP_VEW.mkt_geo_hier where 

tan sin cos
atan asin acos



