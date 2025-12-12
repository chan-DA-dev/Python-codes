%include "/amex2/ramp/misc/other_gms/nshet15/AWB/code/configuration_M19.sas";

proc printto log="/amex2/ramp/misc/other_gms/nshet15/AWB/logs/M19_V4.log" NEW;run;
proc printto print="/amex2/ramp/misc/other_gms/nshet15/AWB/logs/M19_V4.lst" NEW;run; ods listing close;run;

libname ramp_in "/amex2/ramp/misc/sms_msa/Shared/AWB_M19";
Libname temp "/amex2/ramp/misc/other_gms/nshet15/AWB/temp_data";

options compress=yes;
options obs= max;


DATA _NULL_;
V_R3_MONTH = PUT(INTNX('MONTH',TODAY(),-3,'END'),YYMMn6.);
V_R6_MONTH = PUT(INTNX('MONTH',today(),-6,'END'),YYMMn6.);
CALL SYMPUTX("r3_month", V_R3_MONTH);
CALL SYMPUTX("r6_month", V_R6_MONTH);
RUN;

%PUT The value of R3_MONTH is &r3_month.;
%PUT The value of R6_MONTH is &r6_month.;

DATA _NULL_;
ACPT_STA_UPDT_DT = "'"||put(intnx('MONTH',today(),-3,'END'),DATE9.)||"'D";
call symputx("ACPT_STA_UPDT_DT", ACPT_STA_UPDT_DT);
put ACPT_STA_UPDT_DT=;
RUN;

%let work_area=%sysfunc(pathname(work));
data TEMP.GMAR_GLBL_MER_DIM;
set gmwdm.GLBL_MER_DIM(keep= mer_id mer_no MER_SRCE_SYS_CD MER_DBA_NM MER_LGL_NM MER_HIER_LVL_NO SEIMS_ACCT_CLASS_4_CD SEIMS_ACCT_CLASS_6_CD SEIMS_ACCT_CLASS_8_CD
MER_CTY_CD DMA_CD PHYS_AD_PSTL_AREA_CD SEIMS_INDUS_CD MER_SETUP_DT MER_ORIG_CD BRND_TOC_MER_IN MER_STA_CD MER_ACT_DT MER_CANC_RSN_CD MER_CANC_DT PHONE_NO CREAT_TS
PHYS_AD_LINE_1_TX PHYS_AD_LINE_2_TX PHYS_AD_LINE_3_TX PRIM_CLNT_MGR_FULL_NM PRIM_CLNT_MGMT_DIV_NM PRIM_LGCY_SALE_SRVC_ID MER_REIN_DT MER_REIN_RSN_CD SRVC_BUS_CTR_CD 
SRVC_BUS_CTR_NM SRVC_SALE_PERS_ID PHYS_AD_POST_TOWN_NM PHYS_AD_RGN_AREA_CD SCF_CD SMSA_CD MCC_INDUS_CD BRND_TOC_MER_ID BRND_TOC_MER_NO BRND_TOC_MER_SRCE_SYS_CD BASE_DISC_RT 
CNTRCT_DISC_RT SUBM_CPBL_TYPE_1_IN srvc_prvd_cd mer_tenure_cd
 YBUNLOAD=YES
 YB_JAVA_HOME="/usr"
 BL_YB_PATH="/usr/lib/ybtools/bin/ybunload"
 BL_LOGFILE="&work_area./GMAR_GLBL_MER_DIM.log"
 BL_DATAFILE="&work_area./GMAR_GLBL_MER_DIM.dat"
 BL_DELETE_DATAFILE=YES);
where MER_CTY_CD in ('840','850','630');
run;

proc sql;
create table AWB_OUT.crmd_ind_base as
  select trimN(se10) as SE_NO length=10,
         trimN(se_crmd_prim_level3_ctgy_cd) as crmc length=4,
         trimN(se_crmd_prim_vrfy_indus_cd) as sic8 length=8,
                 trimN(se_crmd_mer_onln_offln_cd) as MER_ONLN_OFFLN_CD length=1
  from RAMPYB.GMS_MERCHANT_CRMD_ANALYTICAL;
quit;

proc sql;
create table crt_postal_geo_summary as
  select
  trimN(post_cd) as post_cd length=100,
  trimN(fk_mkt_areamkt_are) as DMA_MKT_AREA_ID length=100
  from RAMPCS.crt_postal_geo_summary
  ;
quit;

/* GCL- B2B Decommission Aug2021 till here*/

proc sql;
create table AWB_OUT.dma_mapping as 
select distinct dma_ds as DMA_DS_TX, dma_cd as DMA_MKT_AREA_ID 
from gmwdmpds.GLBL_MER_RSLT_EOM_SNAPSHOT
(YBUNLOAD=YES
 YB_JAVA_HOME="/usr"
 BL_YB_PATH="/usr/lib/ybtools/bin/ybunload"
 BL_LOGFILE="&work_area./glbl_mer_rslt_eom_snapshot.log"
 BL_DATAFILE="&work_area./glbl_mer_rslt_eom_snapshot.dat"
 BL_DELETE_DATAFILE=YES)
where trim(mer_ctry_cd)='840'
and trim(dma_ds)<>'' and trim(dma_cd)<>'';
quit; 

PROC SQL;
create table temp.all_mer_temp1 as
select  a.*, b.MKT_TOC_MER_NO as managed_toc ,b.GCG_IN 
from temp.GMAR_GLBL_MER_DIM as a
left outer join GMWDMPDS.GLBL_MER_RSLT_EOM_SNAPSHOT as b on
a.mer_id = b.mer_id
where a.MER_CTY_CD in ('840','850','630')
      and (a.MER_STA_CD in ("A","R") or (a.MER_STA_CD="C" and YEAR(a.MER_CANC_DT)>=2011))
      and compress(a.mer_no)>'' and compress(a.mer_no)<>'0';
QUIT;
 
proc sort data = TEMP.ALL_MER_TEMP1;
  by mer_no descending MER_SRCE_SYS_CD;
run;

proc sort data = TEMP.ALL_MER_TEMP1 nodupkey;
  by mer_no ;
run;

proc sql;
 create table TEMP.all_mer_temp2 as
	select 
			case when b.mer_id is not null then b.MER_ID else 0 end as mer_id,
			case when b.mer_no is not null then b.mer_no end as se_no, 
			case when b.mer_no is null then 0 /*MMDB Only */
			end as MMDB_MATCH, 
			b.MER_SRCE_SYS_CD as MER_SRCE_SYS_CD,
			b.MER_DBA_NM as DBA_NM,
			b.MER_LGL_NM as Legal_NM,
			case when b.MER_HIER_LVL_NO=2 then "S"
				 when b.MER_HIER_LVL_NO=6 then "C"
				 when b.MER_HIER_LVL_NO=8 then "M"
				 end as mer_hier_lvl_no,
			b.SEIMS_ACCT_CLASS_4_CD as ACCT_class_4_CD,
			b.SEIMS_ACCT_CLASS_6_CD as ACCT_class_6_CD,
			b.SEIMS_ACCT_CLASS_8_CD as ACCT_class_8_CD,
			b.MER_CTY_CD as PHYS_AD_CTRY_CD,
			b.DMA_CD as DMA_MKT_AREA_ID,
			b.PHYS_AD_PSTL_AREA_CD as PHYS_AD_POST_CD_TX,
			b.SEIMS_INDUS_CD as SEIMS_INDUS_DS_CD,
			b.MER_SETUP_DT as MER_SETUP_DT,
			b.MER_ORIG_CD as MER_ACCT_ORIG_CD,
			b.BRND_TOC_MER_IN as TOP_OF_CHAIN_IN,
			b.MER_STA_CD as CUR_MER_STA_CD,
			
			b.mer_tenure_cd as MER_TENURE_MO_CT,
			
			b.MER_ACT_DT as MER_ACT_DT,
			b.MER_CANC_RSN_CD as CUR_MER_CANC_CD,
			b.MER_CANC_DT as CUR_MER_CANC_DT,
			b.PHONE_NO as PHONE_NO,

			b.CREAT_TS,

			b.PHYS_AD_LINE_1_TX,
			b.PHYS_AD_LINE_2_TX,
			b.PHYS_AD_LINE_3_TX,
			b.PRIM_CLNT_MGR_FULL_NM,
			b.PRIM_CLNT_MGMT_DIV_NM,
			PRIM_LGCY_SALE_SRVC_ID,
			b.MER_REIN_DT,
			b.MER_REIN_RSN_CD,
			b.SRVC_BUS_CTR_CD, 
			b.SRVC_BUS_CTR_NM,
			b.SRVC_SALE_PERS_ID,
			b.PHYS_AD_POST_TOWN_NM as city_nm,
			b.PHYS_AD_RGN_AREA_CD,
			b.SCF_CD,
			b.SMSA_CD,
			b.MCC_indus_CD,
			b.BRND_TOC_MER_ID,
			b.BRND_TOC_MER_NO,
			b.BRND_TOC_MER_SRCE_SYS_CD,
			b.BASE_DISC_RT,
			b.CNTRCT_DISC_RT,
			b.SUBM_CPBL_TYPE_1_IN,
			b.managed_toc,
			b.GCG_IN ,  
			b.srvc_prvd_cd
	from 	TEMP.ALL_MER_TEMP1 as b
	;
quit;  

proc sql;
 create table temp.temp1 as 
	select se_no, count(*) from temp.all_mer_temp2 group by 1; 
/*10,680,120*/
 create table temp.temp3 as 
	select BRND_TOC_MER_NO as aff_toc, count(*) from temp.all_mer_temp2 group by 1; 
 /*3,879,726*/
quit;

PROC SQL;
 create table TEMP.Missing_TOCs as
	select 	distinct aff_toc 
	from 	TEMP.temp3
	where 	aff_toc not in (select se_no from TEMP.temp1) ;
quit; /*433*/;

PROC SQL;
 create table TEMP.GMAR_MISSING_TOCs as 
    select  b.mer_no as se_no,
            b.mer_id,
            4 as MMDB_MATCH, /*MISSING TOCs Inserted*/
            b.MER_SRCE_SYS_CD,
            b.MER_DBA_NM as DBA_NM,
            b.MER_LGL_NM as Legal_NM,
            Case when b.MER_HIER_LVL_NO=2 then "S"
                when b.MER_HIER_LVL_NO=6 then "C"
                when b.MER_HIER_LVL_NO=8 then "M"
                else put(b.MER_HIER_LVL_NO, ? 1.)
            end as MER_HIER_LVL_NO  ,
            b.SEIMS_ACCT_CLASS_4_CD as ACCT_class_4_CD,
            b.SEIMS_ACCT_CLASS_6_CD as ACCT_class_6_CD,
            b.SEIMS_ACCT_CLASS_8_CD as ACCT_class_8_CD,
            b.MER_CTY_CD as PHYS_AD_CTRY_CD,
            b.DMA_CD as DMA_MKT_AREA_ID,
            b.PHYS_AD_PSTL_AREA_CD as PHYS_AD_POST_CD_TX, 
            b.SEIMS_INDUS_CD as SEIMS_INDUS_DS_CD,
            b.MER_SETUP_DT as MER_SETUP_DT,
            b.MER_ORIG_CD as MER_ACCT_ORIG_CD,
            b.BRND_TOC_MER_IN as TOP_OF_CHAIN_IN,
            b.MER_STA_CD as CUR_MER_STA_CD,
            b.MER_ACT_DT as MER_ACT_DT,
            b.MER_CANC_RSN_CD as CUR_MER_CANC_CD,
            b.MER_CANC_DT as CUR_MER_CANC_DT,
            b.PHONE_NO,
            b.CREAT_TS,
            b.PHYS_AD_LINE_1_TX,
            b.PHYS_AD_LINE_2_TX,
            b.PHYS_AD_LINE_3_TX,
            b.PRIM_CLNT_MGR_FULL_NM,
/*            b.PRIM_CLNT_MGMT_DIV_NM,*/
			C.PRIM_CLNT_MGMT_DIV_NM,
            b.MER_REIN_DT,
            b.MER_REIN_RSN_CD,
            b.SRVC_BUS_CTR_CD,
            b.SRVC_BUS_CTR_NM,
            b.SRVC_SALE_PERS_ID,
            b.PHYS_AD_RGN_AREA_CD,
            b.SCF_CD,
            b.SMSA_CD,
            b.MCC_INDUS_CD,
            b.BRND_TOC_MER_ID,
            b.BRND_TOC_MER_NO,
            b.BRND_TOC_MER_SRCE_SYS_CD,
            0 as DISC_FULL_RT,
            b.BASE_DISC_RT,
            b.CNTRCT_DISC_RT,
            c.MKT_TOC_MER_NO as managed_toc,
			c.GCG_IN , /*GCG Reconciliation*/
            b.srvc_prvd_cd
    from    TEMP.GMAR_GLBL_MER_DIM as b
    left outer join GMWDMPDS.GLBL_MER_RSLT_EOM_SNAPSHOT as c 
    on b.mer_id=c.mer_id 
    where   b.mer_no in (select * from TEMP.Missing_TOCs where compress(aff_toc)>'')
			and b.MER_CTY_CD in ('840','850','630');
quit; /*433*/


proc sort data = TEMP.GMAR_MISSING_TOCs;
  by se_no descending MER_SRCE_SYS_CD ;
run;

proc sort data= TEMP.GMAR_MISSING_TOCs nodupkey;
by se_no ; /*432*/

data TEMP.ALL_MER_TEMP;
  merge TEMP.all_mer_temp2 (in=a ) TEMP.GMAR_MISSING_TOCs (in=b);
    by se_no;

run; 

/*PROC DATASETS LIB=WORK NOLIST;*/
/*DELETE ALL_MER_TEMP1 ALL_MER_TEMP2 MISSING_TOCS GMAR_MISSING_TOCs TEMP1 TEMP3 GMAR_GLBL_MER_DIM;*/
/*RUN;*/

PROC SQL;
 create table temp.all_mer_out as
	select 
			a.*,
			month(CUR_MER_CANC_DT) as CANCEL_MONTH,
			YEAR(CUR_MER_CANC_DT) as CANCEL_YEAR,
			month(MER_SETUP_DT) as OPEN_MONTH,
			YEAR(MER_SETUP_DT) as OPEN_YEAR,
			(case when CUR_MER_STA_CD ='A' then 1
					when CUR_MER_STA_CD ='R' and MER_REIN_DT<=&month_end. then 1 
					when CUR_MER_STA_CD ='C' and CUR_MER_CANC_DT>&month_end. then 1
					else 0 end) as active_ind,			
			case when MER_TENURE_MO_CT >= 0 and MER_TENURE_MO_CT <=11 then "<1yr"
				 when MER_TENURE_MO_CT >= 12 and MER_TENURE_MO_CT <=23 then "1-2yrs"
				 when MER_TENURE_MO_CT >= 24 and MER_TENURE_MO_CT <=35 then "2-3yrs"
				 when MER_TENURE_MO_CT >= 36then "3yrs+"
			end as mer_tenure_per, 
			case when SEIMS_INDUS_DS_CD in ('591' , '682' , '900' , '654' , '665' ,
						'666' , '668' , '669' , '672' , '673' , '674' , '678' ,
   						 '680' , '681' , '683' , '699' , '710' , '725' , '922' , 
						'927' , '931' , '932' , '946' , '646' ,
						 '648' , '687' , '696' , '865' , '872' , '880' ,
						'630' , '644' , '645' , '647' , '649' , '659' ,
						 '660' , '661' , '689' , '694' , '866' , '656' ,
						'657' , '658' , '688' , '869' , '916' , '947' ,
						 '677' , '945' , '971' , '605' , '608' , '642' ,
						'664' , '667' , '684' , '735' , '911' , 'A01' ,
						 '640' , '641' , '653' , '873' , '877' , '349' , 
						'486' , '521' , '662' , '663' , '686' , '690' ,
						 '691' , '692' , '693' , '695' , '861' , '862' ,
						'863' , '864' , '867' , '868' , '870' , '871' ,
						 '875' , '876' , '878' , '879' , '881' , '883' , '948' )
				then 1 else 0 end as B2B_ind, /* renamed from b2b_ind_old */
			'PROP' as portfolio_type, 
			case when GCG_IN ='Y' then "GCG"
           		 when PRIM_CLNT_MGMT_DIV_NM contains 'GCG' then "GCG"
				 when PRIM_CLNT_MGMT_DIV_NM contains 'US NATIONAL' then "NCG"
				 when PRIM_CLNT_MGMT_DIV_NM contains 'US REGIONAL' then "RCG"
				 when PRIM_CLNT_MGMT_DIV_NM contains ('US SMALL MERCHANTS CCLM') then "CCLM"
				 when PRIM_CLNT_MGMT_DIV_NM contains ('US SMALL MERCHANTS AGGREGATORS') then "Small Merchants Aggregators"
				 when compress(ACCT_class_8_CD) >' ' and substr(ACCT_class_8_CD,1,2) <> '10' then "OTH_SM AMEX Internal"
				 when compress(MER_SRCE_SYS_CD) <> "USD" or PHYS_AD_CTRY_CD not in ('001','840') then "OTH_SM INTL"				
				 else "OTH_SM"
			     end as partner_typ,
		  Case  when GCG_IN ='Y' then "GCG"
			    when PRIM_CLNT_MGMT_DIV_NM contains 'GCG' then "GCG"
				when PRIM_CLNT_MGMT_DIV_NM contains 'US NATIONAL' then "National"
				when PRIM_CLNT_MGMT_DIV_NM contains 'US REGIONAL' then "Regional"
				else 'Small Merchants' end as segment,
			"Prop" as SE_TYPE FORMAT $12. length 12,
			CASE WHEN compress(a.BRND_TOC_MER_NO)<'' or a.BRND_TOC_MER_NO is null then a.se_no 
				 else a.BRND_TOC_MER_NO end as AFF_TOC	
	from 	temp.ALL_MER_TEMP as a 
;
QUIT; 


/*PROC DATASETS LIB=WORK NOLIST;*/
/*DELETE  ALL_MER_TEMP;*/
/*RUN;*/

PROC SQL;
	create index se_no on temp.all_mer_out(se_no);/*10,680,552*/
quit;	
	/* cv part for the traditional merchants */

proc sql;
 create table temp.GLBL_CV as  
	select 	mer_id, 
			SRC_STM_ID,
			mo_id, 
			sum(tot_bsn_usd_vol) as tot_bsn_usd_vol, 
			sum(db_roc_ct) as db_roc_ct,
			sum(net_roc_ct) as net_roc_ct,
			sum(cr_roc_ct) as cr_roc_ct,
			sum(dcn_rev_usd_am) as dcn_rev_usd_am
	from	GMWDM.V_MER_GLBL_SUBM_MO_F_ALL(keep = mer_id src_stm_id mo_id tot_bsn_usd_vol db_roc_ct net_roc_ct cr_roc_ct dcn_rev_usd_am) 
	where	MO_ID >= 201101
	group by mer_id, SRC_STM_ID,mo_id
	order by mer_id, SRC_STM_ID,mo_id
	;
quit;

PROC SQL;
Create table TEMP.ALL_MER_CV as
Select a.*,
	sum(case when mo_id >=&yr_ly3.01 and mo_id <= &yr_ly3.12 then tot_bsn_usd_vol else 0 end) as ly3_cv_fy,
	sum(case when mo_id >=&yr_ly2.01 and mo_id <= &yr_ly2.12 then tot_bsn_usd_vol else 0 end) as ly2_cv_fy,
	sum(case when mo_id >=&yr_ly.01 and mo_id <= &yr_ly.12 then tot_bsn_usd_vol else 0 end) as ly_cv_fy,
	sum(case when mo_id =&last_r12_month. then tot_bsn_usd_vol else 0 end ) as LY_CV_CurM,
	sum(case when mo_id >=(&cur_year_begin.-100) and mo_id <=(&cur_month.-100) then tot_bsn_usd_vol else 0 end) as LY_CV_YTD,		
	
	/*	Added by Purnima - R3/R6 variable*/

	sum(case when mo_id >=(&r12_month.-100) and mo_id <=(&cur_month.-100) then tot_bsn_usd_vol else 0 end) as LY_CV_R12,
	sum(case when mo_id >=(&r3_month.-100) and mo_id <=(&cur_month.-100) then tot_bsn_usd_vol else 0 end) as LY_CV_R3,
	sum(case when mo_id >=(&r6_month.-100) and mo_id <=(&cur_month.-100) then tot_bsn_usd_vol else 0 end) as LY_CV_R6,

	sum(case when mo_id =&cur_month. then tot_bsn_usd_vol else 0 end ) as CV_CurM,
	sum(case when mo_id >=&cur_year_begin. and mo_id <=&cur_month. then tot_bsn_usd_vol else 0 end) as CV_YTD,

	/*	Added by Purnima - R3/R6 variable*/

	sum(case when mo_id >=&r12_month. and mo_id <=&cur_month. then tot_bsn_usd_vol else 0 end) as CV_R12,
	sum(case when mo_id >=&r3_month. and mo_id <=&cur_month. then tot_bsn_usd_vol else 0 end) as CV_R3,
	sum(case when mo_id >=&r6_month. and mo_id <=&cur_month. then tot_bsn_usd_vol else 0 end) as CV_R6,

	sum(case when mo_id >=&yr_ly3.01 and mo_id <= &yr_ly3.12 then net_roc_ct else 0 end) as ly3_roc_fy,
	sum(case when mo_id >=&yr_ly2.01 and mo_id <= &yr_ly2.12 then net_roc_ct else 0 end) as ly2_roc_fy,
	sum(case when mo_id >=&yr_ly.01 and mo_id <= &yr_ly.12 then net_roc_ct else 0 end) as ly_roc_fy,
	sum(case when mo_id =&last_r12_month. then net_roc_ct else 0 end ) as LY_roc_CurM,
	sum(case when mo_id >=(&cur_year_begin.-100) and mo_id <=(&cur_month.-100) then net_roc_ct else 0 end) as LY_ROC_YTD,

	/*	Added by Purnima - R3/R6 variable*/

	sum(case when mo_id >=(&r12_month.-100) and mo_id <= (&cur_month.-100) then net_roc_ct else 0 end) as LY_ROC_R12,
	sum(case when mo_id >=(&r3_month.-100) and mo_id <= (&cur_month.-100) then net_roc_ct else 0 end) as LY_ROC_R3,
	sum(case when mo_id >=(&r6_month.-100) and mo_id <= (&cur_month.-100) then net_roc_ct else 0 end) as LY_ROC_R6,

	sum(case when mo_id =&cur_month. then net_roc_ct else 0 end ) as ROC_CurM,
	sum(case when mo_id >=&cur_year_begin. and mo_id <=&cur_month. then net_roc_ct else 0 end) as ROC_YTD,

	/*	Added by Purnima - R3/R6 variable*/

	sum(case when mo_id >=&r12_month. and mo_id <=&cur_month. then net_roc_ct else 0 end) as ROC_R12,
	sum(case when mo_id >=&r3_month. and mo_id <=&cur_month. then net_roc_ct else 0 end) as ROC_R3,
	sum(case when mo_id >=&r6_month. and mo_id <=&cur_month. then net_roc_ct else 0 end) as ROC_R6,

	sum(case when mo_id >=&r12_month. and mo_id <=&cur_month. then db_roc_ct else 0 end) as DBROCS_R12,
	sum(case when mo_id =&cur_month. then DCN_REV_USD_AM else 0 end ) as DCN_REV_CurM,
	sum(case when mo_id =&last_r12_month. then DCN_REV_USD_AM else 0 end ) as DCN_REV_LY_CurM,
	sum(case when mo_id >=&cur_year_begin. and mo_id <=&cur_month. then DCN_REV_USD_AM else 0 end) as DCN_REV_YTD,
	sum(case when mo_id >=&r12_month. and mo_id <=&cur_month. then DCN_REV_USD_AM else 0 end) as DCN_REV_R12,
	sum(case when mo_id >=(&r12_month.-100) and mo_id <= (&cur_month.-100) then DCN_REV_USD_AM else 0 end) as LY_DCN_REV_R12,
	sum(case when mo_id >=(&cur_year_begin.-100) and mo_id <=(&cur_month.-100) then DCN_REV_USD_AM else 0 end) as LY_DCN_REV_YTD


	from TEMP.all_mer_out as a left join TEMP.glbl_cv as b on
	a.mer_id=b.mer_id group by a.mer_id;
	quit;

/*PROC DATASETS LIB=WORK NOLIST;*/
/*DELETE glbl_cv;*/
/*RUN;*/

data TEMP.all_mer_cv1;
set TEMP.all_mer_cv;
if DBROCS_R12 >= 1 and CV_R12 >= 0.000005 then cv_active_ind = 1;
else cv_active_ind = 0;
if cv_r12<=0 then cur_cv_r12_band='01.<=0       	';
else if cv_r12>0 and cv_r12 < 50 then  cur_cv_r12_band='02.$1-<$50   	';
else if cv_r12 >= 50 and cv_r12 <250 then 	cur_cv_r12_band='03.$50-<250 	';
else if cv_r12 >= 250 and cv_r12 <500 then 	cur_cv_r12_band='04.$250-<500 	';
else if cv_r12 >= 500 and cv_r12 <1000 then 	cur_cv_r12_band='05.$500-<1K  	';
else if cv_r12 >= 1000 and cv_r12 <2000 then  cur_cv_r12_band='06.$1K-<2K   	';
else if cv_r12 >= 2000 and cv_r12 <3000 then  cur_cv_r12_band='07.$2K-<3K   	';
else if cv_r12 >= 3000 and cv_r12 <5000 then  cur_cv_r12_band='08.$3K-<5K 	';
else if cv_r12 >= 5000 and cv_r12 <10000 then	cur_cv_r12_band='09.$5K-<10K 	';
else if cv_r12 >= 10000 and cv_r12 < 15000 then  	cur_cv_r12_band='10.$10K-<15K   ';
else if cv_r12 >= 15000 and cv_r12 < 25000 then  	cur_cv_r12_band='11.$15K-<25K   ';
else if cv_r12 >= 25000 and cv_r12 < 50000 then  	cur_cv_r12_band='12.$25K-<50K   ';
else if cv_r12 >= 50000 and cv_r12 < 100000 then  	cur_cv_r12_band='13.$50K-<100K  ';
else if cv_r12 >= 100000 and cv_r12 < 250000 then 	cur_cv_r12_band='14.$100K-<250K ';
else if cv_r12 >= 250000 and cv_r12 < 500000 then 	cur_cv_r12_band='15.$250K-<500K ';
else if cv_r12 >= 500000 and cv_r12 < 750000 then 	cur_cv_r12_band='16.$500K-<750K ';
else if cv_r12 >= 750000 and cv_r12 < 1000000 then  cur_cv_r12_band='17.$750K-<1MM  ';
else if cv_r12 >= 1000000 and cv_r12 < 2000000 then cur_cv_r12_band='18.$1MM-<2MM   ';
else if cv_r12 >= 2000000 and cv_r12 < 3000000 then 	cur_cv_r12_band='19.$2MM-<3MM   ';
else if cv_r12 >= 3000000 and cv_r12 < 10000000 then 	cur_cv_r12_band='20.$3MM-<10MM  ';
else if cv_r12 >= 10000000 then cur_cv_r12_band='21.$10MM+      ' ;

if cv_r3<=0 then cur_cv_r3_band='01.<=0       	';
else if cv_r3>0 and cv_r3 < 50 then  cur_cv_r3_band='02.$1-<$50   	';
else if cv_r3 >= 50 and cv_r3 <250 then 	cur_cv_r3_band='03.$50-<250 	';
else if cv_r3 >= 250 and cv_r3 <500 then 	cur_cv_r3_band='04.$250-<500 	';
else if cv_r3 >= 500 and cv_r3 <1000 then 	cur_cv_r3_band='05.$500-<1K  	';
else if cv_r3 >= 1000 and cv_r3 <2000 then  cur_cv_r3_band='06.$1K-<2K   	';
else if cv_r3 >= 2000 and cv_r3 <3000 then  cur_cv_r3_band='07.$2K-<3K   	';
else if cv_r3 >= 3000 and cv_r3 <5000 then  cur_cv_r3_band='08.$3K-<5K 	';
else if cv_r3 >= 5000 and cv_r3 <10000 then	cur_cv_r3_band='09.$5K-<10K 	';
else if cv_r3 >= 10000 and cv_r3 < 15000 then  	cur_cv_r3_band='10.$10K-<15K   ';
else if cv_r3 >= 15000 and cv_r3 < 25000 then  	cur_cv_r3_band='11.$15K-<25K   ';
else if cv_r3 >= 25000 and cv_r3 < 50000 then  	cur_cv_r3_band='12.$25K-<50K   ';
else if cv_r3 >= 50000 and cv_r3 < 100000 then  	cur_cv_r3_band='13.$50K-<100K  ';
else if cv_r3 >= 100000 and cv_r3 < 250000 then 	cur_cv_r3_band='14.$100K-<250K ';
else if cv_r3 >= 250000 and cv_r3 < 500000 then 	cur_cv_r3_band='15.$250K-<500K ';
else if cv_r3 >= 500000 and cv_r3 < 750000 then 	cur_cv_r3_band='16.$500K-<750K ';
else if cv_r3 >= 750000 and cv_r3 < 1000000 then  cur_cv_r3_band='17.$750K-<1MM  ';
else if cv_r3 >= 1000000 and cv_r3 < 2000000 then cur_cv_r3_band='18.$1MM-<2MM   ';
else if cv_r3 >= 2000000 and cv_r3 < 3000000 then 	cur_cv_r3_band='19.$2MM-<3MM   ';
else if cv_r3 >= 3000000 and cv_r3 < 10000000 then 	cur_cv_r3_band='20.$3MM-<10MM  ';
else if cv_r3 >= 10000000 then cur_cv_r3_band='21.$10MM+      ' ;

if cv_r6<=0 then cur_cv_r6_band='01.<=0       	';
else if cv_r6>0 and cv_r6 < 50 then  cur_cv_r6_band='02.$1-<$50   	';
else if cv_r6 >= 50 and cv_r6 <250 then 	cur_cv_r6_band='03.$50-<250 	';
else if cv_r6 >= 250 and cv_r6 <500 then 	cur_cv_r6_band='04.$250-<500 	';
else if cv_r6 >= 500 and cv_r6 <1000 then 	cur_cv_r6_band='05.$500-<1K  	';
else if cv_r6 >= 1000 and cv_r6 <2000 then  cur_cv_r6_band='06.$1K-<2K   	';
else if cv_r6 >= 2000 and cv_r6 <3000 then  cur_cv_r6_band='07.$2K-<3K   	';
else if cv_r6 >= 3000 and cv_r6 <5000 then  cur_cv_r6_band='08.$3K-<5K 	';
else if cv_r6 >= 5000 and cv_r6 <10000 then	cur_cv_r6_band='09.$5K-<10K 	';
else if cv_r6 >= 10000 and cv_r6 < 15000 then  	cur_cv_r6_band='10.$10K-<15K   ';
else if cv_r6 >= 15000 and cv_r6 < 25000 then  	cur_cv_r6_band='11.$15K-<25K   ';
else if cv_r6 >= 25000 and cv_r6 < 50000 then  	cur_cv_r6_band='12.$25K-<50K   ';
else if cv_r6 >= 50000 and cv_r6 < 100000 then  	cur_cv_r6_band='13.$50K-<100K  ';
else if cv_r6 >= 100000 and cv_r6 < 250000 then 	cur_cv_r6_band='14.$100K-<250K ';
else if cv_r6 >= 250000 and cv_r6 < 500000 then 	cur_cv_r6_band='15.$250K-<500K ';
else if cv_r6 >= 500000 and cv_r6 < 750000 then 	cur_cv_r6_band='16.$500K-<750K ';
else if cv_r6 >= 750000 and cv_r6 < 1000000 then  cur_cv_r6_band='17.$750K-<1MM  ';
else if cv_r6 >= 1000000 and cv_r6 < 2000000 then cur_cv_r6_band='18.$1MM-<2MM   ';
else if cv_r6 >= 2000000 and cv_r6 < 3000000 then 	cur_cv_r6_band='19.$2MM-<3MM   ';
else if cv_r6 >= 3000000 and cv_r6 < 10000000 then 	cur_cv_r6_band='20.$3MM-<10MM  ';
else if cv_r6 >= 10000000 then cur_cv_r6_band='21.$10MM+      ' ;

run;


proc sort data=temp.all_mer_cv1 out=awb_out.all_mer_cv1 noduprecs;
by mer_id;
run;

/**************       OPTBLUE DATA GENERATION ****************/
/**************       OPTBLUE.SELLER_CHAR     ****************/


proc sort data=OPTMETA.SELLER_CHAR out=AWB_OUT.ob_char_temp 
(where=(tpa_se_no NOT IN ('3343897567','2044727287','1049092174','2044439040','4452822323','3042881599','4105150783','')
                               and sell_ctry_cd in ('US','PR','VI'))) 
nodupkey ;
by SUBM_SE_NO SELL_ID;
run;


proc sql; 
create table temp.ob_char_temp as 
	select 	m.*,			
			DATEPART(SRCE_CREAT_TS) 		 as create_dt format date9.,
			DATEPART(LST_SPNSR_MER_UPDT_TS)  as status_dt format date9.,
			DATEPART(m.SRCE_CREAT_TS) 		 as sign_date format date9.,
			DATEPART(m.SRCE_CREAT_TS)  	     as open_date format date9.
	from 	awb_out.ob_char_temp as m;
quit;

PROC SORT DATA=TEMP.OB_CHAR_TEMP; BY SUBM_SE_NO SELL_ID; RUN;

PROC SQL;	 /* This step errored out*/
CREATE TABLE TEMP.QUERY_FOR_SPNSR_MER_LINK AS
SELECT T1.NEW_SUBM_NTRL_MER_ID AS NEW_SUBM_SE_NO 
, T1.NEW_SELL_ID
, T1.ORIG_SUBM_NTRL_MER_ID AS ORIG_SUBM_SE_NO
, T1.orig_sell_id
, T1.CHNG_RSN_CD 
, T2.SIGN_DATE AS ORIG_SIGN_DATE
, T2.INIT_SUBM_DT AS ORIG_INIT_SUBM_DT
FROM OPTMETA.SPNSR_MER_LINK T1 
left join TEMP.OB_CHAR_TEMP  T2 
on trim(T1.ORIG_SUBM_NTRL_MER_ID) = trim(T2.subm_se_no) and trim(T1.ORIG_SELL_ID) = trim(T2.SELL_ID)
where trim(T1.SELL_CHNG_STA_CD) = 'C';
QUIT;


PROC SORT DATA=TEMP.QUERY_FOR_SPNSR_MER_LINK; BY ORIG_SUBM_SE_NO ORIG_SELL_ID NEW_SUBM_SE_NO NEW_SELL_ID; RUN; 


/*Formatting the Submission SE variables to Format Char 10.*/

DATA AWB_OUT.CROSS_XREF_TABLE(DROP=NEW_SUBM_SE_NO1 ORIG_SUBM_SE_NO1);
 SET TEMP.QUERY_FOR_SPNSR_MER_LINK(RENAME=(NEW_SUBM_SE_NO=NEW_SUBM_SE_NO1 ORIG_SUBM_SE_NO=ORIG_SUBM_SE_NO1));
 LENGTH NEW_SUBM_SE_NO ORIG_SUBM_SE_NO $ 10.;
 INFORMAT NEW_SUBM_SE_NO ORIG_SUBM_SE_NO $CHAR10.;
 FORMAT NEW_SUBM_SE_NO ORIG_SUBM_SE_NO $CHAR10.;
   NEW_SUBM_SE_NO  = NEW_SUBM_SE_NO1;
   ORIG_SUBM_SE_NO = ORIG_SUBM_SE_NO1;
RUN;

PROC SORT DATA=AWB_OUT.CROSS_XREF_TABLE NODUPKEY; BY ORIG_SUBM_SE_NO ORIG_SELL_ID; RUN;

/* Portfolio Change - amish1 2017/10/14
 Refreshing the open_date, close_date, init_subm_dt for new SE
	and update the seller_acpt_sta_cd and seller_acpt_sta_updt_dt for original SE*/


PROC SORT DATA=AWB_OUT.CROSS_XREF_TABLE OUT=TEMP.OLD_PRTR_INFO(KEEP=ORIG_SUBM_SE_NO ORIG_SELL_ID NEW_SUBM_SE_NO NEW_SELL_ID CHNG_RSN_CD
                                                           RENAME=(ORIG_SUBM_SE_NO=SUBM_SE_NO ORIG_SELL_ID=SELL_ID)); 
            BY ORIG_SUBM_SE_NO ORIG_SELL_ID; RUN;

PROC SORT DATA=AWB_OUT.CROSS_XREF_TABLE OUT=TEMP.NEW_PRTR_INFO(KEEP=NEW_SUBM_SE_NO NEW_SELL_ID ORIG_SIGN_DATE ORIG_INIT_SUBM_DT
                                                           RENAME=(NEW_SUBM_SE_NO=SUBM_SE_NO NEW_SELL_ID=SELL_ID)); 
             BY NEW_SUBM_SE_NO NEW_SELL_ID; RUN;


DATA TEMP.NEW_OB_CHAR_TEMP(DROP=NEW_SUBM_SE_NO NEW_SELL_ID CHNG_RSN_CD ORIG_SIGN_DATE ORIG_INIT_SUBM_DT);
 MERGE TEMP.OB_CHAR_TEMP     (IN=A)
       TEMP.OLD_PRTR_INFO     (IN=B)
	   TEMP.NEW_PRTR_INFO     (IN=C)
       ;
       BY SUBM_SE_NO SELL_ID;

	   IF A THEN DO; 
	     IF B THEN DO; /* If record is present in table A and table B then do the below */
		   SELL_ACPT_STA_CD=CHNG_RSN_CD;
		   IF MISSING(SELL_ACPT_STA_UPDT_DT) THEN SELL_ACPT_STA_UPDT_DT = &ACPT_STA_UPDT_DT;
		 END;
		 IF C THEN DO;  /* If record is present in table A and table C then do the below */
		   SIGN_DATE=ORIG_SIGN_DATE;
		   OPEN_DATE=ORIG_SIGN_DATE;
		   IF NOT MISSING(ORIG_INIT_SUBM_DT) THEN INIT_SUBM_DT = ORIG_INIT_SUBM_DT;
		 END;
		 OUTPUT;/* Rest of the records of table A write as is*/
		END;
RUN;


PROC SQL;
Create table TEMP.ob_char_temp as
Select a.*,	year(open_date) as open_year,
			month(open_date) as open_month
  FROM TEMP.NEW_OB_CHAR_TEMP AS A;
QUIT;

proc sql;
 create table AWB_OUT.seller_temp as
	select 
			case when b.sell_id is not null then b.sell_id else '' end as mer_id,
			b.sell_se_no as se_no, 
			b.subm_se_no,
			b.payee_se_no,
			b.tpa_se_no,
			b.mtch_tpa_se_no,
			b.mtch_sell_id,
			b.mtch_sell_subm_se_no,
			b.srce_sys_id as MER_SRCE_SYS_CD,
			b.sell_dba_nm as DBA_NM,
			b.sell_lgl_nm as Legal_NM,
			b.sell_city_nm as city_nm,
			b.REL_GRP_TYPE_NO, 
			b.REL_GRP_TYPE_UPDT_DT,
			b.rel_subgrp_type_cd,
		
		    CASE WHEN B.SELL_CTRY_CD = 'US' THEN '840'
			WHEN B.SELL_CTRY_CD = 'CA' THEN '124'
			WHEN B.SELL_CTRY_CD = 'MX' THEN '484'
			WHEN B.SELL_CTRY_CD = 'PR' THEN '630'
			WHEN B.SELL_CTRY_CD = 'VI' THEN '850'
			END AS PHYS_AD_CTRY_CD,
			b.sell_pstl_CD as PHYS_AD_POST_CD_TX,
			b.SEIMS_INDUS_DS_CD as SEIMS_INDUS_DS_CD,
			b.open_date as MER_SETUP_DT format date9.,
			b.init_subm_dt as MER_ACT_DT format date9.,
			b.SELL_BUS_PHONE_NO as PHONE_NO,
			
			b.SELL_MER_CTGY_CD as MCC_INDUS_CD,
			b.SELL_RGN_AREA_CD as PHYS_AD_RGN_AREA_CD,
			b.SELL_ST_AD_LINE_1_TX as AD_LINE_1_TX,
			b.SELL_ST_AD_LINE_2_TX as AD_LINE_2_TX,
			b.SELL_ST_AD_LINE_3_TX as AD_LINE_3_TX,
			b.SELL_EMAIL_AD_TX as EMAIL_AD_TX,
			b.CREAT_TS,
			b.SRCE_CREAT_TS,
			b.SELL_URL_TX,
			b.INIT_REC_SRCE_CD,
			b.ISO_REGIS_NO,
			b.MKT_ELIG_CD,
			b.BUS_CTR_CD as SRVC_BUS_CTR_CD,
			b.LANG_PREF_CD,
			b.sale_chan_cd,
			b.sale_chan_nm,
			b.subm_type_cd,
			b.SALE_REPR_ID,

			b.SELL_ACPT_STA_UPDT_DT,
			b.SELL_ACPT_STA_CD as cur_mer_sta_cd,
			b.SELL_ACPT_STA_CD,
			b.bank_card_act_in,
			b.JCB_ACPT_IN,
			b.SGFNT_OWN_FIRST_NM,
			b.SGFNT_OWN_LST_NM,
			b.sign_Date  /*Added Dec'17 - Canada Portfolio Changes*/
	from 	TEMP.ob_char_temp as b
	;
quit;



PROC SQL;
Create table AWB_OUT.seller_temp1 as
Select a.*, 
			(case when PHYS_AD_CTRY_CD in ('840') then 'SELL' else 'SELL_INTL' end) as se_type FORMAT $12. length 12,
			year(mer_setup_dt) as open_year,
			month(mer_setup_dt) as open_month,
			year(MER_ACT_DT) as SUBM_YEAR,
			month(MER_ACT_DT) as SUBM_MONTH,
			intck('month',mer_setup_dt,&month_end.) as mer_tenure_mo_ct,
	        (case when MER_ACT_DT <>. and MER_ACT_DT<= &month_end. then intck('month',mer_setup_dt,MER_ACT_DT) else 9999 end) as activated_per
from AWB_OUT.seller_temp as a;
	
QUIT;

data AWB_OUT.SELLER_MO_FIN_SMRY;
set OPTMETA.SELLER_MO_FIN_SMRY
(YBUNLOAD=YES
 YB_JAVA_HOME="/usr"
 BL_YB_PATH="/usr/lib/ybtools/bin/ybunload"
 BL_LOGFILE="&work_area./SELLER_MO_FIN_SMRY.log"
 BL_DATAFILE="&work_area./SELLER_MO_FIN_SMRY.dat"
 BL_DELETE_DATAFILE=YES);
run;

/*Updating Previous Partner's SE_NO & SUBM_SE_NO with New Partner's SE_NO & SUBM_SE_NO*/

PROC SORT DATA=AWB_OUT.SELLER_MO_FIN_SMRY; BY SUBM_SE_NO SELL_ID; RUN;



DATA AWB_OUT.NEW_SELLER_MO_FIN_SMRY(DROP=NEW_SUBM_SE_NO NEW_SELL_ID);
 MERGE AWB_OUT.SELLER_MO_FIN_SMRY  (IN=A)
       TEMP.OLD_PRTR_INFO       (IN=B DROP=CHNG_RSN_CD)
	   ;
       BY SUBM_SE_NO SELL_ID;
	   IF A THEN DO;
	    IF B THEN DO;
		  SUBM_SE_NO = NEW_SUBM_SE_NO;
		  SELL_ID    = NEW_SELL_ID;
        END;
		OUTPUT;
	   END;
RUN;

PROC SORT DATA=AWB_OUT.NEW_SELLER_MO_FIN_SMRY; BY SUBM_SE_NO SELL_ID; RUN;
	

proc sql;
create table AWB_OUT.seller_cv_smry as
select a.*, 
min(case when CHRG_VOL_NET_USD_AM>0.000005 and CHRG_VOL_DR_TRANS_CT>=1 then mo_id end) as first_cv_mo_id,
sum(case when mo_id>=&yr_ly3.01 and mo_id<=&yr_ly3.12 then CHRG_VOL_NET_USD_AM else 0 end) as ly3_cv_fy,
sum(case when mo_id>=&yr_ly2.01 and mo_id<=&yr_ly2.12 then CHRG_VOL_NET_USD_AM else 0 end) as ly2_cv_fy,
sum(case when mo_id>=&yr_ly.01 and mo_id<=&yr_ly.12 then CHRG_VOL_NET_USD_AM else 0 end) as ly_cv_fy,
sum(case when MO_ID = &last_r12_month. then CHRG_VOL_NET_USD_AM else 0 end ) as LY_CV_CurM,
sum(case when mo_id>=(&cur_year_begin.-100) and mo_id <=(&cur_month.-100) then CHRG_VOL_NET_USD_AM else 0 end) as LY_CV_YTD,

sum(case when mo_id>=(&r12_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_NET_USD_AM else 0 end) as LY_CV_R12,
sum(case when mo_id>=(&r3_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_NET_USD_AM else 0 end) as LY_CV_R3,
sum(case when mo_id>=(&r6_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_NET_USD_AM else 0 end) as LY_CV_R6,

sum(Case when MO_ID = &cur_month. then CHRG_VOL_NET_USD_AM else 0 end) as CV_curm,
sum(case when mo_id>=&cur_year_begin. and mo_id<=&cur_month. then CHRG_VOL_NET_USD_AM else 0 end) as CV_YTD,

sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then CHRG_VOL_NET_USD_AM else 0 end) as CV_R12,
sum(case when mo_id>=&r3_month. and mo_id<=&cur_month. then CHRG_VOL_NET_USD_AM else 0 end) as CV_R3,
sum(case when mo_id>=&r6_month. and mo_id<=&cur_month. then CHRG_VOL_NET_USD_AM else 0 end) as CV_R6,

sum(case when mo_id>=&yr_ly3.01 and mo_id<=&yr_ly3.12 then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_ly3,
sum(case when mo_id>=&yr_ly3.01 and mo_id<=&yr_ly3.12 then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_ly3,
sum(case when mo_id>=&yr_ly2.01 and mo_id<=&yr_ly2.12 then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_ly2,
sum(case when mo_id>=&yr_ly2.01 and mo_id<=&yr_ly2.12 then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_ly2,
sum(case when mo_id>=&yr_ly.01 and mo_id<=&yr_ly.12 then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_ly,
sum(case when mo_id>=&yr_ly.01 and mo_id<=&yr_ly.12 then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_ly,
sum(Case when MO_ID = &last_r12_month. then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_LY_CurM,
sum(Case when MO_ID = &last_r12_month. then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_LY_CurM,
sum(case when mo_id>=(&cur_year_begin.-100) and mo_id <=(&cur_month.-100) then CHRG_VOL_CR_TRANS_CT else 0 end) as YOY_CR_ROC_YTD,
sum(case when mo_id>=(&cur_year_begin.-100) and mo_id <=(&cur_month.-100) then CHRG_VOL_DR_TRANS_CT else 0 end) as YOY_DR_ROC_YTD,

sum(case when mo_id>=(&r12_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_CR_TRANS_CT else 0 end) as YOY_CR_ROC_R12,
sum(case when mo_id>=(&r6_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_CR_TRANS_CT else 0 end) as YOY_CR_ROC_R6,
sum(case when mo_id>=(&r3_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_CR_TRANS_CT else 0 end) as YOY_CR_ROC_R3,

sum(case when mo_id>=(&r12_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_DR_TRANS_CT else 0 end) as YOY_DR_ROC_R12,
sum(case when mo_id>=(&r6_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_DR_TRANS_CT else 0 end) as YOY_DR_ROC_R6,
sum(case when mo_id>=(&r3_month.-100) and mo_id<=(&cur_month.-100) then CHRG_VOL_DR_TRANS_CT else 0 end) as YOY_DR_ROC_R3,

sum(Case when MO_ID = &cur_month. then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_curm,
sum(Case when MO_ID = &cur_month. then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_curm,
sum(case when mo_id>=&cur_year_begin. and mo_id<=&cur_month. then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_YTD,
sum(case when mo_id>=&cur_year_begin. and mo_id<=&cur_month. then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_YTD,

sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_R12,
sum(case when mo_id>=&r3_month. and mo_id<=&cur_month. then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_R3,
sum(case when mo_id>=&r6_month. and mo_id<=&cur_month. then CHRG_VOL_CR_TRANS_CT else 0 end) as CR_ROC_R6,
sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_R12,
sum(case when mo_id>=&r3_month. and mo_id<=&cur_month. then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_R3,
sum(case when mo_id>=&r6_month. and mo_id<=&cur_month. then CHRG_VOL_DR_TRANS_CT else 0 end) as DR_ROC_R6,

sum(Case when MO_ID = &cur_month. then DISC_USD_AM else 0 end) as DCN_REV_curM,
sum(Case when MO_ID = &last_r12_month. then DISC_USD_AM else 0 end) as DCN_REV_LY_CurM,
sum(case when mo_id>=&cur_year_begin. and mo_id<=&cur_month. then DISC_USD_AM else 0 end) as DCN_REV_YTD,
sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then DISC_USD_AM else 0 end) as DCN_REV_R12,
sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then CHRGBK_CT else 0 end) as CHRGBK_CT_R12,
sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then CHRGBK_USD_AM else 0 end) as CHRGBK_USD_AM_R12,
sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then CHRGBK_RVS_CT else 0 end) as CHRGBK_RVS_CT_R12,
sum(case when mo_id>=&r12_month. and mo_id<=&cur_month. then CHRGBK_RVS_USD_AM else 0 end) as CHRGBK_RVS_USD_AM_R12

from AWB_OUT.seller_temp1 as a left join AWB_OUT.NEW_SELLER_MO_FIN_SMRY as b
on a.mer_id=b.sell_id
and a.SUBM_SE_NO = b.SUBM_SE_NO
group by a.SUBM_SE_NO, a.mer_id;
quit;

data AWB_OUT.seller_cv_smry(drop=CR_ROC_ly3 DR_ROC_ly3 CR_ROC_ly2 DR_ROC_ly2 CR_ROC_ly DR_ROC_ly CR_ROC_LY_CurM DR_ROC_LY_CurM YOY_CR_ROC_YTD YOY_DR_ROC_YTD 
YOY_CR_ROC_R12 YOY_CR_ROC_R6 YOY_CR_ROC_R3 YOY_DR_ROC_R12 YOY_DR_ROC_R6 YOY_DR_ROC_R3 CR_ROC_curm DR_ROC_curm CR_ROC_YTD DR_ROC_YTD CR_ROC_R12 CR_ROC_R6 CR_ROC_R3);
	set AWB_OUT.seller_cv_smry;
	ROC_YTD=(DR_ROC_YTD-CR_ROC_YTD);
	ROC_curm=(DR_ROC_curm-CR_ROC_curm);

	LY_ROC_R12=(YOY_DR_ROC_R12 - YOY_CR_ROC_R12);
	LY_ROC_R3 =(YOY_DR_ROC_R3 - YOY_CR_ROC_R3);
	LY_ROC_R6 =(YOY_DR_ROC_R6 - YOY_CR_ROC_R6);

	ROC_R12 = (DR_ROC_R12 - CR_ROC_R12);
	ROC_R6 = (DR_ROC_R6 - CR_ROC_R6);
	ROC_R3 = (DR_ROC_R3 - CR_ROC_R3);

	LY_ROC_YTD=(YOY_DR_ROC_YTD-YOY_CR_ROC_YTD);
	LY_ROC_CURM=(DR_ROC_LY_CurM-CR_ROC_LY_CurM);
	LY_ROC_FY=(DR_ROC_ly-CR_ROC_ly);
	LY2_ROC_FY=(DR_ROC_ly2-CR_ROC_ly2);
	LY3_ROC_FY=(DR_ROC_ly3-CR_ROC_ly3);

IF NOT MISSING(FIRST_CV_MO_ID) THEN DO;
		 
		 BCV_OPEN_DATE   = FIRST_CV_MO_ID;
	     FIRST_CV_MO_ID1 = INPUT(PUT(FIRST_CV_MO_ID,6.) || '01',YYMMDD10.);
	     BCV_END_DATE    = INPUT(PUT(INTNX('YEAR',FIRST_CV_MO_ID1,+1,'S'),YYMMN6.),6.);
	     BCV_END_MONTH   = INT(MOD(BCV_END_DATE,100));
	     BCV_END_YEAR    = INT(DIVIDE(BCV_END_DATE,100));;
	     BCV_END_MOID    = BCV_END_DATE;
		 
		 IF INT(DIVIDE(FIRST_CV_MO_ID,100)) = &yr_cy. AND (INPUT(PUT(MER_SETUP_DT,YYMMN6.),6.) <= FIRST_CV_MO_ID < INPUT(PUT(INTNX('YEAR',MER_SETUP_DT,+1,'S'),YYMMN6.),6.)) THEN YTD_ALIF_BCV=1;
		 ELSE YTD_ALIF_BCV=0;
		 IF INT(DIVIDE(FIRST_CV_MO_ID,100)) = &yr_ly. AND (INPUT(PUT(MER_SETUP_DT,YYMMN6.),6.) <= FIRST_CV_MO_ID < INPUT(PUT(INTNX('YEAR',MER_SETUP_DT,+1,'S'),YYMMN6.),6.))
    	 AND (0 <= INTCK('MONTH',INPUT(PUT(FIRST_CV_MO_ID,6.) || '01',YYMMDD10.),&month_end.) < 13) THEN CARRY_OVER_BCV=1;
		 ELSE CARRY_OVER_BCV=0;
END;
run;

proc sort data=AWB_OUT.seller_cv_smry noduprecs;
by mer_id;
run;

proc sql;
 create table AWB_OUT.ob_all(rename=(DR_ROC_R12=DBROCS_R12)) as 
 	select	m.*, 		
			(case 	when mer_tenure_mo_ct <=11 then "<1yr"
					when mer_tenure_mo_ct >=12 and mer_tenure_mo_ct <=23 then "1-2yrs"
					else "2yrs+"
					end) as mer_tenure_per,
			/*geo.DMA_MKT_AREA_ID,*/
			geo.DMA_MKT_AREA_ID,
			dma.DMA_DS_TX,	
			
			(case when DR_ROC_R12>=1 and cv_r12>0.000005 then 1 else 0 end) as cv_active_ind,
			(case when cv_r12>=50 then 1 else 0 end) as cv_active_50,	
			case when cv_r12<=0 then 									'01.<=0       	'
				 when cv_r12>0 and cv_r12 < 50 then  				'02.$1-<$50   	'
				 when cv_r12 >= 50 and cv_r12 <250 then 			'03.$50-<250 	'
				 when cv_r12 >= 250 and cv_r12 <500 then 			'04.$250-<500 	'
				 when cv_r12 >= 500 and cv_r12 <1000 then 			'05.$500-<1K  	'
				 when cv_r12 >= 1000 and cv_r12 <2000 then  		'06.$1K-<2K   	'
				 when cv_r12 >= 2000 and cv_r12 <3000 then  		'07.$2K-<3K   	'
				 when cv_r12 >= 3000 and cv_r12 <5000 then  		'08.$3K-<5K 	'
				 when cv_r12 >= 5000 and cv_r12 <10000 then			'09.$5K-<10K 	'
				when cv_r12 >= 10000 and cv_r12 < 15000 then  		'10.$10K-<15K   '
				when cv_r12 >= 15000 and cv_r12 < 25000 then  		'11.$15K-<25K   '
				when cv_r12 >= 25000 and cv_r12 < 50000 then  		'12.$25K-<50K   '
				when cv_r12 >= 50000 and cv_r12 < 100000 then  		'13.$50K-<100K  '
				when cv_r12 >= 100000 and cv_r12 < 250000 then 		'14.$100K-<250K '
				when cv_r12 >= 250000 and cv_r12 < 500000 then 		'15.$250K-<500K '
				when cv_r12 >= 500000 and cv_r12 < 750000 then 		'16.$500K-<750K '
				when cv_r12 >= 750000 and cv_r12 < 1000000 then  	'17.$750K-<1MM  '
				when cv_r12 >= 1000000 and cv_r12 < 2000000 then 	'18.$1MM-<2MM   '
				when cv_r12 >= 2000000 and cv_r12 < 3000000 then 	'19.$2MM-<3MM   '
				when cv_r12 >= 3000000 and cv_r12 < 10000000 then 	'20.$3MM-<10MM  '
				when cv_r12 >= 10000000 then 							'21.$10MM+      ' 
			end as cur_cv_r12_band,

			case when cv_r6<=0 then 									'01.<=0       	'
				 when cv_r6>0 and cv_r6 < 50 then  				'02.$1-<$50   	'
				 when cv_r6 >= 50 and cv_r6 <250 then 			'03.$50-<250 	'
				 when cv_r6 >= 250 and cv_r6 <500 then 			'04.$250-<500 	'
				 when cv_r6 >= 500 and cv_r6 <1000 then 			'05.$500-<1K  	'
				 when cv_r6 >= 1000 and cv_r6 <2000 then  		'06.$1K-<2K   	'
				 when cv_r6 >= 2000 and cv_r6 <3000 then  		'07.$2K-<3K   	'
				 when cv_r6 >= 3000 and cv_r6 <5000 then  		'08.$3K-<5K 	'
				 when cv_r6 >= 5000 and cv_r6 <10000 then			'09.$5K-<10K 	'
				when cv_r6 >= 10000 and cv_r6 < 15000 then  		'10.$10K-<15K   '
				when cv_r6 >= 15000 and cv_r6 < 25000 then  		'11.$15K-<25K   '
				when cv_r6 >= 25000 and cv_r6 < 50000 then  		'12.$25K-<50K   '
				when cv_r6 >= 50000 and cv_r6 < 100000 then  		'13.$50K-<100K  '
				when cv_r6 >= 100000 and cv_r6 < 250000 then 		'14.$100K-<250K '
				when cv_r6 >= 250000 and cv_r6 < 500000 then 		'15.$250K-<500K '
				when cv_r6 >= 500000 and cv_r6 < 750000 then 		'16.$500K-<750K '
				when cv_r6 >= 750000 and cv_r6 < 1000000 then  	'17.$750K-<1MM  '
				when cv_r6 >= 1000000 and cv_r6 < 2000000 then 	'18.$1MM-<2MM   '
				when cv_r6 >= 2000000 and cv_r6 < 3000000 then 	'19.$2MM-<3MM   '
				when cv_r6 >= 3000000 and cv_r6 < 10000000 then 	'20.$3MM-<10MM  '
				when cv_r6 >= 10000000 then 							'21.$10MM+      ' 
			end as cur_cv_r6_band,

			case when cv_r3<=0 then 									'01.<=0       	'
				 when cv_r3>0 and cv_r3 < 50 then  				'02.$1-<$50   	'
				 when cv_r3 >= 50 and cv_r3 <250 then 			'03.$50-<250 	'
				 when cv_r3 >= 250 and cv_r3 <500 then 			'04.$250-<500 	'
				 when cv_r3 >= 500 and cv_r3 <1000 then 			'05.$500-<1K  	'
				 when cv_r3 >= 1000 and cv_r3 <2000 then  		'06.$1K-<2K   	'
				 when cv_r3 >= 2000 and cv_r3 <3000 then  		'07.$2K-<3K   	'
				 when cv_r3 >= 3000 and cv_r3 <5000 then  		'08.$3K-<5K 	'
				 when cv_r3 >= 5000 and cv_r3 <10000 then			'09.$5K-<10K 	'
				when cv_r3 >= 10000 and cv_r3 < 15000 then  		'10.$10K-<15K   '
				when cv_r3 >= 15000 and cv_r3 < 25000 then  		'11.$15K-<25K   '
				when cv_r3 >= 25000 and cv_r3 < 50000 then  		'12.$25K-<50K   '
				when cv_r3 >= 50000 and cv_r3 < 100000 then  		'13.$50K-<100K  '
				when cv_r3 >= 100000 and cv_r3 < 250000 then 		'14.$100K-<250K '
				when cv_r3 >= 250000 and cv_r3 < 500000 then 		'15.$250K-<500K '
				when cv_r3 >= 500000 and cv_r3 < 750000 then 		'16.$500K-<750K '
				when cv_r3 >= 750000 and cv_r3 < 1000000 then  	'17.$750K-<1MM  '
				when cv_r3 >= 1000000 and cv_r3 < 2000000 then 	'18.$1MM-<2MM   '
				when cv_r3 >= 2000000 and cv_r3 < 3000000 then 	'19.$2MM-<3MM   '
				when cv_r3 >= 3000000 and cv_r3 < 10000000 then 	'20.$3MM-<10MM  '
				when cv_r3 >= 10000000 then 							'21.$10MM+      ' 
			end as cur_cv_r3_band
 from    AWB_OUT.seller_cv_smry as m
				left join AWB_OUT.crt_postal_geo_summary as geo
					on substr(PHYS_AD_POST_CD_TX,1,5) =substr(geo.post_cd,1,5)
				left join 
					AWB_OUT.dma_mapping as dma
					on geo.DMA_MKT_AREA_ID = dma.DMA_MKT_AREA_ID;  
	quit;


	
proc sql;
create table AWB_OUT.ob_output as
select m.*,
n.active_ind as mtch_se_active_ind,
n.cv_active_ind as mtch_se_cv_active_ind,
n.srvc_prvd_cd  as mtch_se_srvc_prvd_cd,
n.segment as mtch_se_segment,
n.portfolio_type as mtch_se_portfolio_type,
n.se_type as mtch_se_type,
n.aff_toc as mtch_se_aff_toc			
from AWB_OUT.ob_all as m left join awb_out.all_mer_cv1 as n
on m.MTCH_TPA_SE_NO=n.se_no;
quit;

/*Cancel inactive NAB*/
proc sql;
 create table AWB_OUT.ob_output2 as 
	select	m.*,
			(case when REL_GRP_TYPE_NO=3 then	'New Signings'
					when REL_GRP_TYPE_NO=1 then	'Non Accepting'
					when REL_GRP_TYPE_NO=2 then 'Accepting Amex, New to Partner'
					when REL_GRP_TYPE_NO=4 then	'Amex Base Conversion'
					else 'other'
			end) as relation_type
		
	from 	AWB_OUT.ob_output as m;
quit;

data AWB_OUT.ob_output3;
	set 	AWB_OUT.ob_output2;
	where 	compress(relation_type)<>'other'
	and		(mer_setup_dt between '01NOV2013'd and &month_end.);
	/*amish1 2018.01.22- Exclusion of merchants who enrolled on or after Jan 1st 2018, and were cancelled on the same date as of their creation date*/
    IF (DATEPART(SRCE_CREAT_TS) >='01JAN2018'D) AND (SELL_ACPT_STA_UPDT_DT EQ DATEPART(SRCE_CREAT_TS)) 
	AND cur_mer_sta_cd in ('D', 'N') THEN DELETE;
	IF cur_mer_sta_cd = 'E' THEN DELETE;
	run	;


filename FileList pipe "cd /amex/ramp/tech1/AWB_CARE/main_area;ls -t all_mer_seller_&prev_run_yr._&prev_run_mn.*"; 

data OB_Output_list; 
format Name $800.; 
infile FileList dsd missover; 
input name $ ;
if _n_ >1 then delete;
b=scan(Name,1,'.');
c=catx('.','smsawbsc',b);
run;

proc sql;
select c into :ob_list separated by ' ' from OB_Output_list ;
quit;

%put &ob_list;

/* this patch added on 11/4/2016 for having the cancel year and cancel month for reinstated se's */
/* amish159 - Oct18 - Pulling Previous Months conv_active_ind conv_cv_active to fix Portf_Basln issue*/

data AWB_OUT.mid_output(Keep=mer_id subm_se_no cancel_year cancel_month reinstate_year reinstate_month /*conv_active_ind conv_cv_active*/
              Rename=(cancel_year=cnc_year_old cancel_month=cnc_mnth_old reinstate_year=rein_year_old reinstate_month=rein_mnth_old /*conv_active_ind=conv_active_ind_old conv_cv_active=conv_cv_active_old*/));
set &ob_list.;
where upcase(se_type) contains 'SELL';
run;

proc sort data=AWB_OUT.ob_output3; /*Error*/
by mer_id subm_se_no;
run;

proc sort data=AWB_OUT.mid_output;
by mer_id subm_se_no;
run;

data AWB_OUT.mid_ob_output;
merge AWB_OUT.ob_output3(in=a) AWB_OUT.mid_output(in=b);  /* ERROR*/
by mer_id subm_se_no;
if a;
/*if not missing(mtch_tpa_se_no) then conv_active_ind_new=coalesce(conv_active_ind,conv_active_ind_old);*/
/*if not missing(mtch_tpa_se_no) then conv_cv_active_new =coalesce(conv_cv_active,conv_cv_active_old);*/
/*drop conv_active_ind conv_active_ind_old conv_cv_active conv_cv_active_old;*/
/*rename conv_active_ind_new=conv_active_ind conv_cv_active_new=conv_cv_active;*/
run;

/* cancellation logic changed basis sell_acpt_sta_updt_dt */
/*2018-07-18 amish1  Updated the logic to cancel the portfolio transferred merchants irrespective of cancellation date, marking active_ind,onbook_lif to 0*/
data AWB_OUT.ob_output4;
set AWB_OUT.mid_ob_output;
active_ind=1; 
/* Portfolio Change - amish1 2017/10/13
Updated Check to have T, I SELL_ACPT_STA_CD as well - To update Orginal SE fields
If ((SELL_ACPT_STA_UPDT_DT  > '01SEP2013'd  and SELL_ACPT_STA_UPDT_DT <= &cxl_month.) or SELL_ACPT_STA_UPDT_DT = '') and cur_mer_sta_cd in ('D', 'N') then do;
	If ((SELL_ACPT_STA_UPDT_DT  > '01SEP2013'd  and SELL_ACPT_STA_UPDT_DT <= &cxl_month.) or SELL_ACPT_STA_UPDT_DT = '') and cur_mer_sta_cd in ('D', 'N','T','I') then do;*/
	If (((SELL_ACPT_STA_UPDT_DT  > '01SEP2013'd  and SELL_ACPT_STA_UPDT_DT <= &cxl_month.) or SELL_ACPT_STA_UPDT_DT = '') and cur_mer_sta_cd in ('D', 'N')) or cur_mer_sta_cd in('T','I','E') then do;
	active_ind=0; 
	cancel_year=year(SELL_ACPT_STA_UPDT_DT); 
	cancel_month=month(SELL_ACPT_STA_UPDT_DT);
	reinstate_year=rein_year_old;
	reinstate_month=rein_mnth_old;
	end;
	else if (SELL_ACPT_STA_UPDT_DT >'01SEP2013'd and SELL_ACPT_STA_UPDT_DT<=&month_end.)  and cur_mer_sta_cd in ('R') then do;
	reinstate_year= year(SELL_ACPT_STA_UPDT_DT);
	reinstate_month= month(SELL_ACPT_STA_UPDT_DT);
	cancel_year=cnc_year_old;
	cancel_month=cnc_mnth_old;
	end;

	
	total_lif=1;
	if cv_active_ind=1 then cv_alif=1; else cv_alif=0;
	if cv_active_50=1 then cv50_alif=1; else cv50_alif=0;	
	
	if active_ind=1 and cv_active_50=1 then  onbook_alif50=1; else onbook_alif50=0;
run;


PROC DATASETS LIB=AWB_OUT NOLIST;/*X*/
DELETE mid_ob_output;
RUN;


proc sql;
 create table AWB_OUT.ob_output6 as 
	select	m.*,
			(case when REL_GRP_TYPE_NO =3 then 'Ph1- Net New' 
                  when REL_GRP_TYPE_NO =2 and mtch_se_srvc_prvd_cd>'0000' then 'Ph1- FldCnvOP'
                  when REL_GRP_TYPE_NO =2 and mtch_se_srvc_prvd_cd='0000' then 'Ph1- FldCnvPr'
                  when REL_GRP_TYPE_NO =2 and MTCH_TPA_SE_NO is null and MTCH_SELL_ID is not null and MTCH_SELL_SUBM_SE_NO is not null then 'Ph1- FldCnvOB'
                  when REL_GRP_TYPE_NO =2 then 'Ph1- FldCnvUNK'

                  when REL_GRP_TYPE_NO =1 then 'Ph2- PartnrNAB'

				  when REL_GRP_TYPE_NO = 4 AND REL_SUBGRP_TYPE_CD='A' then 'Ph3- OPBsCnvA'
                  when REL_GRP_TYPE_NO = 4 AND REL_SUBGRP_TYPE_CD='B' then 'Ph4- PrBsCnvB'
                  when REL_GRP_TYPE_NO = 4 AND REL_SUBGRP_TYPE_CD='C' then 'Ph4- PrBsCnvC'
                  when REL_GRP_TYPE_NO = 4 AND REL_SUBGRP_TYPE_CD='' then 'Ph4- UNKBsCnv'
              end) as PHASE

	from 	AWB_OUT.ob_output4 as m;
quit;

PROC DATASETS LIB=AWB_OUT NOLIST;
DELETE ob_output4;
RUN;

/*Changes made in Feb'19 run*/
data awb_out.ob_output_final ;
               set AWB_OUT.ob_output6;
if  active_ind=1 and cv_active_ind=1 and mtch_se_active_ind=1 and mtch_se_cv_active_ind=1 then dual_ALIF_ind='Y'; else dual_ALIF_ind='N';

if  active_ind=1 and mtch_se_active_ind=1 then dual_LIF_ind='Y'; 
else if active_ind=0 and mtch_se_active_ind=1 and mtch_se_cv_active_ind=0 and SELL_ACPT_STA_UPDT_DT >= '01JUN2017'd  /*and Cancel_lif = 1 */ then dual_LIF_ind='Z'; 
else dual_LIF_ind='N';

if dual_ALIF_ind='Y' and dual_LIF_ind='Y' then dual_active_ind='Y';        /*For dual LIF take "Y" and "P"; for dual ALIF take "Y"*/

else if dual_ALIF_ind='N' and dual_LIF_ind='Y' and mtch_se_cv_active_ind=1  then dual_active_ind='X';   /*Dedupe these from OB LIF*/

else if dual_ALIF_ind='N' and  ( dual_LIF_ind='Y' or dual_LIF_ind='Z')       then dual_active_ind='P';  else dual_active_ind='N';

	if active_ind=1 and dual_active_ind ne 'X' then onbook_lif=1; else onbook_lif=0;
	if active_ind=1 and cv_active_ind=1 and dual_active_ind ne 'X' then onbook_alif=1; else onbook_alif=0;
run;

proc sort data=awb_out.ob_output_final nodupkey;
by se_no;
run;

proc sql;
create table AWB_OUT.all_mer_final as
select a.*, b.mtch_tpa_se_no, b.dual_active_ind from 
awb_out.all_mer_cv1 as a left join awb_out.ob_output_final as b on 
a.se_no=b.mtch_tpa_se_no;
quit;

proc sort data=AWB_OUT.all_mer_final;
by se_no descending dual_active_ind;
run;

proc sort data=AWB_OUT.all_mer_final nodupkey ;
by se_no ;
run;

data AWB_OUT.all_mer_final_mid;
set AWB_OUT.all_mer_final;
	if active_ind=1 and SUBM_CPBL_TYPE_1_IN='Y' AND Dual_active_ind not in ('Y'  'P') then onbook_lif=1; else onbook_lif=0;/*1.2*/
	if active_ind=1 and cv_active_ind=1 and SUBM_CPBL_TYPE_1_IN='Y' AND Dual_active_ind NOT IN ('Y', 'P') then onbook_alif=1; else onbook_alif=0;
run;

Data AWB_OUT.all_mer_final1;
set AWB_OUT.all_mer_final_mid;
id = put(mer_id, 30.);
drop mer_id;
rename id=mer_id;
run;

proc sort data=AWB_OUT.all_mer_final1 out=AWB_OUT.all_mer_sorted nodupkey;
by se_no;
run;


proc sort data=awb_out.ob_output_final out=AWB_OUT.ob_sorted nodupkey;
by se_no;
run;



/* Combine both Prop and OptBlue */
data AWB_OUT.combined_iclic_info(rename=(line1=PHYS_AD_LINE_1_TX line2=PHYS_AD_LINE_2_TX line3=PHYS_AD_LINE_3_TX));
merge AWB_OUT.all_mer_sorted(Rename=(PHYS_AD_LINE_1_TX=line1 PHYS_AD_LINE_2_TX=line2 PHYS_AD_LINE_3_TX=line3)) 
AWB_OUT.ob_sorted(Rename=(AD_LINE_1_TX=line1 AD_LINE_2_TX=line2 AD_LINE_3_TX=line3));
by se_no;
run;

PROC DATASETS LIB=AWB_OUT NOLIST;/*X*/
DELETE all_mer_sorted ob_sorted;
RUN;
/*Create industry file*/

data temp.crmd_ind (keep=SE_NO crmc sic8);
set AWB_OUT.crmd_ind_base;
run;

proc sort nodupkey data=temp.crmd_ind;
  by se_no;
run;

/************************************************
*               122 INDUSTRY CODE               *
*               9 INDUSTRY GROUPS               *
************************************************/

data temp.crmd_ind;
  length mf_ind_cd $5 INDUSTRY $53;
  set temp.crmd_ind;

  sic1=substr(sic8,1,1);
  sic2=substr(sic8,1,2);
  sic3=substr(sic8,1,3);
  sic4=substr(sic8,1,4);
  sic6=substr(sic8,1,6);

if CRMC="1039" then MF_Ind_Cd="M0110"; else
if CRMC="1057" then MF_Ind_Cd="M0120"; else
if CRMC="0174" then MF_Ind_Cd="M0130"; else
if CRMC="1086" then MF_Ind_Cd="M0130"; else
if CRMC="1087" then MF_Ind_Cd="M0130"; else
if CRMC="1101" then MF_Ind_Cd="M0130"; else
if CRMC="1244" then MF_Ind_Cd="M0130"; else
if CRMC="1310" then MF_Ind_Cd="M0130"; else
if CRMC="1414" then MF_Ind_Cd="M0130"; else
if CRMC="1103" then MF_Ind_Cd="M0140"; else
if CRMC="1428" then MF_Ind_Cd="M0140"; else
if CRMC="1626" then MF_Ind_Cd="M0140"; else
if CRMC="0519" then MF_Ind_Cd="M0150"; else
if CRMC="0100" then MF_Ind_Cd="M0160"; else
if CRMC="0123" then MF_Ind_Cd="M0160"; else
if CRMC="0163" then MF_Ind_Cd="M0160"; else
if CRMC="1008" then MF_Ind_Cd="M0160"; else
if CRMC="1097" then MF_Ind_Cd="M0160"; else
if CRMC="1106" then MF_Ind_Cd="M0160"; else
if CRMC="1227" then MF_Ind_Cd="M0160"; else
if CRMC="1462" then MF_Ind_Cd="M0160"; else
if CRMC="1250" then MF_Ind_Cd="M0170"; else
if CRMC="1476" then MF_Ind_Cd="M0170"; else
if CRMC="0150" then MF_Ind_Cd="M0180"; else
if CRMC="0180" then MF_Ind_Cd="M0190"; else
if CRMC in (' ',"0000") then MF_Ind_Cd="M0200"; else
if CRMC="0106" then MF_Ind_Cd="M0200"; else
if CRMC="0166" then MF_Ind_Cd="M0200"; else
if CRMC="1112" then MF_Ind_Cd="M0200"; else
if CRMC="1165" then MF_Ind_Cd="M0200"; else
if CRMC="1224" then MF_Ind_Cd="M0200"; else
if CRMC="1257" then MF_Ind_Cd="M0200"; else
if CRMC="1319" then MF_Ind_Cd="M0200"; else
if CRMC="1679" then MF_Ind_Cd="M0200"; else
if CRMC="0195" then MF_Ind_Cd="M0210"; else
if CRMC="1185" then MF_Ind_Cd="M0210"; else
if CRMC="1348" then MF_Ind_Cd="M0210"; else
if CRMC="1660" then MF_Ind_Cd="M0210"; else
if CRMC="1661" then MF_Ind_Cd="M0210"; else
if CRMC="1663" then MF_Ind_Cd="M0210"; else
if CRMC="0951" then MF_Ind_Cd="M0220"; 
if SIC6="599912" then MF_Ind_Cd="M1230"; else
if SIC6="599911" then MF_Ind_Cd="M1270"; else
if SIC6="599906" then MF_Ind_Cd="M2260"; else
if SIC6="599913" then MF_Ind_Cd="M2270"; else
if SIC6="754903" then MF_Ind_Cd="M3230"; else
if SIC4="5813" then MF_Ind_Cd="M0230"; else
if SIC4="5411" then MF_Ind_Cd="M1110"; else
if SIC4="5541" then MF_Ind_Cd="M1140"; else
if SIC4="5621" then MF_Ind_Cd="M1150"; else
if SIC4="5651" then MF_Ind_Cd="M1170"; else
if SIC4="5921" then MF_Ind_Cd="M1210"; else
if SIC4="5992" then MF_Ind_Cd="M1220"; else
if SIC4="5961" then MF_Ind_Cd="M1250"; else
if SIC4="5211" then MF_Ind_Cd="M2110"; else
if SIC4="5511" then MF_Ind_Cd="M2130"; else
if SIC4="5531" then MF_Ind_Cd="M2150"; else
if SIC4="5712" then MF_Ind_Cd="M2180"; else
if SIC4="5932" then MF_Ind_Cd="M2220"; else
if SIC4="5941" then MF_Ind_Cd="M2230"; else
if SIC4="5944" then MF_Ind_Cd="M2240"; else
if SIC4="5947" then MF_Ind_Cd="M2250"; else
if SIC4="7011" then MF_Ind_Cd="M3110"; else
if SIC4="7216" then MF_Ind_Cd="M3130"; else
if SIC4="7231" then MF_Ind_Cd="M3160"; else
if SIC4="7241" then MF_Ind_Cd="M3170"; else
if SIC4="7538" then MF_Ind_Cd="M3200"; else
if SIC4="7542" then MF_Ind_Cd="M3220"; else
if SIC4="7911" then MF_Ind_Cd="M3280"; else
if SIC4="7933" then MF_Ind_Cd="M3280"; else
if SIC4="7991" then MF_Ind_Cd="M3280"; else
if SIC4="7992" then MF_Ind_Cd="M3280"; else
if SIC4="7997" then MF_Ind_Cd="M3280"; else
if SIC4="8111" then MF_Ind_Cd="M3380"; else
if SIC4="8011" then MF_Ind_Cd="M4110"; else
if SIC4="8021" then MF_Ind_Cd="M4120"; else
if SIC4="8041" then MF_Ind_Cd="M4130"; else
if SIC4="8042" then MF_Ind_Cd="M4140"; else
if SIC4="8721" then MF_Ind_Cd="M5170"; else
if SIC4="5047" then MF_Ind_Cd="M6120"; else
if SIC4="5085" then MF_Ind_Cd="M6150"; else
if SIC4="0742" then MF_Ind_Cd="M7110"; else
if SIC4="1711" then MF_Ind_Cd="M7150"; else
if SIC4="4121" then MF_Ind_Cd="M8110"; else
if SIC3="596" then MF_Ind_Cd="M1260"; else
if SIC3="571" then MF_Ind_Cd="M2190"; else
if SIC3="721" then MF_Ind_Cd="M3140"; else
if SIC3="753" then MF_Ind_Cd="M3210"; else
if SIC3="804" then MF_Ind_Cd="M4150"; else
if SIC3="504" then MF_Ind_Cd="M6130"; else
if SIC3="508" then MF_Ind_Cd="M6160"; else
if SIC3="542" then MF_Ind_Cd="M1120"; else
if SIC3="546" then MF_Ind_Cd="M1130"; else
if SIC3="549" then MF_Ind_Cd="M1131"; else
if SIC3="563" then MF_Ind_Cd="M1160"; else
if SIC3="566" then MF_Ind_Cd="M1180"; else
if SIC3="591" then MF_Ind_Cd="M1200"; else
if SIC3="594" then MF_Ind_Cd="M1201"; else
if SIC3="523" then MF_Ind_Cd="M2120"; else
if SIC3="525" then MF_Ind_Cd="M2120"; else
if SIC3="526" then MF_Ind_Cd="M2120"; else
if SIC3="552" then MF_Ind_Cd="M2140"; else
if SIC3="557" then MF_Ind_Cd="M2160"; else
if SIC3="572" then MF_Ind_Cd="M2200"; else
if SIC3="573" then MF_Ind_Cd="M2210"; else
if SIC3="722" then MF_Ind_Cd="M3150"; else
if SIC3="726" then MF_Ind_Cd="M3180"; else
if SIC3="792" then MF_Ind_Cd="M3270"; else
if SIC3="821" then MF_Ind_Cd="M3300"; else
if SIC3="822" then MF_Ind_Cd="M3310"; else
if SIC3="866" then MF_Ind_Cd="M3350"; else
if SIC3="803" then MF_Ind_Cd="M4150"; else
if SIC3="806" then MF_Ind_Cd="M4160"; else
if SIC3="731" then MF_Ind_Cd="M5110"; else
if SIC3="733" then MF_Ind_Cd="M5120"; else
if SIC3="734" then MF_Ind_Cd="M5130"; else
if SIC3="735" then MF_Ind_Cd="M5140"; else
if SIC3="737" then MF_Ind_Cd="M5150"; else
if SIC3="874" then MF_Ind_Cd="M5180"; else
if SIC3="503" then MF_Ind_Cd="M6110"; else
if SIC3="507" then MF_Ind_Cd="M6110"; else
if SIC3="506" then MF_Ind_Cd="M6140"; else
if SIC3="509" then MF_Ind_Cd="M6170"; else
if SIC3="078" then MF_Ind_Cd="M7120"; else
if SIC3="271" then MF_Ind_Cd="M7170"; else
if SIC3="272" then MF_Ind_Cd="M7170"; else
if SIC3="273" then MF_Ind_Cd="M7170"; else
if SIC3="274" then MF_Ind_Cd="M7170"; else
if SIC3="421" then MF_Ind_Cd="M8120"; else
if SIC3="422" then MF_Ind_Cd="M8130"; else
if SIC3="472" then MF_Ind_Cd="M8140"; else
if SIC3="481" then MF_Ind_Cd="M8150"; else
if SIC2="72" then MF_Ind_Cd="M3190"; else
if SIC2="75" then MF_Ind_Cd="M3240"; else
if SIC2="76" then MF_Ind_Cd="M3250"; else
if SIC2="78" then MF_Ind_Cd="M3260"; else
if SIC2="79" then MF_Ind_Cd="M3290"; else
if SIC2="86" then MF_Ind_Cd="M3390"; else
if SIC2="80" then MF_Ind_Cd="M4170"; else
if SIC2="73" then MF_Ind_Cd="M5160"; else
if SIC2="87" then MF_Ind_Cd="M5190"; else
if SIC2="17" then MF_Ind_Cd="M7160"; else
if SIC2="27" then MF_Ind_Cd="M7180"; else
if SIC2="83" then MF_Ind_Cd="M3330"; else
if SIC2="84" then MF_Ind_Cd="M3340"; else
if SIC2="51" then MF_Ind_Cd="M6190"; else
if SIC2="56" then MF_Ind_Cd="M1190"; else
if SIC2="53" then MF_Ind_Cd="M1240"; else
if SIC2="52" then MF_Ind_Cd="M1280"; else
if SIC2="54" then MF_Ind_Cd="M1280"; else
if SIC2="57" then MF_Ind_Cd="M1280"; else
if SIC2="59" then MF_Ind_Cd="M1280"; else
if SIC2="55" then MF_Ind_Cd="M2170"; else
if SIC2="70" then MF_Ind_Cd="M3120"; else
if SIC2="82" then MF_Ind_Cd="M3320"; else
if SIC2="64" then MF_Ind_Cd="M3360"; else
if SIC2="60" then MF_Ind_Cd="M3370"; else
if SIC2="61" then MF_Ind_Cd="M3370"; else
if SIC2="62" then MF_Ind_Cd="M3370"; else
if SIC2="63" then MF_Ind_Cd="M3370"; else
if SIC2="65" then MF_Ind_Cd="M3370"; else
if SIC2="67" then MF_Ind_Cd="M3370"; else
if SIC2="89" then MF_Ind_Cd="M5200"; else
if SIC2="50" then MF_Ind_Cd="M6180"; else
if SIC2="15" then MF_Ind_Cd="M7140"; else
if SIC2="16" then MF_Ind_Cd="M7140"; else
if SIC2="20" then MF_Ind_Cd="M7190"; else
if SIC2="21" then MF_Ind_Cd="M7190"; else
if SIC2="22" then MF_Ind_Cd="M7190"; else
if SIC2="23" then MF_Ind_Cd="M7190"; else
if SIC2="24" then MF_Ind_Cd="M7190"; else
if SIC2="25" then MF_Ind_Cd="M7190"; else
if SIC2="26" then MF_Ind_Cd="M7190"; else
if SIC2="28" then MF_Ind_Cd="M7190"; else
if SIC2="29" then MF_Ind_Cd="M7190"; else
if SIC2="30" then MF_Ind_Cd="M7190"; else
if SIC2="31" then MF_Ind_Cd="M7190"; else
if SIC2="32" then MF_Ind_Cd="M7190"; else
if SIC2="33" then MF_Ind_Cd="M7190"; else
if SIC2="34" then MF_Ind_Cd="M7190"; else
if SIC2="35" then MF_Ind_Cd="M7190"; else
if SIC2="36" then MF_Ind_Cd="M7190"; else
if SIC2="37" then MF_Ind_Cd="M7190"; else
if SIC2="38" then MF_Ind_Cd="M7190"; else
if SIC2="39" then MF_Ind_Cd="M7190"; else
if SIC2="40" then MF_Ind_Cd="M8160"; else
if SIC2="41" then MF_Ind_Cd="M8160"; else
if SIC2="42" then MF_Ind_Cd="M8160"; else
if SIC2="43" then MF_Ind_Cd="M8160"; else
if SIC2="44" then MF_Ind_Cd="M8160"; else
if SIC2="45" then MF_Ind_Cd="M8160"; else
if SIC2="46" then MF_Ind_Cd="M8160"; else
if SIC2="47" then MF_Ind_Cd="M8160"; else
if SIC2="48" then MF_Ind_Cd="M8170"; else
if SIC2="49" then MF_Ind_Cd="M8170"; else
if SIC2="91" then MF_Ind_Cd="M3400"; else
if SIC2="92" then MF_Ind_Cd="M3400"; else
if SIC2="93" then MF_Ind_Cd="M3400"; else
if SIC2="94" then MF_Ind_Cd="M3400"; else
if SIC2="95" then MF_Ind_Cd="M3400"; else
if SIC2="96" then MF_Ind_Cd="M3400"; else
if SIC2="97" then MF_Ind_Cd="M3400"; else
if SIC2="99" then MF_Ind_Cd="M3400"; else
if SIC2="01" then MF_Ind_Cd="M7130"; else
if SIC2="02" then MF_Ind_Cd="M7130"; else
if SIC2="07" then MF_Ind_Cd="M7130"; else
if SIC2="08" then MF_Ind_Cd="M7130"; else
if SIC2="09" then MF_Ind_Cd="M7130"; else
if SIC2="10" then MF_Ind_Cd="M7130"; else
if SIC2="12" then MF_Ind_Cd="M7130"; else
if SIC2="13" then MF_Ind_Cd="M7130"; else
if SIC2="14" then MF_Ind_Cd="M7130"; 
if substr(MF_Ind_Cd,1,2)='M0' then INDUSTRY='1)Eating and Drinking Places'; else
if substr(MF_Ind_Cd,1,2)='M1' then INDUSTRY='2)Retailers, Non-Durable and General Goods'; else
if substr(MF_Ind_Cd,1,2)='M2' then INDUSTRY='3)Retailers, Durable Goods'; else
if substr(MF_Ind_Cd,1,2)='M3' | MF_Ind_Cd='M7170' then INDUSTRY='4)Consumer and General Services'; else
if substr(MF_Ind_Cd,1,2)='M4' | MF_Ind_Cd='M7110' then INDUSTRY='5)Health and Medical Services'; else
if substr(MF_Ind_Cd,1,2)='M5' | MF_Ind_Cd='M7180' then INDUSTRY='6)Business Services'; else
if substr(MF_Ind_Cd,1,2)='M6' then INDUSTRY='7)Wholesalers'; else
if substr(MF_Ind_Cd,1,2)='M7' then INDUSTRY='8)Manufacturing, Construction, Agriculture and Mining'; else
if substr(MF_Ind_Cd,1,2)='M8' then INDUSTRY='9)Transportation, Communications, Public Utilities'; 
run;


/************************************************
*               42 LEVEL2 INDUSTRIES            *
************************************************/
data temp.crmd_ind;
  length SUBINDUSTRY $67;
  set temp.crmd_ind;

  if MF_Ind_Cd="M0110" then SUBINDUSTRY="Asian Restaurant"; else
  if MF_Ind_Cd="M0120" then SUBINDUSTRY="Bar & Grill Restaurant"; else
  if MF_Ind_Cd="M0130" then SUBINDUSTRY="Casual Restaurant"; else
  if MF_Ind_Cd="M0140" then SUBINDUSTRY="Caterers"; else
  if MF_Ind_Cd="M0150" then SUBINDUSTRY="Delicatessen"; else
  if MF_Ind_Cd="M0160" then SUBINDUSTRY="Ethnic Cuisine, Other"; else
  if MF_Ind_Cd="M0170" then SUBINDUSTRY="Fast Food Restaurant"; else
  if MF_Ind_Cd="M0180" then SUBINDUSTRY="Mexican Restaurant"; else
  if MF_Ind_Cd="M0190" then SUBINDUSTRY="Pizza Restaurant"; else
  if MF_Ind_Cd="M0200" then SUBINDUSTRY="Specialty Restaurant"; else
  if MF_Ind_Cd="M0210" then SUBINDUSTRY="Specialty Snack/Beverage Bar"; else
  if MF_Ind_Cd="M0220" then SUBINDUSTRY="Varied Menu Restaurant"; else
  if MF_Ind_Cd="M0230" then SUBINDUSTRY="Drinking Places (Alcoholic Beverages)"; else
  if mf_ind_cd="M3360" then SUBINDUSTRY="Finance, Insurance and Real Estate"; else
  if mf_ind_cd="M3370" then SUBINDUSTRY="Finance, Insurance and Real Estate"; else
  if mf_ind_cd="M3400" then SUBINDUSTRY="Public Administration"; else

  if sic2 in ('07','80') then SUBINDUSTRY="Health and Medical Services"; else
  if sic2="27" then SUBINDUSTRY="Printing, Publishing, And Allied Industries"; else
  if sic2="50" then SUBINDUSTRY="Wholesale Trade-durable Goods"; else
  if sic2="51" then SUBINDUSTRY="Wholesale Trade-non-durable Goods"; else
  if sic2="52" then SUBINDUSTRY="Building Materials, Hardware, Garden Supply, And Mobile Home Dealer"; else
  if sic2="53" then SUBINDUSTRY="General Merchandise Stores"; else
  if sic2="54" then SUBINDUSTRY="Food Stores"; else
  if sic2="55" then SUBINDUSTRY="Automotive Dealers And Gasoline Service Stations"; else
  if sic2="56" then SUBINDUSTRY="Apparel And Accessory Stores"; else
  if sic2="57" then SUBINDUSTRY="Home Furniture, Furnishings, And Equipment Stores"; else
  if sic2="59" then SUBINDUSTRY="Miscellaneous Retail"; else
  if sic2="70" then SUBINDUSTRY="Hotels, Rooming Houses, Camps, And Other Lodging Places"; else
  if sic2="72" then SUBINDUSTRY="Personal Services"; else
  if sic2="73" then SUBINDUSTRY="Business Services"; else
  if sic2="75" then SUBINDUSTRY="Automotive Repair, Services, And Parking"; else
  if sic2="76" then SUBINDUSTRY="Miscellaneous Repair Services"; else
  if sic2="78" then SUBINDUSTRY="Motion Pictures"; else
  if sic2="79" then SUBINDUSTRY="Amusement And Recreation Services"; else
  if sic2="81" then SUBINDUSTRY="Legal Services"; else
  if sic2="82" then SUBINDUSTRY="Educational Services"; else
  if sic2="83" then SUBINDUSTRY="Social Services"; else
  if sic2="84" then SUBINDUSTRY="Museums, Art Galleries, And Botanical And Zoological Gardens"; else
  if sic2="86" then SUBINDUSTRY="Membership Organizations"; else
  if sic2="87" then SUBINDUSTRY="Engineering, Accounting, Research, Management, And Related Services"; else
  if sic2="89" then SUBINDUSTRY="Miscellaneous Services"; else

  if substr(MF_Ind_Cd,1,2)='M7' then SUBINDUSTRY='Manufacturing, Construction, Agriculture and Mining'; else
  if substr(MF_Ind_Cd,1,2)='M8' then SUBINDUSTRY='Transportation, Communications, Public Utilities';
run;


PROC SQL;
 create table AWB_OUT.all_mer_seller2 as
	select a.*,
                case when a.MER_ACCT_ORIG_CD in ('06','07','08','12','17','20','22','37') 
					then 'Y' end as SMA_origin,

			case when a.MER_ACCT_ORIG_CD in ('03','04','09','14','19','24','28','34', /* PROP origin */
											'02','31','32','33' ) then "Prop" /* Shared origin */
				 when a.MER_ACCT_ORIG_CD in ('06','07','20','22') then "SMA-ESA"
				 when a.MER_ACCT_ORIG_CD in ('08') then "SMA-WTH PROP" 
				 when a.MER_ACCT_ORIG_CD in ('12','17') then "SMA-WTH Other"
            	 when a.MER_ACCT_ORIG_CD= '37' then "SMA-One Point"
				 when a.MER_ACCT_ORIG_CD= '38' then "SMA-OptBlue"
				 else "OTH"
		    end as ACQ_CHANNEL,
			f.industry as INDUSTRY_CRMD, 
			f.subindustry as  SUBINDUSTRY_CRMD
		from AWB_OUT.combined_iclic_info as a
				left join temp.crmd_ind as f
				     on a.se_no=f.se_no
; 
QUIT;

PROC DATASETS LIB=WORK NOLIST;/*X*/
DELETE crmd_ind;
RUN;

PROC DATASETS LIB=AWB_OUT NOLIST;/*X*/
DELETE combined_iclic_info;
RUN;

/*amish1 Mar-18 - Dropped SOW Fields*/

PROC SQL;
/** process share of wallet data **/
 create table AWB_OUT.all_mer_seller3 as 	
	Select 				
			a.*,
			c.industry as OptBlue_industry,
			c.mcc_desc as OB_MCC_DESC
	from	AWB_OUT.all_mer_seller2 as a
			left join ramp_in.OptBlue_MCC_industry as c
					on a.MCC_INDUS_CD = c.mcc_cd
	;
QUIT;

proc sort data=AWB_OUT.all_mer_seller3 out=AWB_OUT.all_mer_seller4 nodupkey;
by se_no;
run;


data AWB_OUT.all_mer_seller_new(rename=(DMA_DS_TX =DMA_DS_TX1));
set AWB_OUT.all_mer_seller4;
run;


/* Dataset Decommission - amish159 - 2018/02/07 - GMWPARK.ONLINE_MERCHANT_LIST is
decommissioned on 16-Mar-2018 and CRMDPARK.CRMD_DTL with MER_ONLN_OFFLN_CD = '1' will
hold the online merchants list.
left join GMWPARK.ONLINE_MERCHANT_LIST as d*/


/* created a temp table from CRMD_DTL with the needed columns and used that temp table in the join - AUG2021*/ 
/* GCL Decommission -  CRMDPARK removed Aug2021*/
/*proc sql;
create table crmd_park_dtl_onln as select se_no,MER_ONLN_OFFLN_CD from CRMDPARK.CRMD_DTL ;
quit;*/

/*proc sql;
create table crmd_park_dtl_onln as
  select trim(se10) as se_no,
         trim(se_crmd_mer_onln_offln_cd) as MER_ONLN_OFFLN_CD
  from RAMPYB.GMS_MERCHANT_CRMD_ANALYTICAL
  (
  YBUNLOAD=YES
  YB_JAVA_HOME="/usr"
  BL_YB_PATH="/usr/lib/ybtools/bin/ybunload"
  BL_LOGFILE="&stag_area./crmd_ind_analytic.log"
  BL_DATAFILE="&stag_area./crmd_ind_analytic.dat"
  );
quit;*/


data TEMP.crmd_park_dtl_onln (keep=se_no MER_ONLN_OFFLN_CD);
set AWB_OUT.crmd_ind_base;
run;

PROC SQL;
CREATE TABLE AWB_OUT.all_mer_seller_temp as 
Select a.*, 
(case when d.se_no is not null then 'Y' else 'N' end) as online_ind    ,
(case when a.DMA_MKT_AREA_ID is not null then dma.DMA_DS_TX else a.DMA_DS_TX1 end) as DMA_DS_TX,           
                 case
                      when GCG_IN ='Y' then "GCG"/*GCG Reconciliation*/
                      when PORTFOLIO_TYPE in ("CCLM") and compress(PRIM_CLNT_MGMT_DIV_NM) in ("USSMALLMERCHANTSCCLM") and PRIM_LGCY_SALE_SRVC_ID in ('06M0', '06MA', '06MB', '06MC') then "CCLM-CMT"
                      when PORTFOLIO_TYPE in ("CCLM") and compress(PRIM_CLNT_MGMT_DIV_NM) in ("USSMALLMERCHANTSCCLM") then "CCLM-Managed"
                      when PORTFOLIO_TYPE in ("OTH_SM", "OTH_SM INTL","OTH_SM AMEX Internal") then "OTH_SM"
                      when PORTFOLIO_TYPE in ("OPTBLUE") then "OPTBLUE"
                      when PORTFOLIO_TYPE in ("AGGREGATORS") then "AGGREGATORS"
                      else PRIM_CLNT_MGMT_DIV_NM
                 end as sub_segment
from  AWB_OUT.all_mer_seller_new as a
left join temp.crmd_park_dtl_onln as d
      on a.se_no=d.se_no
   and d.MER_ONLN_OFFLN_CD = '1'               
                      left join              
						AWB_OUT.dma_mapping as dma
							on a.DMA_MKT_AREA_ID = dma.DMA_MKT_AREA_ID
;
QUIT;

Data temp.a1;
set AWB_OUT.all_mer_seller_temp;
where upcase(se_type) not contains ('SELL');
run;

Data temp.a2(drop= sub_segment);
set AWB_OUT.all_mer_seller_temp;
where upcase(se_type) contains ('SELL');
run;


proc sql;
create table temp.a3 as
select distinct a.subm_se_no, b.sub_segment 
from
temp.a2 as a left join temp.a1 as b on
a.subm_se_no=b.se_no;
quit;

proc sql;
create table temp.a4 as
select a.*, b.sub_segment from
temp.a2 as a left join temp.a3 as b on
a.subm_se_no=b.subm_se_no;
quit;



proc sort data=temp.a1 out=temp.a5 nodupkey;
by se_no;
run;

proc sort data=temp.a4 nodupkey;
by se_no;
run;


proc sql;
create table temp.optblue_detail as
select a.*, b.mer_setup_dt as prop_dt, b.active_ind as prop_ind 
from
temp.a4 as a 
left join temp.a5 as b 
on
a.mtch_tpa_se_no=b.se_no;
quit;



proc sql;
create table temp.optblue_detail1 as 
select *, case
when prop_ind=1 then &month_end.
when prop_ind=0 and active_ind=1 then &month_end.
when prop_ind=0 and active_ind=0 then mdy(cancel_month,1,cancel_year) end as can_dt format Date9.
from temp.optblue_detail;
quit;


data temp.optblue_detail2;
set temp.optblue_detail1;
if mtch_tpa_se_no <> '' then
tot_tenure=intck('month',prop_dt,can_dt);
run;


proc sql;
create table temp.opd as
select *, max(tot_tenure) as total_tenure
from temp.optblue_detail2(drop= prop_dt prop_ind can_dt) group by mtch_tpa_se_no;
quit;


proc sql;
create table temp.prop as
select a.*, b.total_tenure from
temp.a5 as a left join temp.opd  as b on
a.se_no=b.mtch_tpa_se_no;
quit;

data temp.prop1;
set temp.prop;
if mtch_tpa_se_no = '' and active_ind=1 then total_tenure=intck('month', mer_setup_dt, &month_end.);
else if mtch_tpa_sE_no = '' and active_ind=0 then total_tenure=intck('month', mer_setup_dt, mdy(cancel_month,1,cancel_year));
run;


proc sort data=temp.opd nodupkey;by se_no;run;

proc sort data=temp.prop1 nodupkey;by se_no;run;

data temp.all_mer_seller6(drop= tot_tenure);
merge temp.opd temp.prop1;
by se_no;
run;


/*optblue seller report patch added */
data _null_;
dtt = put(intnx('MONTH',today(),-1,'END'),YYMMn6.);
call symputx("dtt", dtt);
put dtt= ;
prev_dt = put(intnx('MONTH',today(),-2,'END'),YYMMn6.);
call symputx("prev_dt", prev_dt);
put prev_dt=;
EX_CANDT = "'"||put(intnx('MONTH',today(),-3,'END'),DATE9.)||"'d";
call symputx("EX_CANDT", EX_CANDT);
put EX_CANDT=;
CANDT = "'"||put(intnx('MONTH',today(),-1,'END'),DATE9.)||"'d";
call symputx("CANDT", CANDT);
put CANDT=;
SGDT = "'"||put(intnx('MONTH',today(),-1,'END'),DATE9.)||"'d";
call symputx("SGDT", SGDT);
put SGDT=;
r12start = put(intnx('MONTH',today(),-12,'END'),YYMMn6.);
call symputx("r12start", r12start);
put r12start=;
r12end = put(intnx('MONTH',today(),-1,'END'),YYMMn6.);
call symputx("r12end", r12end);
put r12end=;
run;

* 2016 Monthly CV Roll;
proc sql;
create table temp.seller_vol as
select subm_se_no,sell_id as mer_id, chrg_vol_net_loc_am/100 as amt_curr_dt,(chrg_vol_dr_trans_ct - chrg_vol_cr_trans_ct) as cnt_curr_dt,
        chrg_vol_dr_trans_ct as drcnt_curr_dt
/* Portfolio Change - amish1 2017/10/13
from SELLER_MO_FIN_SMRY  */
from awb_out.NEW_SELLER_MO_FIN_SMRY 
  where mo_id = &dtt. and 
  (tpa_se_no in (select distinct tpa_Se_no from /*SELLER_MO_FIN_SMRY*/ awb_out.NEW_SELLER_MO_FIN_SMRY) and
subm_Se_no in (select distinct subm_se_no from /*SELLER_MO_FIN_SMRY*/ awb_out.NEW_SELLER_MO_FIN_SMRY));
run;


proc sort data=temp.seller_vol; by subm_se_no mer_id; run;
/*match the smsawbsc.seller_activation with CROSS_XREF table and pull all the records
with original Subm/sell_id and then replace the original sell id/subm se with new sell_id/se
 then append records to seller activation1*/

/* Portfolio Change - amish1 2017/10/13*/

/* for production change awb_out to SMSAWBSC */
proc sort data=SMSAWBSC.seller_activation OUT=temp.seller_activation_temp; 
by subm_se_no mer_id; 
run;
PROC SORT DATA=TEMP.OLD_PRTR_INFO (DROP=CHNG_RSN_CD RENAME=(SELL_ID=MER_ID)); 
BY SUBM_SE_NO MER_ID; 
RUN;
PROC SORT DATA=TEMP.NEW_PRTR_INFO (DROP=ORIG_SIGN_DATE ORIG_INIT_SUBM_DT RENAME=(SELL_ID=MER_ID));
BY SUBM_SE_NO MER_ID; 
RUN;

/*Updating the Original SE's with New SE's*/
DATA TEMP.OLD_TO_NEW_SE_SELL_ACT(DROP=NEW_SUBM_SE_NO NEW_SELL_ID);
 MERGE TEMP.SELLER_ACTIVATION_TEMP  (IN=A)
       TEMP.OLD_PRTR_INFO           (IN=B)
	   ;
	   BY SUBM_SE_NO MER_ID;

	   IF A AND B THEN DO;
	     SUBM_SE_NO = NEW_SUBM_SE_NO;
		 MER_ID     = NEW_SELL_ID;
		 OUTPUT;
	   END;
RUN;

PROC SORT DATA=TEMP.OLD_TO_NEW_SE_SELL_ACT; BY SUBM_SE_NO MER_ID; RUN;

/*Dropping the New SE's from seller_activation*/
DATA TEMP.NEW_SE_SELL_ACT;
  MERGE TEMP.SELLER_ACTIVATION_TEMP (IN=A)
        TEMP.NEW_PRTR_INFO     (IN=B)
		;
		BY SUBM_SE_NO MER_ID;
		IF A THEN DO;
          IF B THEN DO;
             OUTPUT TEMP.NEW_SE_SELL_ACT;
		  END;
		END;
RUN;

PROC SORT DATA=TEMP.NEW_SE_SELL_ACT; BY SUBM_SE_NO MER_ID; RUN;

/* Run starting here */

DATA TEMP.NEW_NOTIN_SELL_ACT	(KEEP=SUBM_SE_NO MER_ID)
     TEMP.FINAL_NEW_SE_SELL_ACT	(DROP=NPTD_CV_ACT_IND NACT_DT NACT_5_IND NACT_5_DT NACT_50_IND NACT_50_DT 
	   								NPTD_CHRG_AM NPTD_CHRG_CT NAMT_prev_dt NCNT_prev_dt NDRCNT_prev_dt)
     TEMP.ACT_ORIG_NEW_INACT	(DROP=NPTD_CV_ACT_IND NACT_DT NACT_5_IND NACT_5_DT NACT_50_IND NACT_50_DT 
	   								NPTD_CHRG_AM NPTD_CHRG_CT NAMT_prev_dt NCNT_prev_dt NDRCNT_prev_dt);

 MERGE TEMP.OLD_TO_NEW_SE_SELL_ACT (IN=A)
       TEMP.NEW_SE_SELL_ACT        (IN=B 
RENAME=(PTD_CV_ACT_IND=NPTD_CV_ACT_IND ACT_DT=NACT_DT ACT_5_IND=NACT_5_IND
ACT_5_DT=NACT_5_DT ACT_50_IND=NACT_50_IND ACT_50_DT=NACT_50_DT PTD_CHRG_AM=NPTD_CHRG_AM PTD_CHRG_CT=NPTD_CHRG_CT
AMT_prev_dt =NAMT_prev_dt CNT_prev_dt=NCNT_prev_dt DRCNT_prev_dt=NDRCNT_prev_dt))
	   ;
       BY SUBM_SE_NO MER_ID;

	   IF A THEN DO;
        IF B THEN DO;
	     IF PTD_CV_ACT_IND GT NPTD_CV_ACT_IND THEN PTD_CV_ACT_IND=PTD_CV_ACT_IND;
		 ELSE PTD_CV_ACT_IND=NPTD_CV_ACT_IND;
		 
		 IF NOT MISSING(ACT_DT) THEN DO;
		   IF NOT MISSING(NACT_DT) THEN DO;
		     IF ACT_DT LT NACT_DT THEN ACT_DT=ACT_DT;
		     ELSE ACT_DT=NACT_DT;
		   END;
		   ELSE DO;
		          ACT_DT=ACT_DT;
		   END;
		 END;
		 ELSE DO;
		   ACT_DT = NACT_DT;
		 END;

         IF ACT_5_IND GT NACT_5_IND THEN ACT_5_IND=ACT_5_IND;
		 ELSE ACT_5_IND=NACT_5_IND;

		 IF NOT MISSING(ACT_5_DT) THEN DO;
		   IF NOT MISSING(NACT_5_DT) THEN DO;
 		     IF ACT_5_DT LT NACT_5_DT THEN ACT_5_DT=ACT_5_DT;
		     ELSE ACT_5_DT=NACT_5_DT;
		   END;
		   ELSE DO;
		     ACT_5_DT=ACT_5_DT;
		   END;
		 END;
		 ELSE DO;
		   ACT_5_DT=NACT_5_DT;
		 END;

         IF ACT_50_IND GT NACT_50_IND THEN ACT_50_IND=ACT_50_IND;
		 ELSE ACT_50_IND=NACT_50_IND;

		 IF NOT MISSING(ACT_50_DT) THEN DO;
		   IF NOT MISSING(NACT_50_DT) THEN DO;
             IF ACT_50_DT LT NACT_50_DT THEN ACT_50_DT=ACT_50_DT;
		     ELSE ACT_50_DT=NACT_50_DT;
		   END;
		   ELSE DO;
		     ACT_50_DT=ACT_50_DT;
		   END;
		 END;
		 ELSE DO;
		  ACT_50_DT=NACT_50_DT;
		 END;

		 PTD_CHRG_AM=SUM(PTD_CHRG_AM,NPTD_CHRG_AM);
         PTD_CHRG_CT=SUM(PTD_CHRG_CT,NPTD_CHRG_CT);
         AMT_prev_dt=SUM(AMT_prev_dt,NAMT_prev_dt);
         CNT_prev_dt=SUM(CNT_prev_dt,NCNT_prev_dt);
         DRCNT_prev_dt=SUM(DRCNT_prev_dt,NDRCNT_prev_dt);

		 OUTPUT TEMP.FINAL_NEW_SE_SELL_ACT;
	    END;
	    ELSE DO;
		   OUTPUT TEMP.ACT_ORIG_NEW_INACT;
		END;
	  END;
	  ELSE DO;
       OUTPUT TEMP.NEW_NOTIN_SELL_ACT;
	  END;
RUN;
 



PROC SORT DATA=FINAL_NEW_SE_SELL_ACT; BY SUBM_SE_NO MER_ID; RUN;
PROC SORT DATA=ACT_ORIG_NEW_INACT   ; BY SUBM_SE_NO MER_ID; RUN;
PROC SORT DATA=NEW_NOTIN_SELL_ACT   ; BY SUBM_SE_NO MER_ID; RUN;

DATA OTHER_SELL_ACT;
  MERGE SELLER_ACTIVATION_TEMP (IN=A)
        FINAL_NEW_SE_SELL_ACT  (IN=B KEEP=SUBM_SE_NO MER_ID)
        ;BY SUBM_SE_NO MER_ID;
		IF A AND NOT B THEN OUTPUT;
RUN;

PROC SORT DATA=OTHER_SELL_ACT ; BY SUBM_SE_NO MER_ID; RUN;

/*Setting the above 3 datasets into one dataset, SELLER_ACTIVATION1*/
DATA SELLER_ACTIVATION1;
 SET OTHER_SELL_ACT 
     FINAL_NEW_SE_SELL_ACT
	 ACT_ORIG_NEW_INACT
	 ;
RUN;

PROC DATASETS LIB=WORK NOLIST;/*X*/
DELETE NEW_NOTIN_SELL_ACT FINAL_NEW_SE_SELL_ACT ACT_ORIG_NEW_INACT OTHER_SELL_ACT SELLER_ACTIVATION_TEMP;
RUN;

/*Sorting the dataset - seller_Activation1*/
proc sort data=seller_activation1; by subm_se_no mer_id; run;

data seller_activation(rename=(amt_curr_dt=amt_prev_dt cnt_curr_dt=cnt_prev_dt drcnt_curr_dt=drcnt_prev_dt));
     merge seller_activation1(in=a keep=subm_se_no mer_id ptd_cv_act_ind act_5_ind act_50_ind ptd_chrg_am ptd_chrg_ct act_dt act_5_dt act_50_dt) seller_vol(in=b);
	 by subm_se_no mer_id;
	 if b then do;
	 if ptd_cv_act_ind ne 1 then do;
	 if sum(ptd_chrg_am,amt_curr_dt) > 0.000005 and sum(ptd_chrg_ct,drcnt_curr_dt) >= 1 then do; ptd_cv_act_ind = 1; act_dt=&dtt.; end; end;
     if act_5_ind ne 1 then do;
	 if sum(ptd_chrg_am,amt_curr_dt) >= 5      then do; act_5_ind  = 1; act_5_dt=&dtt.;  end; end;
	 if act_50_ind ne 1 then do;
     if sum(ptd_chrg_am,amt_curr_dt) >= 50     then do; act_50_ind = 1; act_50_dt=&dtt.; end; end;
     ptd_chrg_am   = sum(ptd_chrg_am,amt_curr_dt);
     ptd_chrg_ct   = sum(ptd_chrg_ct,cnt_curr_dt);
	 end;
run;
proc sort data=seller_activation out=awb_out.seller_activation nodupkey; by subm_se_no mer_id; run;
proc sort data=opd out=opd1 nodupkey; by subm_se_no mer_id; run;

PROC DATASETS LIB=WORK NOLIST;/*X*/
DELETE seller_activation opd;
RUN;

data optblue_seller_report(drop=amt_&dtt. cnt_&dtt. drcnt_&dtt.);
    merge opd1 awb_out.seller_activation;	
	by subm_se_no mer_id;
	if act_dt not = . then do;
	act_months = intck('month',mer_setup_dt,mdy(input(substr(put(act_dt,6.),5,2),2.)*1,1,input(substr(put(act_dt,6.),1,4),4.)*1)); 
    if act_months in (-1,0)             then act_30  = 1;
	if act_months in (-1,0,1)           then act_60  = 1;
	if act_months in (-1,0,1,2)         then act_90  = 1;
	if act_months in (-1,0,1,2,3)       then act_120 = 1;
	if act_months in (-1,0,1,2,3,4)     then act_150 = 1;
	if act_months in (-1,0,1,2,3,4,5)   then act_180 = 1;
	end;
	if act_5_dt not = . then do;
	act5_months = intck('month',mer_Setup_dt,mdy(input(substr(put(act_5_dt,6.),5,2),2.)*1,1,input(substr(put(act_5_dt,6.),1,4),4.)*1)); 
    if act5_months in (-1,0)            then act5_30  = 1;
	if act5_months in (-1,0,1)          then act5_60  = 1;
	if act5_months in (-1,0,1,2)        then act5_90  = 1;
	if act5_months in (-1,0,1,2,3)      then act5_120 = 1;
	if act5_months in (-1,0,1,2,3,4)    then act5_150 = 1;
	if act5_months in (-1,0,1,2,3,4,5)  then act5_180 = 1;
	end;
	if act_50_dt not = . then do;
	act50_months = intck('month',mer_setup_dt,mdy(input(substr(put(act_50_dt,6.),5,2),2.)*1,1,input(substr(put(act_50_dt,6.),1,4),4.)*1)); 
    if act50_months in (-1,0)     		 then act50_30  = 1;
	if act50_months in (-1,0,1)   		 then act50_60  = 1;
	if act50_months in (-1,0,1,2) 		 then act50_90  = 1;
	if act50_months in (-1,0,1,2,3)     then act50_120 = 1;
	if act50_months in (-1,0,1,2,3,4)   then act50_150 = 1;
	if act50_months in (-1,0,1,2,3,4,5) then act50_180 = 1;
	end;
	/* Portfolio Change - amish1 2017/10/25*/
	if (SELL_ACPT_STA_UPDT_DT <= &EX_CANDT. and /*cur_mer_sta_cd in ('D','N')*/ cur_mer_sta_cd in ('D','N')) or cur_mer_sta_cd in('T','I','E') then seller_status_exec = 'C'; else seller_status_exec = 'A';
	if (SELL_ACPT_STA_UPDT_DT <= &CANDT. and /*cur_mer_sta_cd in ('D','N')*/  cur_mer_sta_cd in ('D','N')) or cur_mer_sta_cd in('T','I','E') then cur_mer_sta_cd = 'C'; else cur_mer_sta_cd = 'A';
run;
proc sql;
create table optblue_seller_report as
select b.*,a.*
from optblue_seller_report a left join ramp_in.optblue_mcc_industry b
on a.MCC_INDUS_CD = b.mcc_cd;
quit;

proc sql;
create table optblue_seller_report as
select b.portfolio_type,b.partner_typ,b.partner_nm as ob_prtr_nm,a.*
from optblue_seller_report(drop=portfolio_type partner_typ) a left join ramp_in.seller_hier_tpa b
on a.tpa_se_no = b.tpa_se_no;
quit;


data optblue_seller_report;
set optblue_seller_report;

if trim(Partner_Typ)='' then do;
       if subm_type_cd = 'T' then do; PORTFOLIO_TYPE  = 'OPTBLUE';    partner_typ = 'OPTBLUE TRADITIONAL'; end;
       if subm_type_cd = 'B' then do; PORTFOLIO_TYPE  = 'OPTBLUE';    partner_typ = 'OPTBLUE PAYFAC'; end;
       if subm_type_cd = 'C' then do; PORTFOLIO_TYPE  = 'AGGREGATOR'; partner_typ = 'AGGREGATOR REPORTED'; end;
       if subm_type_cd = 'A' then do; PORTFOLIO_TYPE  = 'AGGREGATOR'; partner_typ = 'AGGREGATOR NOT-REPORTED'; end;
end;

If subm_type_cd = 'B' then do; PORTFOLIO_TYPE  = 'OPTBLUE';    partner_typ = 'OPTBLUE PAYFAC'; end;

if subm_se_no in ('6569612666') then do; 
OB_PRTR_NM = 'WEPAYCHASE'; portfolio_type = 'AGGREGATOR'; partner_typ = 'AGGREGATOR NOT-REPORTED'; ob_consider_ind =4; end;              
              
if subm_se_no in ('6569612625','6569612583','6569612641','6569612591','6569612609','6569612567','6569612575','6569612633','6569612658') then do; 
OB_PRTR_NM = 'WEPAYCHASE'; portfolio_type = 'AGGREGATOR'; partner_typ = 'AGGREGATOR REPORTED'; ob_consider_ind =3; end;   

if subm_se_no in ('3056947070','3056947054','3056947047','3056947039','3056947021','3056947013','3056947005','3056946999','3056946981') then do; 
OB_PRTR_NM = 'WAVE INC'; portfolio_type = 'AGGREGATOR'; partner_typ = 'AGGREGATOR REPORTED'; ob_consider_ind =3; end;   

if subm_se_no in ('5548346659','1548293893','1548293885','1546508706','6542457700','6544840812','6544840861','6544840853',
'6544840838','6544840820','6544840804','6544840796','6544840788','6544840770','6542462940','6542460035',
'6542457692','6542457684','6542457676','6542457668','6542457627') then do; 
OB_PRTR_NM = 'WEPAY'; portfolio_type = 'AGGREGATOR'; partner_typ = 'AGGREGATOR NOT-REPORTED'; ob_consider_ind =4; end;

if subm_se_no in ('5361914104','1213504780','3219653557','3219653540','3214523953','3214523862','3214523854','3214523839',
'3214522831','3214522815','3214522773','3214522765','5368343919','3214523946') then do; 
OB_PRTR_NM = 'GLOBAL'; portfolio_type = 'AGGREGATOR'; partner_typ = 'AGGREGATOR REPORTED'; ob_consider_ind =3; end; 

if subm_se_no in ('1433064094','1433064219','1433064177','1433064169','1433064151','1433064144','1433064136','1433064128','1433064110','1433064102') then do; 
OB_PRTR_NM = 'PROPAY'; PORTFOLIO_TYPE  = 'OPTBLUE';    partner_typ = 'OPTBLUE PAYFAC'; end;        

if subm_se_no in ('1057969123','1052648524','1052648516','1052648508','1052648490','1052487998','1052487980','1052487964','1052487287','1052648532','1052487279',
'1052487261','1052487253','1052487246','2053159406','2053159356','2053159349','1052487238','1052487220','1052487212','1052487204','1052487196',
'1052487188','1052487170','1052487162','1052487154','1052487147','1052487121','1052487063','1052487105','1052487097','1052487089','1430745760',
'1430745604','1052487113','1341210805','1341210789','1058091232','1058091216','1058091208','6561619339','1058091190','1058091182','1058091174',
'1341210748','1341210755') then do; OB_PRTR_NM = 'TRANSFIRST';PORTFOLIO_TYPE  = 'OPTBLUE';    partner_typ = 'OPTBLUE TRADITIONAL'; end;              

if subm_se_no in ('2071861884','2071050199','2071050181','2070896097','2070896089','2070896071','2070896063','2070896055','2070896048','2070896030','2070896022','2070896014') then do; 
OB_PRTR_NM = 'EPX'; PORTFOLIO_TYPE = 'AGGREGATOR'; partner_typ = 'AGGREGATOR REPORTED'; ob_consider_ind =3; end;     

if subm_se_no in ('6569613854','6569613813','6569613805','6569613797','6569613789','6569613771','6569613730','6569613722','6569613714') then do; 
OB_PRTR_NM = 'PAYPAL PRO'; PORTFOLIO_TYPE = 'AGGREGATOR'; partner_typ = 'AGGREGATOR NOT-REPORTED'; ob_consider_ind =4; end;  

run;

PROC DATASETS LIB=WORK NOLIST;/*X*/
DELETE opd1;
RUN;

proc sort data=optblue_seller_report nodupkey;by se_no;run;
proc sort data=all_mer_seller6 out=all_mer_seller6 nodupkey;by se_no;run;

data all_mer_seller7(drop= tot_tenure DMA_DS_TX1);
merge optblue_seller_report all_mer_seller6;
by se_no;
where se_no ne '';
run;

PROC DATASETS LIB=WORK NOLIST;/*X*/
DELETE all_mer_seller6 optblue_seller_report;
RUN;


proc sql;
 create table all_mer_seller8 as
	Select m.*, 		
			case when open_year>&yr_ly3. then 2
				 when cancel_year >&yr_ly3. then 1 
				 else active_ind end as ly3_active_ind,
			case when open_year>&yr_ly2. then 2
				 when cancel_year >&yr_ly2. then 1 
				 else active_ind end as ly2_active_ind,
			case when open_year>&yr_ly. then 2
				 when cancel_year >&yr_ly. then 1 
				 else active_ind end as ly_active_ind
	from all_mer_seller7 as m
	where 	compress(se_no)  >'' and compress(se_no) <>'0'
;
quit;

PROC DATASETS LIB=WORK NOLIST;/*X*/
DELETE all_mer_seller7;
RUN;

/*Indexed the final dataset at SE_NO*/
data awb_out.all_mer_seller_bfr(index=(se_no)) ; 
	set all_mer_seller8;
run;

PROC DATASETS LIB=WORK NOLIST;/*X*/
DELETE all_mer_seller8;
RUN;

/* Code change for Prop-TPSPs dt 15Apr2021 */
 proc sql noprint;
select distinct "'"||trim(tpa_se_no)||"'" into:tpsp_agg separated by ',' 
from ramp_in.seller_hier_tpa where portfolio_type = 'PROP-TPSP';
quit;

DATA TPSP;
	SET gmwdm.glbl_mer_dim (WHERE=(MER_NO IN (&tpsp_agg.)));
RUN;

/*proc freq data=tpsp;*/
/*tables MER_HIER_LVL_NO/list;*/
/*run; */

DATA MAP CAP (KEEP= MER_ID MER_NO) SELL (KEEP= MER_ID MER_NO);
	SET TPSP;
	IF MER_HIER_LVL_NO = 8 THEN OUTPUT MAP;
	ELSE IF MER_HIER_LVL_NO = 6 THEN OUTPUT CAP;
	ELSE OUTPUT SELL;
RUN;

DATA SELL;
	SET SELL;
	brnd_toc_mer_no = MER_NO;
RUN;

DATA CAP;
	SET CAP;
	brnd_toc_mer_no = MER_NO;
RUN;

/*PULL CAPS FOR TPSPS AND FOR AGGREGATORS WITH HIGHEST LEVEL OF A CAP*/

DATA TPSP_CAPS;
	SET gmwdm.glbl_mer_dim (WHERE=(MER_HIER_LVL_NO=6 AND brnd_toc_mer_no IN (&tpsp_agg.)));
RUN;

/*FIND ALL LOCATIONS ASSOCIATED WITH THE CAPS*/
PROC SQL;
	CREATE TABLE TPSP_CAPS_LIF AS
	SELECT A.MER_ID AS MER_ID,A.PRIM_CAP_MER_NO, A.MER_NO AS MER_NO, B.brnd_toc_mer_no
	FROM GMWDMOPS.V_NA_MER_AFFL_DIM AS A
	INNER JOIN TPSP_CAPS AS B
	ON A.PRIM_CAP_MER_NO=B.MER_NO;
QUIT;

/*FIND ALL LOCATIONS ASSOCIATED WITH THE CAPS*/
PROC SQL;
	CREATE TABLE CAPS_LIF AS
	SELECT A.MER_ID AS MER_ID,A.PRIM_CAP_MER_NO, A.MER_NO AS MER_NO, B.brnd_toc_mer_no
	FROM GMWDMOPS.V_NA_MER_AFFL_DIM AS A
	INNER JOIN CAP AS B
	ON A.PRIM_CAP_MER_NO=B.MER_NO;
QUIT;

PROC SQL;
	CREATE TABLE MAP_LIF AS
	SELECT B.MER_ID, B.MER_NO, B.brnd_toc_mer_no
	FROM MAP AS A
	LEFT JOIN gmwdm.glbl_mer_dim AS B
	ON A.MER_NO = B.mkt_toc_mer_no or A.MER_NO = B.brnd_toc_mer_no ;
QUIT;

/*STACK THE TWO DATASETS TO ENSURE THAT LOCATIONS FOR THESE TPSPS WHICH ARE UNDER THEIR CAPS BUT A DIFFERENT MANAGED_TOC GET INCLUDED*/
DATA TPSP_2;
	SET MAP_LIF TPSP_CAPS_LIF SELL CAPS_LIF;
	IF MER_ID = '' THEN DELETE;
RUN;

/*DEDUPE*/
PROC SORT DATA=TPSP_2 OUT=awb_out.TPSP_2 NODUPKEY;
	BY MER_ID;
RUN;

proc sql;
create table TPSP_2_v2 as
select *, "Y" as tpsp_flag from awb_out.TPSP_2;
quit;

proc sql;
create table awb_out.all_mer_seller_with_tpsp_flag as
select a.*,b.tpsp_flag from 
awb_out.all_mer_seller_bfr a left join TPSP_2_v2 b
on a.se_no=b.mer_no;
quit;

data awb_out.all_mer_seller(drop=tpsp_flag);
set awb_out.all_mer_seller_with_tpsp_flag;
if tpsp_flag="Y" then do;
	portfolio_type  = 'PROP';
	partner_typ     = 'PROP-TPSP';
end;
run;

data seller_hier_tpa(keep=brnd_toc_mer_no partner_typ PORTFOLIO_TYPE partner_nm ob_consider_ind);
set ramp_in.seller_hier_tpa(where=(ob_consider_ind = 5) rename=(tpa_se_no=brnd_toc_mer_no));
run;

proc sort data=seller_hier_tpa nodupkey; by brnd_toc_mer_no;run;

proc sort data=awb_out.all_mer_seller nodupkey; by brnd_toc_mer_no;run;


data awb_out.all_mer_seller;
merge awb_out.all_mer_seller(in=a) seller_hier_tpa(in=b);
by brnd_toc_mer_no;
if a;
run;

/* Code change for Prop-TPSPs dt 15Apr2021  ENDS HERE*/

            



 
