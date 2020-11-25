/*Create Library*/

libname test 'F:\Kim';

data test.Ccris_entity_info;
 infile 'F:/Kim/IndividualSubject.txt' dlm=" " dsd truncover;
 input @1 FI_Number $20.
 @21 Name $200.
 @221 ID_1 $20.
 @241 ID_2 $20.
 @261 Date_of_Birth $10.
 @271 Basic_Group_Code $2.
 @273 Nationality $3.
 @276 Borrower_Unique_ID $15.
 @291 Dcheqs $15.
 @306 Start_Date $10.
 @316 End_Date $10.
;
run;

data test.ccris_sub_account; /* build SAS dataset */
 infile 'F:/Kim/CCRISSubAccountHistorical.txt' dlm=" "  dsd truncover; /* raw file in */
 input @1 Sub_Account_Unique_ID $15. 
 @16 Master_Account_Unique_ID  $15. 
 @31 Facility_Type $15.
 @46 repay_term $3.
 @49 Special_Fund_Scheme $1.
 @50 Created_Date $10.;
 run;
 
data test.ccris_Master_account_info; /* build SAS dataset */
 infile 'F:/Kim/CCRISMasterAccount.txt' dlm=" "  dsd truncover; /* raw file in */
 input @1 MASTER_ACCOUNT_ID $15. 
 @16 ENTITY_CODE  15. 
 @31 org_id 4.
 @35 org_id_2 $5.
 @40 Approved_Date mmddyy10. 
 @50 Capacity $10.
 @60 Lender_Type $9.
 @69 my_fgn $1.
 @70 LIMIT 15.
 @85 legal_status $5.
 @90 legal_status_date $10.
 @100 Special_Attention_Account   $1.
 @101 DATE_OF_CREATION mmddyy10.
;
 format Approved_Date mmddyy10.;
 format DATE_OF_CREATION mmddyy10.;
run;

data test.ccris_CreditPosition; /* build SAS dataset */
 infile 'F:/Kim/CCRISCreditPosition.txt' dlm=" "  dsd truncover; /* raw file in */
 input @1 MASTER_ACCOUNT_ID $15. 
 @16 SUB_ACCOUNT_ID $15. 
 @31 Account_Status $1.
 @32 BALANCE 15.
 @47 CREDIT_POSITION_DATE mmddyy10.
 @57 Number_of_Instalments_arrears  $3.
 @60 MONTH_IN_ARREARS 3.
 @63 INSTALLMENT_AMOUNT 15.
 @78 Restructured_Date $10.
 @88 Reschedule_Date $10.
 
;
 format CREDIT_POSITION_DATE mmddyy10.;
run;

/* Join Tables */

PROC SQL;
CREATE TABLE master as
select a.*,b.entity_code,b.Master_Account_Unique_ID,b.org_id,B.LIMIT,B.LEGAL_STATUS,b.LEGAL_STATUS_date,b.DATE_OF_CREATION
from Ccris_entity_info a left join Ccris_master_account_info b
on input(A.Borrower_Unique_ID,best.)=b.entity_code;
quit;

PROC SQL;
CREATE TABLE master_SUB as
select a.*,b.Master_Account_Unique_ID,b.Sub_Account_Unique_ID,B.Facility_Type
from master a left join Ccris_sub_account b
on a.Master_Account_ID=b.Master_Account_Unique_ID;
quit;

proc sql;
create table master_crpos_1 as
select a.*,b.Account_Status,B.BALANCE,B.CREDIT_POSITION_DATE,B.MONTH_IN_ARREARS,b.INSTALLMENT_AMOUNT,b.RESTRUCTURED_DATE,b.RESCHEDULE_DATE 
from master_SUB a left join Ccris_creditposition b
on a.Sub_Account_Unique_ID=b.Sub_Account_ID;
quit;

/* Access ODBC Server */
libname odbc1 odbc dsn=ctosmysql1;
libname odbc2 odbc dsn=ctosmysql2;
libname odbc3 odbc dsn=ctosmysql3;
libname odbc4 odbc dsn=ctosmysql4;
libname odbc5 odbc dsn=ctosmysql5;

/* Preliminary Analysis */

/* Import File */

FILENAME REFFILE 'F:/Kim/dev17.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=test.dev17;
	GETNAMES=YES;
RUN;

/* EDA - Frequency Descriptive Statistics */

proc freq data=test.dev17;
tables PL_TAG CC_TAG AL_TAG HL_TAG;
run;

proc means data=test.dev17;
var Total_outstanding_12pl Total_outstanding_12cc Total_outstanding_12al 
	Total_outstanding_12hl;
run;

title "One Sample Inference";
proc ttest data=sashelp.cars alpha=0.05 h0=0;
var _NUMERIC_;
run;
quit;

proc stdize data=sashelp.cars PCTLMTD=Ord_stat outstat=test2
			pctlpts=0.1, 25, 50, 75, 97.5, 99.9999;
var MSRP Invoice Horsepower;
run;


/* Model Development */

/* Data Partition */

proc sort data=test.dev17 out=dev17; by PL_TAG; run;

proc surveyselect data=dev17
	out=ds seed=1234
	method=srs samprate=0.7;
	strata PL_TAG;
run;

data ds;
set ds;
DATA_PARTITION=1;
run;

/* Merge Data Partition Tagging */
proc sort data=ds out=ds; by Borrower_Unique_ID; run;
proc sort data=dev17 out=dev17; by Borrower_Unique_ID; run;

data dev17;
	merge dev17 (in=a)
		  ds (in=b keep = Borrower_Unique_ID Data_Partition);
	by Borrower_Unique_ID;
	if a;
run;

data dev17;
set dev17;
if DATA_PARTITION=. then DATA_PARTITION=0; else DATA_PARTITION=1;
run;

/* Data Transformation */

proc hpbin data = dev17 output = dev17_BIN numbin=5 quantile;
   input Total_outstanding_12pl Total_outstanding_12cc Total_outstanding_12al 
	Total_outstanding_12hl;
   id Borrower_Unique_ID;
run;

proc sort data=dev17 out=dev17; by Borrower_Unique_ID; run;
proc sort data=dev17_BIN out=dev17_BIN; by Borrower_Unique_ID; run;

data dev17;
	merge dev17 (in=a)
		  dev17_BIN (in=b keep = Borrower_Unique_ID BIN_Total_outstanding_12pl BIN_Total_outstanding_12cc BIN_Total_outstanding_12al 
					BIN_Total_outstanding_12hl);
	by Borrower_Unique_ID;
	if a;
run;

/* Create Train and Test datasets */
/*Development*/
data dev;
set dev17;
where DATA_PARTITION=1;
run;

/* Hold-Out */
data ho;
set dev17;
where DATA_PARTITION=0;
run;

/* Run Logit model */
PROC LOGISTIC DATA=dev
		PLOTS(ONLY)=ALL;
	MODEL PL_TAG (Event = '1')=Total_outstanding_12pl Total_outstanding_12cc Total_outstanding_12al 
	Total_outstanding_12hl
/
SELECTION=STEPWISE
SLE=0.1
SLS=0.1
INCLUDE=0
LACKFIT
CTABLE
LINK=LOGIT;
RUN;

/* Store Model File */
PROC LOGISTIC DATA=dev outmodel=test.model
		PLOTS(ONLY)=ALL;
	MODEL PL_TAG (Event = '1')=
	Total_outstanding_12pl Total_outstanding_12cc Total_outstanding_12al 
	Total_outstanding_12hl/
LINK=LOGIT;
score data=dev out=dev_scored;
RUN;

/* Hold-Out Validation */

proc logistic inmodel=test.model (type=logismod);
	score data=ho out=ho_scored fitstat;
run;

/* ************************************************* */

data ccris_entity_info; set "F:\WJ_test\TwoMil\ccris_entity_info.sas7bdat"; run;
data ccris_master_account_info; set "F:\WJ_test\TwoMil\ccris_master_account_info.sas7bdat"; run;
data ccris_sub_account; set "F:\WJ_test\TwoMil\ccris_sub_account.sas7bdat"; run;
data ccris_CreditPosition; set odbc1.historical_ccris_credit_position; run;

proc contents data=ccris_entity_info; run;
proc contents data=ccris_master_account_info; run;
proc contents data=ccris_sub_account; run;
proc contents data=ccris_creditposition; run;

PROC SQL;
CREATE TABLE master as
select a.*,b.*
from Ccris_entity_info a left join Ccris_master_account_info b
on A.Borrower_Unique_ID=b.Borrower_Unique_ID;
quit;

PROC SQL;
CREATE TABLE master_SUB as
select a.*,b.Master_Account_Unique_ID,b.Sub_Account_Unique_ID,B.Facility_Type
from master a left join Ccris_sub_account b
on a.Master_Account_ID=b.Master_Account_Unique_ID;
quit;

proc sql;
create table master_crpos_1 as
select a.*,b.Account_Status,B.BALANCE,B.CREDIT_POSITION_DATE,B.MONTH_IN_ARREARS,b.INSTALLMENT_AMOUNT,b.RESTRUCTURED_DATE,b.RESCHEDULE_DATE 
from master_SUB a left join Ccris_creditposition b
on a.Sub_Account_Unique_ID=b.Sub_Account_ID;
quit;


