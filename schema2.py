import random
import sys
from enum import Enum
# from typing import OrderedDict, Union, List, TypedDict
import faker
from faker_enum import EnumProvider
import pymongo
from bson import json_util
from datetime import timedelta
import numpy as np
import time

fake = faker.Faker()
fake.add_provider( EnumProvider )
faker.Generator.seed(1111)

parties = [] # party IDs
accounts = [] # account numbers
banks = { } # routing numbers
for _ in range(1000000): parties.append( fake.iban() ) ; accounts.append( fake.bban() )
for _ in range(10): banks[ fake.aba() ] = fake.company()

currencies = { 'USD':0.95, 'CAD':0.03, 'EUR':0.02 }

class PARTY_TYPE(str, Enum):
    PERSON="PERSON"
    MERCHANT="MERCHANT"
    BANK="BANK"
    OTHER="OTHER"

class PRODUCT_CATEGORY(str, Enum):
    TRANSFER="TRANSFER"
    PAYMENT="PAYMENT"
    RECURRING="RECURRING"
    PURCHASE="PURCHASE"
    OTHER="OTHER"

class PAYMENT_STATUS(str, Enum):
    REQUEST="REQUEST"
    VALIDATE="VALIDATE"
    APPROVE="APPROVE"
    CLEARED="CLEARED"
    VOID="VOID"

# Payment Event
# 
# Payment Event Description
class PaymentEvent( dict ):
    #
    # Payment Identifier
    # 
    # required
    def __init__( self, instance: dict ):
        self['pmt_id']= lambda: fake.uuid4()
        # Client Party Identifier
        self['clnt_pty_id'] = lambda: fake.random_element( parties )
        # Client Account Number
        self['clnt_acct_no'] = lambda: fake.random_element( accounts )
        # Client Routing Number
        self['clnt_rte_no'] = lambda: random.choice(list(banks) )
        # Client GUID
        self['clnt_guid'] = lambda: fake.uuid4()
        # Counter Party Identifier
        self['cnt_pty_id'] = lambda: fake.random_element( parties )
        # Counter Party Account Number
        self['cnt_pty_acct_no'] = lambda: fake.random_element( accounts )
        # Counter Party Routing Number
        self['cnt_pty_rte_no'] = lambda: random.choice( list(banks) )
        # Counter Party GUID
        self['cnt_pty_guid'] = lambda: fake.uuid4()
        # Initiating Party Identifier
        self['init_pty_id'] = lambda: fake.random_element( parties )
        # Initiating  GUID
        self['init_guid'] = lambda: fake.uuid4()
        # Product Category Type
        self['ctgry_typ'] = lambda: fake.enum( PRODUCT_CATEGORY )
        # Product Category Name
        self['ctgry_nm'] = lambda: "NAME"
        # Product Name
        self['prod_nm'] = lambda: 'PRODUCT NAME'
        # Product Description
        self['prod_de'] = lambda: "PRODUCT DESC"
        # Source System Origination
        self['src_sys_org'] = lambda: fake.bothify('?#')
        # Source System Instance
        self['src_sys_inst'] = lambda: fake.bothify('###')
        # ODS Record Created Date
        self['ods_rec_crtd_dt'] = lambda: fake.past_datetime(start_date="-2y")
        # ODS Record Updated Date
        self['ods_rec_updt_dt'] = lambda: fake.date_time_between(start_date=instance['ods_rec_crtd_dt'], end_date='+10d')
        # Payment Reference Number
        self['pmt_ref_no'] = lambda: fake.bothify('??####-####')
        # End to End Identifier
        self['end_to_end_id'] = lambda: fake.numerify('######.##')
        # Debit Credit Code
        self['dr_cr_cd'] = lambda: fake.bothify('??####-####')
        # Payment Direction Code
        self['pmt_dir_cd'] = lambda: fake.bothify('??####-####')
        # Client Account Company Number
        self['clnt_acct_co_no'] = lambda: fake.numerify('######.##')
        # Client Account Cost Center Number
        self['clnt_acct_cc_no'] = lambda: fake.bban()
        # Client Account Application System Identifier
        self['clnt_acct_appsys_id'] = lambda: fake.bothify('??####-####')
        # Counter Party Account Company Number
        self['cnt_pty_acct_co_no'] = lambda: fake.bban()
        # Counter Party Account Cost Center Number
        self['cnt_pty_acct_cc_no'] = lambda: fake.credit_card_number()
        # Counter Party Account Application System Identifer
        self['cnt_pty_acct_appsys_id'] = lambda: fake.bothify('??####-####')
        # Debit Amount
        self['dr_am'] = lambda: np.random.logseries( 0.9990 ) * 1.
        # Debit Currency Code
        self['dr_curncy_cd'] = lambda: random.choices( list(currencies.keys()), weights = currencies.values(), k=1 )[0]
        # Credit Amount
        self['cr_am'] = lambda: np.random.logseries( 0.9990 ) * 1.
        # Credit Currency Code
        self['cr_curncy_cd'] = lambda: random.choices( list(currencies.keys()), weights = currencies.values(), k=1 )[0]
        # Payment Amount
        self['pmt_am'] = lambda: np.random.logseries( 0.9990 ) * 1.
        # Delivery Channel Code
        self['dlvry_chnl_cd'] = lambda: fake.bothify('??####-####')
        # Delivery Sub Channel Code
        self['dlvry_sub_chnl_cd'] = lambda: fake.bothify('??####-####')
        # Delivery Channel Reference Number
        self['dlvry_chnl_ref_no'] = lambda: fake.bothify('??####-####')
        # Payment Authorization Date Time
        self['pmt_auth_dt_tm'] = lambda: fake.date_time_between(start_date=instance['ods_rec_crtd_dt'], end_date='+1h')
        # Message External Reference Number
        self['msg_extnl_ref_no'] = lambda: fake.bothify('??####-####')
        # Delivery Channel Short Description
        self['dlvry_chnl_shrt_de'] = lambda: fake.bothify('??####-####')
        # Delivery Channel Description
        self['dlvry_chnl_de'] = lambda: fake.bothify('??####-####')
        # Source Record Created Date
        self['src_rec_crtd_dt'] = lambda: fake.date_time_between(start_date=instance['ods_rec_crtd_dt']-timedelta(minutes=1), end_date=instance['ods_rec_crtd_dt'])
        # Source Record Reference Number
        self['src_rec_ref_no'] = lambda: fake.numerify('######.##')
        # Source Record Last Update Date Time
        self['src_rec_lst_updt_dt_tm'] = lambda: instance['src_rec_crtd_dt'] + timedelta(minutes=1)
        # Transfer Type
        self['xfer_typ'] = lambda: fake.bothify('??####-####')
        # Amount Deductable Indicator
        self['am_deductable_in'] = lambda: fake.bothify('??####-####')
        # Component Code
        self['cmpnt_cd'] = lambda: fake.bothify('??####-####')
        # Confirmation Number
        self['cnfrm_no'] = lambda: fake.bothify('??####-####')
        # Consumer Code
        self['cns_cd'] = lambda: fake.bothify('??####-####')
        # Contract Media Recording Identifier
        self['cntrc_mdia_rec_id'] = lambda: fake.bothify('??####-####')
        # Disclosure Identification
        self['dscls_id'] = lambda: fake.bothify('??####-####')
        # Duplicate Override Indicator
        self['dup_ovrd_in'] = lambda: fake.bothify('??####-####')
        # Financial Center Device Identification
        self['fincl_ctr_devc_id'] = lambda: fake.bothify('??####-####')
        # Reversal Code
        self['rvrsl_cd'] = lambda: fake.bothify('??####-####')
        # Transfer Cardinality Code
        self['xfer_crdnt_cd'] = lambda: fake.bothify('??####-####')
        # Transfer Code Indicator
        self['xfer_cd_in'] = lambda: fake.bothify('??####-####')
        # Waiver Indicator
        self['wav_in'] = lambda: fake.bothify('??####-####')
        # Value Date
        self['val_dt'] = lambda: fake.past_date( "-2y" ).isoformat()
        # Payment Type
        self['pmt_typ'] = lambda: fake.bothify('?')
        # Transfer Mode Code
        self['xfer_mode_cd'] = lambda: fake.bothify('??')
        # Transaction Date
        # 
        # required
        self['tran_dt'] = lambda: fake.past_datetime(start_date="-2y")

        self['payment_status'] = ["_PaymentStatusItem"]
        self['pmt_charge'] = "_PmtChargeItem"
        self['payment_account_entry'] = "_AccountEntryItem"
        self['payment_execution'] = "_PmtExecutionItem"
        self['payment_party'] = "_PmtPartyItem"
# # }, total=False)


# Payment Account Entry
class _AccountEntryItem( dict ):
    # Payment Account Entry Identifier
    #
    def __init__( self, instaince: dict ):
        # required
        self['pmt_acc_entr_id'] = lambda: fake.bothify( '??####-####' )
        # Payment Identifier
        # 
        # required
        # self['pmt_id'] = lambda: fake.bothify( '??####-####' )
        # Source System Origination
        # self['src_sys_org'] = lambda: fake.bothify( '??####-####' )
        # Source System Instance
        # self['src_sys_inst'] = lambda: fake.bothify( '??####-####' )
        # ODS Record Created Date
        self['ods_rec_crtd_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # ODS Record Updated Date
        self['ods_rec_updt_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Processing Before Cutoff Indicator
        self['prcs_bfr_ctoff_in'] = lambda: fake.bothify( '??####-####' )
        # Processing Date
        self['prcs_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Posting Date
        self['pstng_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Sequence Number
        self['seq_no'] = lambda: fake.numerify('######.##')
        # Transaction Type
        self['tran_typ'] = lambda: fake.bothify( '??####-####' )
        # Target System Code
        self['trgt_sys_cd'] = lambda: fake.bothify( '??####-####' )
        # Memo Description Text
        self['memo_de_tx'] = lambda: fake.bs()
        # Posting Key Code
        self['pst_key_cd'] = lambda: fake.bothify( '??####-####' )
        # Posting Key Description
        self['pst_key_de'] = lambda: 'POSTING KEY DESC'
        # Transaction Amount
        self['tran_am'] = lambda: np.random.logseries( 0.9990 ) * 1.
        # Transaction Currency Code
        self['tran_curr_cd'] = lambda: random.choices( list(currencies.keys()), weights = currencies.values(), k=1 )[0]
# }, total=False)


# Payment Execution
class _PmtExecutionItem( dict ):
    # Payment Execution Identifier
    def __init__( self, instance: dict ):
        # 
        # required
        self['pmt_exeq_id'] = lambda: fake.bothify( '??####-####' )
        # Payment Identifier
        # 
        # required
        # self['pmt_id'] = lambda: fake.bothify( '??####-####' )
        # Source System Origination
        # self['src_sys_org'] = lambda: fake.bothify( '??####-####' )
        # Source System Instance
        # self['src_sys_inst'] = lambda: fake.bothify( '??####-####' )
        # ODS Record Created Date
        self['ods_rec_crtd_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # ODS Record Updated Date
        self['ods_rec_updt_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Notice of Change Code
        self['notc_of_chg_cd'] = lambda: fake.bothify( '??####-####' )
        # Change Reason Text
        self['chg_rs_tx'] = lambda: fake.bothify( '??####-####' )
        # Charge Bearer Type Code
        self['chrg_bear_typ_cd'] = lambda: fake.bothify( '??####-####' )
        # Payment Initiation Date Time
        self['pmt_initiation_dt_tm'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Payment Purpose Code
        self['pmt_prps_cd'] = lambda: fake.bothify( '??####-####' )
        # Process Type Code
        self['prcs_typ_cd'] = lambda: fake.bothify( '??####-####' )
        # Value Date Time
        self['val_dt_tm'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Availability Date
        self['availability_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Network Indicator
        self['ntwk_in'] = lambda: fake.bothify( '??####-####' )
        # Total Charge Amount
        self['tot_chrg_am'] = lambda: np.random.logseries( 0.9990 ) * 1.
        self['pmt_document'] = "_PmtDocumentItem"
    # }, total=False)


# Payment Document
class _PmtDocumentItem( dict ):
    # Payment Document Identifier
    def __init__( self, instance: dict ):
        # 
        # required
        self['pmt_doc_id'] = lambda: fake.bothify( '??####-####' )
        # Payment Execution Identifier
        # 
        # required
        self['pmt_exeq_id'] = lambda: fake.bothify( '??####-####' )
        # Payment Identifier
        # 
        # required
        # self['pmt_id'] = lambda: fake.bothify( '??####-####' )
        # Source System Origination
        # self['src_sys_org'] = lambda: fake.bothify( '??####-####' )
        # Source System Instance
        # self['src_sys_inst'] = lambda: fake.bothify( '??####-####' )
        # ODS Record Created Date
        # self['ods_rec_crtd_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # ODS Record Updated Date
        # self['ods_rec_updt_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Structured Payment Document Type Code
        self['strd_pmt_doc_typ_cd'] = lambda: fake.bothify( '??##' )
        # Structured Payment Information
        self['strd_pmt_info'] = lambda: fake.bothify( '??####-####' )
        # Structured Payment Document Date Time
        self['strd_pmt_doc_dt_tm'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Language
        self['lang'] = lambda: fake.random_element( ['EN','EN','EN','EN','ES','ES','CN','DE','IT'] )
        # UnStructured Payment Information
        self['ustrd_pmt_info'] = lambda: fake.bothify( '??####-####' )
        # UnStructured Payment Type
        self['unstrd_doc_typ'] = lambda: fake.bothify( '??' )
        # Structured Document Strata Identifier
        self['strd_doc_strata_id'] = lambda: fake.bothify( '??####-####' )
        # Structured Document Archived Indicator
        self['strd_doc_strata_arch_ind'] = lambda: fake.boolean()
        # Structured Document Archived Date
        self['strd_doc_strata_arch_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
    # }, total=False)


# Payment Party
class _PmtPartyItem( dict ):
    # Payment Party Identifier
    def __init__( self, instance: dict ):
        # 
        # required
        # self['pmt_pty_id'] = lambda: fake.uuid4()
        # Payment Identifier
        # 
        # required
        # self['pmt_id'] = lambda: fake.bothify( '??####-####-####' )
        # Routing Number
        # self['rte_no'] = lambda: fake.random_element( banks )
        # Party Identifier
        # self['pty_id'] = lambda: fake.random_element( parties )
        # Source System Origination
        # self['src_sys_org'] = lambda: fake.bothify( '???' )
        # Source System Instance
        # self['src_sys_inst'] = lambda: fake.bothify( '###' )
        # ODS Record Created Date
        # self['ods_rec_crtd_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # ODS Record Updated Date
        # self['ods_rec_updt_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Payment Party Role Type
        # 
        # required
        self['pmt_pty_role_typ'] = lambda: fake.bothify( '????' )
        # Address Line 1 Description
        self['addr_lin_1_de'] = lambda: fake.street_address()
        # Address Line 2 Description
        # self['addr_lin_2_de'] = lambda: fake.bothify( '??####-####' )
        # Address Line 3 Description
        # self['addr_lin_3_de'] = lambda: fake.bothify( '??####-####' )
        # Address Line 4 Description
        # self['addr_lin_4_de'] = lambda: fake.bothify( '??####-####' )
        # Address Line 5 Description
        # self['addr_lin_5_de'] = lambda: fake.bothify( '??####-####' )
        # Address Type Code
        self['addr_typ_cd'] = lambda: fake.random_element( ['A', 'B'] )
        # Country Sub Division Code
        # self['cntry_sub_div_cd'] = lambda: fake.bothify( '??####-####' )
        # Organization BIC
        self['org_bic'] = lambda: fake.bothify( '????????XXX' )
        # Party Identifier Type
        self['pty_id_typ'] = lambda: fake.random_element( ['P','B','M', 'O'] )
        # Party Name
        self['pty_nm'] = lambda: fake.name()
        # Post Code
        self['pst_cd'] = lambda: fake.postcode( )
        # Postal Country Code
        self['pstl_cntry_cd'] = lambda: fake.country_code()
        # Person Reference Number
        # self['prsn_ref_no'] = lambda: fake.bothify( '??####-####' )
        # Town Name
        self['town_nm'] = lambda: fake.city()
        # Postal Country Name
        self['pstl_cntry_nm'] = lambda: fake.country()
        # Country Sub Division Name
        # self['cntry_sub_div_nm'] = lambda: fake.bothify( '??####-####' )
        # Party Contact Info Text
        # self['pty_cntct_info_tx'] = lambda: fake.bothify( '??####-####' )
        # Internal Party Number
        # self['intrnl_pty_no'] = lambda: fake.bothify( '??####-####' )
        # National Bank Identifier
        # self['nbkid'] = lambda: fake.random_element( banks )
        # Clearing System Reference Type
        self['clr_sys_ref_typ'] = lambda: fake.bothify( '??' )
        # Cross Reference Code
        self['xref_cd'] = lambda: fake.bothify( '??####-####' )
        # Bank America Corporation Online Code
        self['bac_onln_cd'] = lambda: fake.bothify( '??####-####' )
        # GUID
        self['guid'] = lambda: fake.uuid4()
        # Account Title
        self['acct_titl'] = lambda: fake.last_name( ) + ' main account'
        # Account Status Code
        self['acct_stat_cd'] = lambda: fake.bothify( '?' )
        # Account Type Description
        self['acct_typ_de'] = lambda: fake.random_element( ['CHECKING', 'SAVINGS', 'MKT', 'BROKERAGE', 'CASH', 'CC', 'ONLINE', 'OTHER'] )
        # Account Name
        self['acct_nm'] = lambda: fake.random_element( accounts )
        # Account Type
        self['acct_typ'] = lambda: fake.random_element( ['CHECKING', 'SAVINGS', 'MKT', 'BROKERAGE', 'CASH', 'CC', 'ONLINE', 'OTHER'] )
        # Alternate Account Indentifier
        self['alt_acct_id'] = lambda: fake.bothify( '??####-####' )
        # Alternate Account Identifier Type
        self['alt_acct_id_typ'] = lambda: fake.bothify( '??####-####' )
        # Alternate Account Number
        self['alt_acct_no'] = lambda: fake.bothify( '??####-####' )
        # Alternate Account Type
        self['alt_acct_typ'] = lambda: fake.bothify( '??##' )
        # Alternate Entity Code
        self['alt_enty_cd'] = lambda: fake.bothify( '??' )
        # Alternate Product Code
        self['alt_prod_cd'] = lambda: fake.bothify( '????' )
        # Product Code
        self['prod_cd'] = lambda: fake.bothify( '????' )
        # Sub Product Code
        self['sub_prod_cd'] = lambda: fake.bothify( '?#' )
        # Account Expiry Month Year Text
        self['acct_expry_mo_yr_tx'] = lambda: fake.future_date( end_date='+5y' ).strftime( "%m/%Y")
        # Account Ownership Flag
        self['acct_own_fl'] = lambda: fake.boolean()
        # Account Authoriztion Code
        self['acct_auth_cd'] = lambda: fake.bothify( '??####-####' )
        # Account Authorization Network Code
        self['acct_auth_ntwk_cd'] = lambda: fake.bothify( '??####' )
        # Account Reference Number
        self['acct_ref_no'] = lambda: fake.bothify( '??####-####-####' )
        # Available Balance Amount
        self['avbl_bal_am'] = lambda: float(fake.numerify('#####.##'))
        # Bank of America Corporation Ownership Indicator
        self['bac_corp_own_in'] = lambda: fake.bothify( '?' )
        # Entity Code
        self['enty_cd'] = lambda: fake.bothify( '??##' )
        # Sub System Identifier
        self['sub_sys_id'] = lambda: fake.bothify( '??####-####' )
        # System Identifier
        self['sys_id'] = lambda: fake.bothify( '??####-####' )
    # }, total=False)


class _PaymentStatusItem( dict ):
    # Payment Status Identifier
    def __init__( self, instance: dict ):
        # 
        # required
        self['pmt_stat_id'] = lambda: fake.random_element( PAYMENT_STATUS )
        # Payment Identifier
        # 
        # required
        # self['pmt_id'] = lambda: fake.bothify( '??####-####' )
        # Source System Origination
        # self['src_sys_org'] = lambda: fake.bothify( '??####-####' )
        # Source System Instance
        # self['src_sys_inst'] = lambda: fake.bothify( '??####-####' )
        # ODS Record Created Date
        self['ods_rec_crtd_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # ODS Record Updated Date
        self['ods_rec_updt_dt'] = lambda: fake.past_datetime( start_date = "-2y" )
        # Payment Status Code
        self['pmt_stat_cd'] = lambda: fake.bothify( '?' )
        # Payment Status Description
        self['pmt_stat_de'] = lambda: "STATUS DESC"
        # Payment Status Additional Description
        self['pmt_stat_add_de'] = lambda: fake.bs()
        # Event Date Time
        self['ev_dt_tm'] = lambda: fake.past_datetime( start_date = "-2y" )
    # }, total=False)


# Payment Charge
class _PmtChargeItem( dict ):
    # Payment Charge Identifier
    def __init__( self, instance: dict ):
        # 
        # required
        self['pmt_chrg_id'] = lambda: fake.bothify( '??####-####' )
        # Payment Identifier
        # 
        # required
        self['pmt_id'] = lambda: fake.bothify( '??####-####' )
        # Source System Origination
        self['src_sys_org'] = lambda: fake.bothify( '??####-####' )
        # Source System Instance
        self['src_sys_inst'] = lambda: fake.bothify( '??####-####' )
        # ODS Record Created Date
        self['ods_rec_crtd_dt'] = lambda: fake.bothify( '??####-####' )
        # ODS Record Updated Date
        self['ods_rec_updt_dt'] = lambda: fake.bothify( '??####-####' )
        # Bearer Type
        # 
        # required
        self['bear_typ'] = lambda: fake.bothify( '??####-####' )
        # Calculation Basis Type
        self['calc_bas_typ'] = lambda: fake.bothify( '??####-####' )
        # Charge Payment Method
        self['chrg_pmt_mthd'] = lambda: fake.bothify( '??####-####' )
        # Charge Type
        # 
        # required
        self['chrg_typ'] = lambda: fake.bothify( '??####-####' )
        # Sub Charge Type
        self['sub_chrg_typ'] = lambda: fake.bothify( '??####-####' )
        # Charge Amount
        self['chrg_amt'] = lambda: np.random.logseries( 0.9990 ) * 1.
        # Currency Code
        self['curncy_cd'] = lambda: random.choices( list(currencies.keys()), weights = currencies.values(), k=1 )[0]
        # Exchange Rate
        self['exhng_rt'] = lambda: fake.numerify('##.##')
        # Charge Waiver Indicator
        self['chrg_wav_in'] = lambda: fake.bothify( '??####-####' )
        # Charge Waiver Reason Text
        self['chrg_wav_rsn_tx'] = lambda: fake.bothify( '??####-####' )
        # }, total=False)


# Flat Payment Event from PoC Team - 2022-08-30
class SimplePayment( dict ):

    def __init__( self, instance: dict ):
        # self[ "_id" ] = lambda: fake.uuid4() # "44f50112-7f54-4b19-bf6d-54dddb398fad",
        self[ "_id" ] = lambda: fake.numerify( "333###############################" ) # "3332454055433332454087121661460702",
        # self[ "PDS_TRANS_ID" ] = lambda: fake.numerify( "333###############################" ) # "3332454055433332454087121661460702",
        # self[ "PDS_TRANS_ID" ] = lambda: fake.uuid4() # "3332454055433332454087121661460702",
        self[ "REQ_TS" ] = lambda: fake.past_datetime( start_date = "-2y" ) # "2022-08-25 20:51:42.596427",
        self[ "INITIATION_TS" ] = lambda: fake.date_time_between(start_date=instance['REQ_TS'], end_date='+1m') # "2022-08-25 20:51:42.596427",
        self[ "PDS_MOD_TS" ] = lambda: fake.date_time_between(start_date=instance['INITIATION_TS'], end_date='+1d') # "2022-08-25 20:51:42.596427",
        self[ "EARLIST_XTRCT_TS" ] = lambda: fake.date_time_between(start_date=instance['INITIATION_TS'], end_date='+2d') # "2022-08-25 20:51:42.596427",
        self[ "XFER_AM" ] = lambda: np.random.logseries( 0.9990 ) # "494",

        self[ "INIT_GUID" ] = lambda: fake.uuid4() # "b95e7655-24b7-11ed-99da-15caf1cc7744",
        self[ "INIT_FRST_NM" ] = lambda: fake.first_name() # Scott
        self[ "INIT_LAST_NM" ] = lambda: fake.last_name() # "Woodard",
        self[ "INIT_BOA_PTY_ID" ] = lambda: random.choice( parties ) # 2289845,
        self[ "SRC_TRNFR_ID" ] = lambda: fake.uuid4() # "b95e7654-24b7-11ed-99da-15caf1cc7744",

        self[ "FR_BOA_PTY_ID" ] = lambda: random.choice( parties ) # "2289845",
        self[ "FR_CURR_EXHG_ID" ] = lambda: "USD"
        self[ "FR_STP_TYP" ] = lambda: "Debit"
        self[ "FR_RTE_NO" ] = lambda: random.choice( list(banks.keys()) ) # "92829839",
        self[ "FR_ACCT_NUM" ] = lambda: random.choice( accounts ) # "333245405543",
        self[ "FR_ACCT_ID" ] = lambda: "6314"
        self[ "FR_FRST_NM" ] = lambda: fake.first_name() # "Scott",
        self[ "FR_LAST_NM" ] = lambda: fake.last_name() # "Woodard",
        self[ "FR_PTY_ADDR_1" ] = lambda: fake.street_address() # "192 Wall Inlet",
        self[ "FR_PTY_ADDR_2" ] = lambda: ""
        self[ "FR_PTY_PST_CD" ] = lambda: fake.postcode() # "71025",
        self[ "FR_PTY_CTY" ] = lambda: fake.city() # "North Joel",
        self[ "FR_PTY_STA" ] = lambda: fake.state() # "Vermont",
        self[ "FR_CNTRY_CD" ] = lambda: "US"
        self[ "FR_PTY_PHN_NO" ] = lambda: fake.phone_number() # "287-269-0861x782",
        self[ "FR_ACC_CTY_CD" ] = lambda: "SAV"
        self[ "FR_SUB_ROD_CD" ] = lambda: "REGS"
        self[ "FR_NBID" ] = lambda: fake.bothify( "???????" ) # "3uWkt6X",
        self[ "FR_PROD_CD" ] = lambda: random.choice( ["CSH", "CRD"] ) # "CSH",
        self[ "FR_CURR_BAL" ] = lambda: np.random.randint( 200000 ) # "144944",
        self[ "FR_ACC_BAL" ] = lambda: instance[ 'FR_CURR_BAL' ] - instance[ 'XFER_AM' ] # 144944,

        self[ "TO_GUID" ] = lambda: fake.uuid4() # "b95e7657-24b7-11ed-99da-15caf1cc7744",
        self[ "TO_RTE_NO" ] = lambda: random.choice( list( banks.keys() ) ) # 33651996,
        self[ "TO_ACCT_NUM" ] = lambda: random.choice( accounts ) # "333245408712",
        self[ "TO_ACCT_ID" ] = lambda: "9483"
        self[ "TO_CURR_EXHG_ID" ] = lambda: "USD"
        self[ "TO_BOA_PTY_ID" ] = lambda: random.choice( parties ) # "2293014",
        self[ "TO_FRST_NM" ] = lambda: fake.first_name() # "Ann",
        self[ "TO_LAST_NM" ] = lambda: fake.last_name() # Markham
        self[ "TO_PTY_ADDR_1" ] = lambda: fake.street_address() # "0403 Marc Spring Suite 311",
        self[ "TO_PTY_ADDR_2" ] = lambda: ""
        self[ "TO_PTY_CTY" ] = lambda: fake.city() # "Tiffanyton",
        self[ "TO_PTY_STA" ] = lambda: fake.state() # "New Jersey",
        self[ "TO_PTY_PST_CD" ] = lambda: fake.postcode() # "76736",
        self[ "TO_CNTRY_CD" ] = lambda: "US"
        self[ "TO_PTY_PHN_NO" ] = lambda: fake.phone_number() # "(231)233-4101x662",
        self[ "TO_PROD_CD" ] = lambda: "IML"
        self[ "TO_SUB_ROD_CD" ] = lambda: "LC"
        self[ "TO_NBID" ] = lambda: "k6cD1Mx"
        self[ "TO_CURR_BAL" ] = lambda: np.random.randint( 200000 ) # "147395",

        self[ "ORIG_AIT_CD" ] = lambda: fake.bothify( "######" ) # "65256",
        self[ "DUP_OVRD_FL" ] = lambda: random.choice( ["Y","N"] ) # "N",
        self[ "XFER_CD" ] = lambda: random.choice( [ "A","B","C","D","E","F" ] ) # "F",
        self[ "PRCS_TYP_CD" ] = lambda: random.choice( ["ST", "CA", "WD", "RF"] ) # "ST",
        self[ "SUB_CHN_CD" ] = lambda: random.choice( [ "REC", "RET", "WTD" ] ) # "RET",
        self[ "RVRSL_CD" ] = lambda: ""
        self[ "CHN_CD" ] = lambda: random.choice( [ 'BKA', 'CCC', 'IVR', 'Mobile', 'Online' ] )
        self[ "ORIG_CNFRM_NO" ] = lambda: ""
        self[ "PMT_DIR_CD" ] = lambda: "ON_US"
        self[ "CMP_CD" ] = lambda: ""
        self[ "XFER_CRNDT" ] = lambda: "ONE_TO_ONE"
        self[ "TRAC_ID" ] = lambda: ""
        self[ "CO_CD" ] = lambda: "0701"
        self[ "CNFRM_NO" ] = lambda: ""
        self[ "ALPH_NMRC_CNFRM_NO" ] = lambda: ""
        self[ "SRC_SEC_ID" ] = lambda: ""
        self[ "COST_CTR+CD" ] = lambda: "88888"
        self[ "CNS_CD" ] = lambda: "59295"


def generateOne():
    instance = {}
    # doc = PaymentEvent()
    doc = SimplePayment( instance )

    # loopOverKeys( doc )
    genDictionaryValues( doc, instance )
    print( json_util.dumps( instance, indent=2 ))

def returnDocs( count=10 ):
    result = []
    for x in range( count ):
        result.append( returnOne() )
    return result

def returnOne():
    instance = {}
    # doc = PaymentEvent( instance )
    doc = SimplePayment( instance )
    genDictionaryValues( doc, instance )
    return instance

def genDictionaryValues( doc : dict, instance : dict ) :
    for k in doc.keys():
        if isinstance( doc[k], dict ):
            instance[ k ] = {}
            genDictionaryValues( doc[k], instance[k] )
            # for l in d[k].keys():
            #     instance[ k ][ l ] = d[k][l]()
        elif isinstance( doc[k], list ):
            count = fake.random_int( min = 0, max = 5 )
            if( count ) :
                instance[ k ] = []
                klass = globals()[ doc.get(k)[0] ]
                for i in range( count ):
                    instance[k].append({})
                    doc2 = klass( instance[k][i] )
                    genDictionaryValues( doc2, instance[k][i] )
        elif type( doc.get( k )) is str:
            # print( "Instantiate class and loop over")
            klass = globals()[ doc.get(k) ]
            instance[ k ] = {}
            doc2 = klass( instance[k] )
            genDictionaryValues( doc2, instance[k] )
        else :
            instance[ k ] = doc[k]() # call the lambda function to generate that value
            if instance[ k ] == None : instance.pop( k ) # remove keys that don't get assigned a value


if __name__ == "__main__":
    batch = []
    size = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    start = time.time()
    for _ in range( size ):
        instance = {}
        # doc = PaymentEvent( instance ) # pass in instance as state context
        doc = SimplePayment( instance )
        genDictionaryValues( doc, instance )
        # instance.pop('previous_doc')
        # if( fake.random_int(min=1, max=3) == 1 ) : instance.pop( 'history', None )
        # if( fake.boolean == True ) : instance.pop( 'previous', None )
        batch.append( instance )
        if (_ > 0 and len(batch) % 1000 == 0):
            end = time.time()
            delta = end - start
            for instance in batch : print( json_util.dumps( instance ) )
            print( f"Time for batch {delta}, generated/sec={len(batch)/delta}", file=sys.stderr )
            batch = []
            start = time.time()
    if len( batch):
        for instance in batch :
            # print( str( instance ) )
            print( json_util.dumps( instance ) )
