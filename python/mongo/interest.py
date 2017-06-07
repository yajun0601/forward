
import pandas as pd     
from numpy  import * 
from pymongo import *
import json


dest_client = MongoClient('mongodb://127.0.0.1:27017/')
dest_db = dest_client.bonds
dest_data = dest_db.issuers
COMMON_RATIO = pd.DataFrame()
data = pd.DataFrame(list(dest_db.bond_balance.find()))

COMMON_RATIO['code'] = data[data['rptDate'] == '20151231']['code']

data = pd.DataFrame(list(dest_db.bond_balance.find()))
COMMON_RATIO['MONETARY_CAP_20151231'] = data[data['rptDate'] == '20151231']['MONETARY_CAP'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['TRADABLE_FIN_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['TRADABLE_FIN_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['NOTES_RCV_20151231'] = data[data['rptDate'] == '20151231']['NOTES_RCV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['ACCT_RCV_20151231'] = data[data['rptDate'] == '20151231']['ACCT_RCV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['OTH_RCV_20151231'] = data[data['rptDate'] == '20151231']['OTH_RCV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['PREPAY_20151231'] = data[data['rptDate'] == '20151231']['PREPAY'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['DVD_RCV_20151231'] = data[data['rptDate'] == '20151231']['DVD_RCV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['INT_RCV_20151231'] = data[data['rptDate'] == '20151231']['INT_RCV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['INVENTORIES_20151231'] = data[data['rptDate'] == '20151231']['INVENTORIES'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['CONSUMPTIVE_BIO_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['CONSUMPTIVE_BIO_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['DEFERRED_EXP_20151231'] = data[data['rptDate'] == '20151231']['DEFERRED_EXP'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['HFS_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['HFS_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['NON_CUR_ASSETS_DUE_WITHIN_1Y_20151231'] = data[data['rptDate'] == '20151231']['NON_CUR_ASSETS_DUE_WITHIN_1Y'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['SETTLE_RSRV_20151231'] = data[data['rptDate'] == '20151231']['SETTLE_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['LOANS_TO_OTH_BANKS_20151231'] = data[data['rptDate'] == '20151231']['LOANS_TO_OTH_BANKS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['MARGIN_ACCT_20151231'] = data[data['rptDate'] == '20151231']['MARGIN_ACCT'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['PREM_RCV_20151231'] = data[data['rptDate'] == '20151231']['PREM_RCV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RCV_FROM_REINSURER_20151231'] = data[data['rptDate'] == '20151231']['RCV_FROM_REINSURER'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RCV_FROM_CEDED_INSUR_CONT_RSRV_20151231'] = data[data['rptDate'] == '20151231']['RCV_FROM_CEDED_INSUR_CONT_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RED_MONETARY_CAP_FOR_SALE_20151231'] = data[data['rptDate'] == '20151231']['RED_MONETARY_CAP_FOR_SALE'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['TOT_ACCT_RCV_20151231'] = data[data['rptDate'] == '20151231']['TOT_ACCT_RCV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['OTH_CUR_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['OTH_CUR_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['TOT_CUR_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['TOT_CUR_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['FIN_ASSETS_AVAIL_FOR_SALE_20151231'] = data[data['rptDate'] == '20151231']['FIN_ASSETS_AVAIL_FOR_SALE'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['HELD_TO_MTY_INVEST_20151231'] = data[data['rptDate'] == '20151231']['HELD_TO_MTY_INVEST'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['INVEST_REAL_ESTATE_20151231'] = data[data['rptDate'] == '20151231']['INVEST_REAL_ESTATE'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['LONG_TERM_EQY_INVEST_20151231'] = data[data['rptDate'] == '20151231']['LONG_TERM_EQY_INVEST'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['LONG_TERM_REC_20151231'] = data[data['rptDate'] == '20151231']['LONG_TERM_REC'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['FIX_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['FIX_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['PROJ_MATL_20151231'] = data[data['rptDate'] == '20151231']['PROJ_MATL'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['CONST_IN_PROG_20151231'] = data[data['rptDate'] == '20151231']['CONST_IN_PROG'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['FIX_ASSETS_DISP_20151231'] = data[data['rptDate'] == '20151231']['FIX_ASSETS_DISP'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['PRODUCTIVE_BIO_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['PRODUCTIVE_BIO_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['OIL_AND_NATURAL_GAS_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['OIL_AND_NATURAL_GAS_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['INTANG_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['INTANG_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['R_AND_D_COSTS_20151231'] = data[data['rptDate'] == '20151231']['R_AND_D_COSTS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['GOODWILL_20151231'] = data[data['rptDate'] == '20151231']['GOODWILL'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['LONG_TERM_DEFERRED_EXP_20151231'] = data[data['rptDate'] == '20151231']['LONG_TERM_DEFERRED_EXP'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['DEFERRED_TAX_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['DEFERRED_TAX_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['LOANS_AND_ADV_GRANTED_20151231'] = data[data['rptDate'] == '20151231']['LOANS_AND_ADV_GRANTED'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['OTH_NON_CUR_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['OTH_NON_CUR_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['TOT_NON_CUR_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['TOT_NON_CUR_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['CASH_DEPOSITS_CENTRAL_BANK_20151231'] = data[data['rptDate'] == '20151231']['CASH_DEPOSITS_CENTRAL_BANK'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['AGENCY_BUS_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['AGENCY_BUS_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RCV_INVEST_20151231'] = data[data['rptDate'] == '20151231']['RCV_INVEST'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['ASSET_DEP_OTH_BANKS_FIN_INST_20151231'] = data[data['rptDate'] == '20151231']['ASSET_DEP_OTH_BANKS_FIN_INST'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['PRECIOUS_METALS_20151231'] = data[data['rptDate'] == '20151231']['PRECIOUS_METALS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RCV_CEDED_UNEARNED_PREM_RSRV_20151231'] = data[data['rptDate'] == '20151231']['RCV_CEDED_UNEARNED_PREM_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RCV_CEDED_CLAIM_RSRV_20151231'] = data[data['rptDate'] == '20151231']['RCV_CEDED_CLAIM_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RCV_CEDED_LIFE_INSUR_RSRV_20151231'] = data[data['rptDate'] == '20151231']['RCV_CEDED_LIFE_INSUR_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['RCV_CEDED_LT_HEALTH_INSUR_RSRV_20151231'] = data[data['rptDate'] == '20151231']['RCV_CEDED_LT_HEALTH_INSUR_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['INSURED_PLEDGE_LOAN_20151231'] = data[data['rptDate'] == '20151231']['INSURED_PLEDGE_LOAN'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['CAP_MRGN_PAID_20151231'] = data[data['rptDate'] == '20151231']['CAP_MRGN_PAID'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['INDEPENDENT_ACCT_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['INDEPENDENT_ACCT_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['TIME_DEPOSITS_20151231'] = data[data['rptDate'] == '20151231']['TIME_DEPOSITS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['SUBR_REC_20151231'] = data[data['rptDate'] == '20151231']['SUBR_REC'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['MRGN_PAID_20151231'] = data[data['rptDate'] == '20151231']['MRGN_PAID'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['SEAT_FEES_EXCHANGE_20151231'] = data[data['rptDate'] == '20151231']['SEAT_FEES_EXCHANGE'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['CLIENTS_CAP_DEPOSIT_20151231'] = data[data['rptDate'] == '20151231']['CLIENTS_CAP_DEPOSIT'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['CLIENTS_RSRV_SETTLE_20151231'] = data[data['rptDate'] == '20151231']['CLIENTS_RSRV_SETTLE'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['OTH_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['OTH_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['DERIVATIVE_FIN_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['DERIVATIVE_FIN_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_ASSETS'].values
COMMON_RATIO['ST_BORROW_20151231'] = data[data['rptDate'] == '20151231']['ST_BORROW'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['TRADABLE_FIN_LIAB_20151231'] = data[data['rptDate'] == '20151231']['TRADABLE_FIN_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['NOTES_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['NOTES_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['ACCT_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['ACCT_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['ADV_FROM_CUST_20151231'] = data[data['rptDate'] == '20151231']['ADV_FROM_CUST'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['EMPL_BEN_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['EMPL_BEN_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['TAXES_SURCHARGES_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['TAXES_SURCHARGES_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['TOT_ACCT_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['TOT_ACCT_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['INT_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['INT_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DVD_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['DVD_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['OTH_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['OTH_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['ACC_EXP_20151231'] = data[data['rptDate'] == '20151231']['ACC_EXP'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DEFERRED_INC_CUR_LIAB_20151231'] = data[data['rptDate'] == '20151231']['DEFERRED_INC_CUR_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['HFS_LIAB_20151231'] = data[data['rptDate'] == '20151231']['HFS_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['NON_CUR_LIAB_DUE_WITHIN_1Y_20151231'] = data[data['rptDate'] == '20151231']['NON_CUR_LIAB_DUE_WITHIN_1Y'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['ST_BONDS_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['ST_BONDS_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['BORROW_CENTRAL_BANK_20151231'] = data[data['rptDate'] == '20151231']['BORROW_CENTRAL_BANK'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DEPOSIT_RECEIVED_IB_DEPOSITS_20151231'] = data[data['rptDate'] == '20151231']['DEPOSIT_RECEIVED_IB_DEPOSITS'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['LOANS_OTH_BANKS_20151231'] = data[data['rptDate'] == '20151231']['LOANS_OTH_BANKS'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['FUND_SALES_FIN_ASSETS_RP_20151231'] = data[data['rptDate'] == '20151231']['FUND_SALES_FIN_ASSETS_RP'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['HANDLING_CHARGES_COMM_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['HANDLING_CHARGES_COMM_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['PAYABLE_TO_REINSURER_20151231'] = data[data['rptDate'] == '20151231']['PAYABLE_TO_REINSURER'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['RSRV_INSUR_CONT_20151231'] = data[data['rptDate'] == '20151231']['RSRV_INSUR_CONT'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['ACTING_TRADING_SEC_20151231'] = data[data['rptDate'] == '20151231']['ACTING_TRADING_SEC'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['ACTING_UW_SEC_20151231'] = data[data['rptDate'] == '20151231']['ACTING_UW_SEC'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['OTH_CUR_LIAB_20151231'] = data[data['rptDate'] == '20151231']['OTH_CUR_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['TOT_CUR_LIAB_20151231'] = data[data['rptDate'] == '20151231']['TOT_CUR_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['LT_BORROW_20151231'] = data[data['rptDate'] == '20151231']['LT_BORROW'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['BONDS_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['BONDS_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['LT_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['LT_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['LT_EMPL_BEN_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['LT_EMPL_BEN_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['SPECIFIC_ITEM_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['SPECIFIC_ITEM_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['PROVISIONS_20151231'] = data[data['rptDate'] == '20151231']['PROVISIONS'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DEFERRED_TAX_LIAB_20151231'] = data[data['rptDate'] == '20151231']['DEFERRED_TAX_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DEFERRED_INC_NON_CUR_LIAB_20151231'] = data[data['rptDate'] == '20151231']['DEFERRED_INC_NON_CUR_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['OTH_NON_CUR_LIAB_20151231'] = data[data['rptDate'] == '20151231']['OTH_NON_CUR_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['TOT_NON_CUR_LIAB_20151231'] = data[data['rptDate'] == '20151231']['TOT_NON_CUR_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['LIAB_DEP_OTH_BANKS_FIN_INST_20151231'] = data[data['rptDate'] == '20151231']['LIAB_DEP_OTH_BANKS_FIN_INST'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['AGENCY_BUS_LIAB_20151231'] = data[data['rptDate'] == '20151231']['AGENCY_BUS_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['CUST_BANK_DEP_20151231'] = data[data['rptDate'] == '20151231']['CUST_BANK_DEP'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['CLAIMS_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['CLAIMS_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DVD_PAYABLE_INSURED_20151231'] = data[data['rptDate'] == '20151231']['DVD_PAYABLE_INSURED'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DEPOSIT_RECEIVED_20151231'] = data[data['rptDate'] == '20151231']['DEPOSIT_RECEIVED'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['INSURED_DEPOSIT_INVEST_20151231'] = data[data['rptDate'] == '20151231']['INSURED_DEPOSIT_INVEST'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['UNEARNED_PREM_RSRV_20151231'] = data[data['rptDate'] == '20151231']['UNEARNED_PREM_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['OUT_LOSS_RSRV_20151231'] = data[data['rptDate'] == '20151231']['OUT_LOSS_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['LIFE_INSUR_RSRV_20151231'] = data[data['rptDate'] == '20151231']['LIFE_INSUR_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['LT_HEALTH_INSUR_V_20151231'] = data[data['rptDate'] == '20151231']['LT_HEALTH_INSUR_V'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['INDEPENDENT_ACCT_LIAB_20151231'] = data[data['rptDate'] == '20151231']['INDEPENDENT_ACCT_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['PREM_RECEIVED_ADV_20151231'] = data[data['rptDate'] == '20151231']['PREM_RECEIVED_ADV'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['PLEDGE_LOAN_20151231'] = data[data['rptDate'] == '20151231']['PLEDGE_LOAN'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['ST_FINL_INST_PAYABLE_20151231'] = data[data['rptDate'] == '20151231']['ST_FINL_INST_PAYABLE'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['OTH_LIAB_20151231'] = data[data['rptDate'] == '20151231']['OTH_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['DERIVATIVE_FIN_LIAB_20151231'] = data[data['rptDate'] == '20151231']['DERIVATIVE_FIN_LIAB'].values / data[data['rptDate'] == '20151231']['TOT_LIAB'].values
COMMON_RATIO['CAP_STK_20151231'] = data[data['rptDate'] == '20151231']['CAP_STK'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['OTHER_EQUITY_INSTRUMENTS_20151231'] = data[data['rptDate'] == '20151231']['OTHER_EQUITY_INSTRUMENTS'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['OTHER_EQUITY_INSTRUMENTS_PRE_20151231'] = data[data['rptDate'] == '20151231']['OTHER_EQUITY_INSTRUMENTS_PRE'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['CAP_RSRV_20151231'] = data[data['rptDate'] == '20151231']['CAP_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['SURPLUS_RSRV_20151231'] = data[data['rptDate'] == '20151231']['SURPLUS_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['UNDISTRIBUTED_PROFIT_20151231'] = data[data['rptDate'] == '20151231']['UNDISTRIBUTED_PROFIT'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['TSY_STK_20151231'] = data[data['rptDate'] == '20151231']['TSY_STK'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['OTHER_COMPREH_INC_BS_20151231'] = data[data['rptDate'] == '20151231']['OTHER_COMPREH_INC_BS'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['SPECIAL_RSRV_20151231'] = data[data['rptDate'] == '20151231']['SPECIAL_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['PROV_NOM_RISKS_20151231'] = data[data['rptDate'] == '20151231']['PROV_NOM_RISKS'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['CNVD_DIFF_FOREIGN_CURR_STAT_20151231'] = data[data['rptDate'] == '20151231']['CNVD_DIFF_FOREIGN_CURR_STAT'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['UNCONFIRMED_INVEST_LOSS_BS_20151231'] = data[data['rptDate'] == '20151231']['UNCONFIRMED_INVEST_LOSS_BS'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['MINORITY_INT_20151231'] = data[data['rptDate'] == '20151231']['MINORITY_INT'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
COMMON_RATIO['EQY_BELONGTO_PARCOMSH_20151231'] = data[data['rptDate'] == '20151231']['EQY_BELONGTO_PARCOMSH'].values / data[data['rptDate'] == '20151231']['TOT_EQUITY'].values
data = pd.DataFrame(list(dest_db.bond_cashflow.find()))
COMMON_RATIO['CASH_RECP_CAP_CONTRIB_20151231'] = data[data['rptDate'] == '20151231']['CASH_RECP_CAP_CONTRIB'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['CASH_REC_SAIMS_20151231'] = data[data['rptDate'] == '20151231']['CASH_REC_SAIMS'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['CASH_RECP_BORROW_20151231'] = data[data['rptDate'] == '20151231']['CASH_RECP_BORROW'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['OTHER_CASH_RECP_RAL_FNC_ACT_20151231'] = data[data['rptDate'] == '20151231']['OTHER_CASH_RECP_RAL_FNC_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['PROC_ISSUE_BONDS_20151231'] = data[data['rptDate'] == '20151231']['PROC_ISSUE_BONDS'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['STOT_CASH_INFLOWS_FNC_ACT_20151231'] = data[data['rptDate'] == '20151231']['STOT_CASH_INFLOWS_FNC_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['CASH_PREPAY_AMT_BORR_20151231'] = data[data['rptDate'] == '20151231']['CASH_PREPAY_AMT_BORR'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['CASH_PAY_DIST_DPCP_INT_EXP_20151231'] = data[data['rptDate'] == '20151231']['CASH_PAY_DIST_DPCP_INT_EXP'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['DVD_PROFIT_PAID_SC_MS_20151231'] = data[data['rptDate'] == '20151231']['DVD_PROFIT_PAID_SC_MS'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['OTHER_CASH_PAY_RAL_FNC_ACT_20151231'] = data[data['rptDate'] == '20151231']['OTHER_CASH_PAY_RAL_FNC_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['STOT_CASH_OUTFLOWS_FNC_ACT_20151231'] = data[data['rptDate'] == '20151231']['STOT_CASH_OUTFLOWS_FNC_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_FNC_ACT'].values
COMMON_RATIO['CASH_RECP_SG_AND_RS_20151231'] = data[data['rptDate'] == '20151231']['CASH_RECP_SG_AND_RS'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['RECP_TAX_RENDS_20151231'] = data[data['rptDate'] == '20151231']['RECP_TAX_RENDS'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['OTHER_CASH_RECP_RAL_OPER_ACT_20151231'] = data[data['rptDate'] == '20151231']['OTHER_CASH_RECP_RAL_OPER_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_INSURED_DEP_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_INSURED_DEP'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_DEP_COB_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_DEP_COB'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_LOANS_CENTRAL_BANK_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_LOANS_CENTRAL_BANK'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_FUND_BORR_OFI_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_FUND_BORR_OFI'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_INT_HANDLING_CHRG_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_INT_HANDLING_CHRG'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['CASH_RECP_PREM_ORIG_INCO_20151231'] = data[data['rptDate'] == '20151231']['CASH_RECP_PREM_ORIG_INCO'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_CASH_RECEIVED_REINSU_BUS_20151231'] = data[data['rptDate'] == '20151231']['NET_CASH_RECEIVED_REINSU_BUS'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_DISP_TFA_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_DISP_TFA'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_DISP_FIN_ASSETS_AVAIL_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_DISP_FIN_ASSETS_AVAIL'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_LOANS_OTHER_BANK_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_LOANS_OTHER_BANK'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_REPURCH_BUS_FUND_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_REPURCH_BUS_FUND'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_CASH_FROM_SEURITIES_20151231'] = data[data['rptDate'] == '20151231']['NET_CASH_FROM_SEURITIES'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['STOT_CASH_INFLOWS_OPER_ACT_20151231'] = data[data['rptDate'] == '20151231']['STOT_CASH_INFLOWS_OPER_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_LENDING_FUND_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_LENDING_FUND'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_FINA_INSTRUMENTS_MEASURED_AT_FMV_20151231'] = data[data['rptDate'] == '20151231']['NET_FINA_INSTRUMENTS_MEASURED_AT_FMV'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['CASH_PAY_GOODS_PURCH_SERV_REC_20151231'] = data[data['rptDate'] == '20151231']['CASH_PAY_GOODS_PURCH_SERV_REC'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['CASH_PAY_BEH_EMPL_20151231'] = data[data['rptDate'] == '20151231']['CASH_PAY_BEH_EMPL'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['PAY_ALL_TYP_TAX_20151231'] = data[data['rptDate'] == '20151231']['PAY_ALL_TYP_TAX'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['OTHER_CASH_PAY_RAL_OPER_ACT_20151231'] = data[data['rptDate'] == '20151231']['OTHER_CASH_PAY_RAL_OPER_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_CLIENTS_LOAN_ADV_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_CLIENTS_LOAN_ADV'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['NET_INCR_DEP_CBOB_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_DEP_CBOB'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['CASH_PAY_CLAIMS_ORIG_INCO_20151231'] = data[data['rptDate'] == '20151231']['CASH_PAY_CLAIMS_ORIG_INCO'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['HANDLING_CHRG_PAID_20151231'] = data[data['rptDate'] == '20151231']['HANDLING_CHRG_PAID'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['COMM_INSUR_PLCY_PAID_20151231'] = data[data['rptDate'] == '20151231']['COMM_INSUR_PLCY_PAID'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['STOT_CASH_OUTFLOWS_OPER_ACT_20151231'] = data[data['rptDate'] == '20151231']['STOT_CASH_OUTFLOWS_OPER_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_OPER_ACT'].values
COMMON_RATIO['CASH_RECP_DISP_WITHDRWL_INVEST_20151231'] = data[data['rptDate'] == '20151231']['CASH_RECP_DISP_WITHDRWL_INVEST'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['CASH_RECP_RETURN_INVEST_20151231'] = data[data['rptDate'] == '20151231']['CASH_RECP_RETURN_INVEST'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['NET_CASH_RECP_DISP_FIOLTA_20151231'] = data[data['rptDate'] == '20151231']['NET_CASH_RECP_DISP_FIOLTA'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['NET_CASH_RECP_DISP_SOBU_20151231'] = data[data['rptDate'] == '20151231']['NET_CASH_RECP_DISP_SOBU'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['OTHER_CASH_RECP_RAL_INV_ACT_20151231'] = data[data['rptDate'] == '20151231']['OTHER_CASH_RECP_RAL_INV_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['STOT_CASH_INFLOWS_INV_ACT_20151231'] = data[data['rptDate'] == '20151231']['STOT_CASH_INFLOWS_INV_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['CASH_PAY_ACQ_CONST_FIOLTA_20151231'] = data[data['rptDate'] == '20151231']['CASH_PAY_ACQ_CONST_FIOLTA'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['CASH_PAID_INVEST_20151231'] = data[data['rptDate'] == '20151231']['CASH_PAID_INVEST'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['NET_INCR_PLEDGE_LOAN_20151231'] = data[data['rptDate'] == '20151231']['NET_INCR_PLEDGE_LOAN'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['NET_CASH_PAY_AQUIS_SOBU_20151231'] = data[data['rptDate'] == '20151231']['NET_CASH_PAY_AQUIS_SOBU'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['OTHER_CASH_PAY_RAL_INV_ACT_20151231'] = data[data['rptDate'] == '20151231']['OTHER_CASH_PAY_RAL_INV_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
COMMON_RATIO['STOT_CASH_OUTFLOWS_INV_ACT_20151231'] = data[data['rptDate'] == '20151231']['STOT_CASH_OUTFLOWS_INV_ACT'].values / data[data['rptDate'] == '20151231']['NET_CASH_FLOWS_INV_ACT'].values
data = pd.DataFrame(list(dest_db.bond_profit.find()))
COMMON_RATIO['OPER_REV_20151231'] = data[data['rptDate'] == '20151231']['OPER_REV'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['INT_INC_20151231'] = data[data['rptDate'] == '20151231']['INT_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['INSUR_PREM_UNEARNED_20151231'] = data[data['rptDate'] == '20151231']['INSUR_PREM_UNEARNED'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['HANDLING_CHRG_COMM_INC_20151231'] = data[data['rptDate'] == '20151231']['HANDLING_CHRG_COMM_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TOT_PREM_INC_20151231'] = data[data['rptDate'] == '20151231']['TOT_PREM_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['REINSUR_INC_20151231'] = data[data['rptDate'] == '20151231']['REINSUR_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['PREM_CEDED_20151231'] = data[data['rptDate'] == '20151231']['PREM_CEDED'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['UNEARNED_PREM_RSRV_WITHDRAW_20151231'] = data[data['rptDate'] == '20151231']['UNEARNED_PREM_RSRV_WITHDRAW'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_INC_AGENCY BUSINESS_20151231'] = data[data['rptDate'] == '20151231']['NET_INC_AGENCY BUSINESS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_INC_UNDERWRITING-BUSINESS_20151231'] = data[data['rptDate'] == '20151231']['NET_INC_UNDERWRITING-BUSINESS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_INC_CUSTOMERASSET-MANAGEMENT BUSINESS_20151231'] = data[data['rptDate'] == '20151231']['NET_INC_CUSTOMERASSET-MANAGEMENT BUSINESS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['OTHER_OPER_INC_20151231'] = data[data['rptDate'] == '20151231']['OTHER_OPER_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_INT_INC_20151231'] = data[data['rptDate'] == '20151231']['NET_INT_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_FEE_AND_COMMISSION_INC_20151231'] = data[data['rptDate'] == '20151231']['NET_FEE_AND_COMMISSION_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_OTHER_OPER_INC_20151231'] = data[data['rptDate'] == '20151231']['NET_OTHER_OPER_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TOT_OPER_COST_20151231'] = data[data['rptDate'] == '20151231']['TOT_OPER_COST'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['OPER_COST_20151231'] = data[data['rptDate'] == '20151231']['OPER_COST'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['INT_EXP_20151231'] = data[data['rptDate'] == '20151231']['INT_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['HANDLING_CHRG_COMM_EXP_20151231'] = data[data['rptDate'] == '20151231']['HANDLING_CHRG_COMM_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['OPER_EXP_20151231'] = data[data['rptDate'] == '20151231']['OPER_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TAXES_SURCHARGES_OPS_20151231'] = data[data['rptDate'] == '20151231']['TAXES_SURCHARGES_OPS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['SELLING_DIST_EXP_20151231'] = data[data['rptDate'] == '20151231']['SELLING_DIST_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['GERL_ADMIN_EXP_20151231'] = data[data['rptDate'] == '20151231']['GERL_ADMIN_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['FIN_EXP_IS_20151231'] = data[data['rptDate'] == '20151231']['FIN_EXP_IS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['IMPAIR_LOSS_ASSETS_20151231'] = data[data['rptDate'] == '20151231']['IMPAIR_LOSS_ASSETS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['PREPAY_SURR_20151231'] = data[data['rptDate'] == '20151231']['PREPAY_SURR'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_CLAIM_EXP_20151231'] = data[data['rptDate'] == '20151231']['NET_CLAIM_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_INSUR_CONT_RSRV_20151231'] = data[data['rptDate'] == '20151231']['NET_INSUR_CONT_RSRV'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['DVD_EXP_INSURED_20151231'] = data[data['rptDate'] == '20151231']['DVD_EXP_INSURED'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['REINSURANCE_EXP_20151231'] = data[data['rptDate'] == '20151231']['REINSURANCE_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['CLAIM_EXP_RECOVERABLE_20151231'] = data[data['rptDate'] == '20151231']['CLAIM_EXP_RECOVERABLE'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['INSUR_RSRV_RECOVERABLE_20151231'] = data[data['rptDate'] == '20151231']['INSUR_RSRV_RECOVERABLE'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['REINSUR_EXP_RECOVERABLE_20151231'] = data[data['rptDate'] == '20151231']['REINSUR_EXP_RECOVERABLE'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['OTHER_OPER_EXP_20151231'] = data[data['rptDate'] == '20151231']['OTHER_OPER_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_INC_OTHER_OPS_20151231'] = data[data['rptDate'] == '20151231']['NET_INC_OTHER_OPS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_GAIN_CHG_FV_20151231'] = data[data['rptDate'] == '20151231']['NET_GAIN_CHG_FV'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_INVEST_INC_20151231'] = data[data['rptDate'] == '20151231']['NET_INVEST_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['INC_INVEST_ASSOC_JV_ENTP_20151231'] = data[data['rptDate'] == '20151231']['INC_INVEST_ASSOC_JV_ENTP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_GAIN_FX_TRANS_20151231'] = data[data['rptDate'] == '20151231']['NET_GAIN_FX_TRANS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['OPPROFIT_20151231'] = data[data['rptDate'] == '20151231']['OPPROFIT'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NON_OPER_REV_20151231'] = data[data['rptDate'] == '20151231']['NON_OPER_REV'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NON_OPER_EXP_20151231'] = data[data['rptDate'] == '20151231']['NON_OPER_EXP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_LOSS_DISP_NONCUR_ASSET_20151231'] = data[data['rptDate'] == '20151231']['NET_LOSS_DISP_NONCUR_ASSET'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TOT_PROFIT_20151231'] = data[data['rptDate'] == '20151231']['TOT_PROFIT'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TAX_20151231'] = data[data['rptDate'] == '20151231']['TAX'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['UNCONFIRMED_INVEST_LOSS_IS_20151231'] = data[data['rptDate'] == '20151231']['UNCONFIRMED_INVEST_LOSS_IS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NET_PROFIT_IS_20151231'] = data[data['rptDate'] == '20151231']['NET_PROFIT_IS'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['MINORITY_INT_INC_20151231'] = data[data['rptDate'] == '20151231']['MINORITY_INT_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['NP_BELONGTO_PARCOMSH_20151231'] = data[data['rptDate'] == '20151231']['NP_BELONGTO_PARCOMSH'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['OTHER_COMPREH_INC_20151231'] = data[data['rptDate'] == '20151231']['OTHER_COMPREH_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TOT_COMPREH_INC_20151231'] = data[data['rptDate'] == '20151231']['TOT_COMPREH_INC'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TOT_COMPREH_INC_MIN_SHRHLDR_20151231'] = data[data['rptDate'] == '20151231']['TOT_COMPREH_INC_MIN_SHRHLDR'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values
COMMON_RATIO['TOT_COMPREH_INC_PARENT_COMP_20151231'] = data[data['rptDate'] == '20151231']['TOT_COMPREH_INC_PARENT_COMP'].values / data[data['rptDate'] == '20151231']['TOT_OPER_REV'].values

tSet=COMMON_RATIO.dropna(axis=0,how='any',thresh=50) # drop if na is above 50
tSet=tSet.dropna(axis=1,how='any', thresh=100) # drop column if na is above 10000

default = pd.DataFrame(list(dest_db.issuers_info.find({})))
default = default.drop(labels=['NATURE','_id','CITY','PROVINCE','COMP_NAME','INDUSTRY_GICS','LISTINGORNOT'],axis=1)

result = default.merge(COMMON_RATIO, how='inner', on='code')

df_column = result[result['df'] == 1.0]
kept_col = set(df_column.dropna(axis=1, how='all').columns)

model = result.drop(labels = list(set(result.columns) - kept_col), axis = 1)
insert_record = json.loads(model.to_json(orient='records'))
ret = dest_db.model2015.insert_many(insert_record)

#

