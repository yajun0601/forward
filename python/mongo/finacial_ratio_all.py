#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 26 17:49:45 2017

@author: zhengyajun
"""

import pandas as pd
from numpy  import *
from pymongo import *
import json

f = open("./interest.py", 'w+')  
#client = MongoClient("mongodb://192.168.10.60:27017/")
#db = client.stocks
#input_data = db.bond_profit
#data = pd.DataFrame(list(input_data.find()))
#
#
dest_client = MongoClient("mongodb://127.0.0.1:27017/")
dest_db = dest_client.bonds
dest_data = dest_db.issuers
#out = pd.DataFrame(list(dest_data.find()))

#rptDate=['20131231','20141231','20151231']

rptDate=['20151231']

head = '''
import pandas as pd     
from numpy  import * 
from pymongo import *
import json


dest_client = MongoClient('mongodb://127.0.0.1:27017/')
dest_db = dest_client.bonds
dest_data = dest_db.issuers
COMMON_RATIO = pd.DataFrame()

COMMON_RATIO['code'] = data[data['rptDate'] == '20151231']['code']
'''
print(head, file = f)   
###  BALANCE SHEET
print("data = pd.DataFrame(list(dest_db.bond_balance.find()))", file = f)
#TOT_ASSETS_RARIO
cols = 'monetary_cap,tradable_fin_assets,notes_rcv,acct_rcv,oth_rcv,prepay,dvd_rcv,int_rcv,inventories,consumptive_bio_assets,deferred_exp,hfs_assets,non_cur_assets_due_within_1y,settle_rsrv,loans_to_oth_banks,margin_acct,prem_rcv,rcv_from_reinsurer,rcv_from_ceded_insur_cont_rsrv,red_monetary_cap_for_sale,tot_acct_rcv,oth_cur_assets,tot_cur_assets,fin_assets_avail_for_sale,held_to_mty_invest,invest_real_estate,long_term_eqy_invest,long_term_rec,fix_assets,proj_matl,const_in_prog,fix_assets_disp,productive_bio_assets,oil_and_natural_gas_assets,intang_assets,r_and_d_costs,goodwill,long_term_deferred_exp,deferred_tax_assets,loans_and_adv_granted,oth_non_cur_assets,tot_non_cur_assets,cash_deposits_central_bank,agency_bus_assets,rcv_invest,asset_dep_oth_banks_fin_inst,precious_metals,rcv_ceded_unearned_prem_rsrv,rcv_ceded_claim_rsrv,rcv_ceded_life_insur_rsrv,rcv_ceded_lt_health_insur_rsrv,insured_pledge_loan,cap_mrgn_paid,independent_acct_assets,time_deposits,subr_rec,mrgn_paid,seat_fees_exchange,clients_cap_deposit,clients_rsrv_settle,oth_assets,derivative_fin_assets'
for ss in cols.upper().split(','):
    for date in rptDate:
        print("COMMON_RATIO[\'" + ss + '_' + date +"\'] = " + "data[data['rptDate'] == \'"+ date +"\'][\'"+ss+"\'].values / "+"data[data['rptDate'] == \'"+ date +"\']['TOT_ASSETS'].values",file = f)
#TOT_LIAB_RARIO():
cols = 'st_borrow,tradable_fin_liab,notes_payable,acct_payable,adv_from_cust,empl_ben_payable,taxes_surcharges_payable,tot_acct_payable,int_payable,dvd_payable,oth_payable,acc_exp,deferred_inc_cur_liab,hfs_liab,non_cur_liab_due_within_1y,st_bonds_payable,borrow_central_bank,deposit_received_ib_deposits,loans_oth_banks,fund_sales_fin_assets_rp,handling_charges_comm_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,oth_cur_liab,tot_cur_liab,lt_borrow,bonds_payable,lt_payable,lt_empl_ben_payable,specific_item_payable,provisions,deferred_tax_liab,deferred_inc_non_cur_liab,oth_non_cur_liab,tot_non_cur_liab,liab_dep_oth_banks_fin_inst,agency_bus_liab,cust_bank_dep,claims_payable,dvd_payable_insured,deposit_received,insured_deposit_invest,unearned_prem_rsrv,out_loss_rsrv,life_insur_rsrv,lt_health_insur_v,independent_acct_liab,prem_received_adv,pledge_loan,st_finl_inst_payable,oth_liab,derivative_fin_liab'
for ss in cols.upper().split(','):
    for date in rptDate:
        print("COMMON_RATIO[\'" + ss + '_' + date +"\'] = " + "data[data['rptDate'] == \'"+ date +"\'][\'"+ss+"\'].values / "+"data[data['rptDate'] == \'"+ date +"\']['TOT_LIAB'].values",file = f)

#TOT_EQUITY_RARIO():
cols = 'cap_stk,other_equity_instruments,other_equity_instruments_PRE,cap_rsrv,surplus_rsrv,undistributed_profit,tsy_stk,other_compreh_inc_bs,special_rsrv,prov_nom_risks,cnvd_diff_foreign_curr_stat,unconfirmed_invest_loss_bs,minority_int,eqy_belongto_parcomsh'
for ss in cols.upper().split(','):
    for date in rptDate:
        print("COMMON_RATIO[\'" + ss + '_' + date +"\'] = " + "data[data['rptDate'] == \'"+ date +"\'][\'"+ss+"\'].values / "+"data[data['rptDate'] == \'"+ date +"\']['TOT_EQUITY'].values",file = f)
        
## CASH FLOW
print("data = pd.DataFrame(list(dest_db.bond_cashflow.find()))", file = f)
#NET_CASH_FLOWS_FNC_ACT
cols = 'CASH_RECP_CAP_CONTRIB,CASH_REC_SAIMS,CASH_RECP_BORROW,OTHER_CASH_RECP_RAL_FNC_ACT,PROC_ISSUE_BONDS,STOT_CASH_INFLOWS_FNC_ACT,CASH_PREPAY_AMT_BORR,CASH_PAY_DIST_DPCP_INT_EXP,DVD_PROFIT_PAID_SC_MS,OTHER_CASH_PAY_RAL_FNC_ACT,STOT_CASH_OUTFLOWS_FNC_ACT'
for ss in cols.split(','):
    for date in rptDate:
        print("COMMON_RATIO[\'" + ss + '_' + date +"\'] = " + "data[data['rptDate'] == \'"+ date +"\'][\'"+ss+"\'].values / "+"data[data['rptDate'] == \'"+ date +"\']['NET_CASH_FLOWS_FNC_ACT'].values",file = f)

#NET_CASH_FLOWS_OPER_ACT_R():
cols = 'CASH_RECP_SG_AND_RS,RECP_TAX_RENDS,OTHER_CASH_RECP_RAL_OPER_ACT,NET_INCR_INSURED_DEP,NET_INCR_DEP_COB,NET_INCR_LOANS_CENTRAL_BANK,NET_INCR_FUND_BORR_OFI,NET_INCR_INT_HANDLING_CHRG,CASH_RECP_PREM_ORIG_INCO,NET_CASH_RECEIVED_REINSU_BUS,NET_INCR_DISP_TFA,NET_INCR_DISP_FIN_ASSETS_AVAIL,NET_INCR_LOANS_OTHER_BANK,NET_INCR_REPURCH_BUS_FUND,NET_CASH_FROM_SEURITIES,STOT_CASH_INFLOWS_OPER_ACT,NET_INCR_LENDING_FUND,NET_FINA_INSTRUMENTS_MEASURED_AT_FMV,CASH_PAY_GOODS_PURCH_SERV_REC,CASH_PAY_BEH_EMPL,PAY_ALL_TYP_TAX,OTHER_CASH_PAY_RAL_OPER_ACT,NET_INCR_CLIENTS_LOAN_ADV,NET_INCR_DEP_CBOB,CASH_PAY_CLAIMS_ORIG_INCO,HANDLING_CHRG_PAID,COMM_INSUR_PLCY_PAID,STOT_CASH_OUTFLOWS_OPER_ACT'
for ss in cols.split(','):
    for date in rptDate:
        print("COMMON_RATIO[\'" + ss + '_' + date +"\'] = " + "data[data['rptDate'] == \'"+ date +"\'][\'"+ss+"\'].values / "+"data[data['rptDate'] == \'"+ date +"\']['NET_CASH_FLOWS_OPER_ACT'].values",file = f)
        
        
#NET_CASH_FLOWS_INV_ACT_R():
cols = 'CASH_RECP_DISP_WITHDRWL_INVEST,CASH_RECP_RETURN_INVEST,NET_CASH_RECP_DISP_FIOLTA,NET_CASH_RECP_DISP_SOBU,OTHER_CASH_RECP_RAL_INV_ACT,STOT_CASH_INFLOWS_INV_ACT,CASH_PAY_ACQ_CONST_FIOLTA,CASH_PAID_INVEST,NET_INCR_PLEDGE_LOAN,NET_CASH_PAY_AQUIS_SOBU,OTHER_CASH_PAY_RAL_INV_ACT,STOT_CASH_OUTFLOWS_INV_ACT'
for ss in cols.split(','):
    for date in rptDate:
        print("COMMON_RATIO[\'" + ss + '_' + date +"\'] = " + "data[data['rptDate'] == \'"+ date +"\'][\'"+ss+"\'].values / "+"data[data['rptDate'] == \'"+ date +"\']['NET_CASH_FLOWS_INV_ACT'].values",file = f)


#PROFIT
print("data = pd.DataFrame(list(dest_db.bond_profit.find()))", file = f)
cols = 'OPER_REV,INT_INC,INSUR_PREM_UNEARNED,HANDLING_CHRG_COMM_INC,TOT_PREM_INC,REINSUR_INC,PREM_CEDED,UNEARNED_PREM_RSRV_WITHDRAW,NET_INC_AGENCY BUSINESS,NET_INC_UNDERWRITING-BUSINESS,NET_INC_CUSTOMERASSET-MANAGEMENT BUSINESS,OTHER_OPER_INC,NET_INT_INC,NET_FEE_AND_COMMISSION_INC,NET_OTHER_OPER_INC,TOT_OPER_COST,OPER_COST,INT_EXP,HANDLING_CHRG_COMM_EXP,OPER_EXP,TAXES_SURCHARGES_OPS,SELLING_DIST_EXP,GERL_ADMIN_EXP,FIN_EXP_IS,IMPAIR_LOSS_ASSETS,PREPAY_SURR,NET_CLAIM_EXP,NET_INSUR_CONT_RSRV,DVD_EXP_INSURED,REINSURANCE_EXP,CLAIM_EXP_RECOVERABLE,INSUR_RSRV_RECOVERABLE,REINSUR_EXP_RECOVERABLE,OTHER_OPER_EXP,NET_INC_OTHER_OPS,NET_GAIN_CHG_FV,NET_INVEST_INC,INC_INVEST_ASSOC_JV_ENTP,NET_GAIN_FX_TRANS,OPPROFIT,NON_OPER_REV,NON_OPER_EXP,NET_LOSS_DISP_NONCUR_ASSET,TOT_PROFIT,TAX,UNCONFIRMED_INVEST_LOSS_IS,NET_PROFIT_IS,MINORITY_INT_INC,NP_BELONGTO_PARCOMSH,OTHER_COMPREH_INC,TOT_COMPREH_INC,TOT_COMPREH_INC_MIN_SHRHLDR,TOT_COMPREH_INC_PARENT_COMP'

for ss in cols.split(','):
    for date in rptDate:
        print("COMMON_RATIO[\'" + ss + '_' + date +"\'] = " + "data[data['rptDate'] == \'"+ date +"\'][\'"+ss+"\'].values / "+"data[data['rptDate'] == \'"+ date +"\']['TOT_OPER_REV'].values",file = f)


insert_str = '''
default = pd.DataFrame(list(dest_db.issuers_info.find({})))
default = default.drop(labels=['NATURE','_id','CITY','PROVINCE','COMP_NAME','INDUSTRY_GICS','LISTINGORNOT'],axis=1)
result = default.merge(COMMON_RATIO, how='inner', on='code')
insert_record = json.loads(result.to_json(orient='records'))
ret = dest_db.model2015.insert_many(insert_record)
'''
print(insert_str, file=f)

f.close()
