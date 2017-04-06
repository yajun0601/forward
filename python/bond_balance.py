#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  6 11:52:24 2017

@author: yajun
"""

from WindPy import *
import pandas as pd
from numpy  import *
from pymongo import *
import json
from bson.son import SON
w.start()
client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.stocks

pipeline = [{"$group" : {"_id" : "$comp_name", "code" : {"$last":"$_id"}}},
            {"$sort": SON([("_id", -1)])}
            ]
code = db.bond.aggregate(pipeline)
record = (list(code)) #Convert the input to an array.

stock_list = list()
for rr in record:
    #print(rr['code'])
    stock_list.append(rr['code'])
  
def balance_total():
#    collection = db.BalanceSheet
#    stocks = "000001.SZ,000002.SZ"
#5042 = 5040+2 = 252*20 + 2  
    a=array(stock_list[:50])
    b = a.reshape(2,25)
    m,n = b.shape
    for rptdate in rptDate_list:
        for x in range(m):
            stocks = ",".join(b[x])
            print(stocks)
            datart = w.wss(stocks, "comp_name,province,city,listingornot,industry_CSRC12,monetary_cap,tradable_fin_assets,notes_rcv,acct_rcv,oth_rcv,prepay,dvd_rcv,int_rcv,inventories,consumptive_bio_assets,deferred_exp,hfs_assets,non_cur_assets_due_within_1y,settle_rsrv,loans_to_oth_banks,margin_acct,prem_rcv,rcv_from_reinsurer,rcv_from_ceded_insur_cont_rsrv,red_monetary_cap_for_sale,tot_acct_rcv,oth_cur_assets,tot_cur_assets,fin_assets_avail_for_sale,held_to_mty_invest,invest_real_estate,long_term_eqy_invest,long_term_rec,fix_assets,proj_matl,const_in_prog,fix_assets_disp,productive_bio_assets,oil_and_natural_gas_assets,intang_assets,r_and_d_costs,goodwill,long_term_deferred_exp,deferred_tax_assets,loans_and_adv_granted,oth_non_cur_assets,tot_non_cur_assets,cash_deposits_central_bank,agency_bus_assets,rcv_invest,asset_dep_oth_banks_fin_inst,precious_metals,rcv_ceded_unearned_prem_rsrv,rcv_ceded_claim_rsrv,rcv_ceded_life_insur_rsrv,rcv_ceded_lt_health_insur_rsrv,insured_pledge_loan,cap_mrgn_paid,independent_acct_assets,time_deposits,subr_rec,mrgn_paid,seat_fees_exchange,clients_cap_deposit,clients_rsrv_settle,oth_assets,derivative_fin_assets,tot_assets,st_borrow,tradable_fin_liab,notes_payable,acct_payable,adv_from_cust,empl_ben_payable,taxes_surcharges_payable,tot_acct_payable,int_payable,dvd_payable,oth_payable,acc_exp,deferred_inc_cur_liab,hfs_liab,non_cur_liab_due_within_1y,st_bonds_payable,borrow_central_bank,deposit_received_ib_deposits,loans_oth_banks,fund_sales_fin_assets_rp,handling_charges_comm_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,oth_cur_liab,tot_cur_liab,lt_borrow,bonds_payable,lt_payable,lt_empl_ben_payable,specific_item_payable,provisions,deferred_tax_liab,deferred_inc_non_cur_liab,oth_non_cur_liab,tot_non_cur_liab,liab_dep_oth_banks_fin_inst,agency_bus_liab,cust_bank_dep,claims_payable,dvd_payable_insured,deposit_received,insured_deposit_invest,unearned_prem_rsrv,out_loss_rsrv,life_insur_rsrv,lt_health_insur_v,independent_acct_liab,prem_received_adv,pledge_loan,st_finl_inst_payable,oth_liab,derivative_fin_liab,tot_liab,cap_stk,other_equity_instruments,other_equity_instruments_PRE,cap_rsrv,surplus_rsrv,undistributed_profit,tsy_stk,other_compreh_inc_bs,special_rsrv,prov_nom_risks,cnvd_diff_foreign_curr_stat,unconfirmed_invest_loss_bs,minority_int,eqy_belongto_parcomsh,tot_equity,tot_liab_shrhldr_eqy","industryType=2;unit=1;rptDate=%d;rptType=1"%rptdate)
            #datart = w.wss( stocks , "comp_name,city,listingornot,industry_CSRC12,monetary_cap,tradable_fin_assets,notes_rcv,acct_rcv,oth_rcv,prepay,dvd_rcv,int_rcv,inventories,consumptive_bio_assets,deferred_exp,hfs_assets,non_cur_assets_due_within_1y,settle_rsrv,loans_to_oth_banks,margin_acct,prem_rcv,rcv_from_reinsurer,rcv_from_ceded_insur_cont_rsrv,red_monetary_cap_for_sale,tot_acct_rcv,oth_cur_assets,tot_cur_assets,fin_assets_avail_for_sale,held_to_mty_invest,invest_real_estate,long_term_eqy_invest,long_term_rec,fix_assets,proj_matl,const_in_prog,fix_assets_disp,productive_bio_assets,oil_and_natural_gas_assets,intang_assets,r_and_d_costs,goodwill,long_term_deferred_exp,deferred_tax_assets,loans_and_adv_granted,oth_non_cur_assets,tot_non_cur_assets,cash_deposits_central_bank,agency_bus_assets,rcv_invest,asset_dep_oth_banks_fin_inst,precious_metals,rcv_ceded_unearned_prem_rsrv,rcv_ceded_claim_rsrv,rcv_ceded_life_insur_rsrv,rcv_ceded_lt_health_insur_rsrv,insured_pledge_loan,cap_mrgn_paid,independent_acct_assets,time_deposits,subr_rec,mrgn_paid,seat_fees_exchange,clients_cap_deposit,clients_rsrv_settle,oth_assets,derivative_fin_assets,tot_assets,st_borrow,tradable_fin_liab,notes_payable,acct_payable,adv_from_cust,empl_ben_payable,taxes_surcharges_payable,tot_acct_payable,int_payable,dvd_payable,oth_payable,acc_exp,deferred_inc_cur_liab,hfs_liab,non_cur_liab_due_within_1y,st_bonds_payable,borrow_central_bank,deposit_received_ib_deposits,loans_oth_banks,fund_sales_fin_assets_rp,handling_charges_comm_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,oth_cur_liab,tot_cur_liab,lt_borrow,bonds_payable,lt_payable,lt_empl_ben_payable,specific_item_payable,provisions,deferred_tax_liab,deferred_inc_non_cur_liab,oth_non_cur_liab,tot_non_cur_liab,liab_dep_oth_banks_fin_inst,agency_bus_liab,cust_bank_dep,claims_payable,dvd_payable_insured,deposit_received,insured_deposit_invest,unearned_prem_rsrv,out_loss_rsrv,life_insur_rsrv,lt_health_insur_v,independent_acct_liab,prem_received_adv,pledge_loan,st_finl_inst_payable,oth_liab,derivative_fin_liab,tot_liab,cap_stk,other_equity_instruments,other_equity_instruments_PRE,cap_rsrv,surplus_rsrv,undistributed_profit,tsy_stk,other_compreh_inc_bs,special_rsrv,prov_nom_risks,cnvd_diff_foreign_curr_stat,unconfirmed_invest_loss_bs,minority_int,eqy_belongto_parcomsh,tot_equity,tot_liab_shrhldr_eqy","industryType=1;unit=1;rptDate=%d;rptType=1"%rptdate)
            if datart.ErrorCode != 0:
                print(datart)
                client.close()
                break;
            mm = mat(datart.Data)
            df = pd.DataFrame(mm.T, index = datart.Codes, columns = datart.Fields)
            df['code'] = datart.Codes
            df['rptDate'] = [rptdate for code in datart.Codes]
            
            insert_record = json.loads(df.to_json(orient='records'))
            ret = db.bondBalance.insert_many(insert_record)
            print(ret, rptdate)
        