# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 11:25:33 2017

@author: Administrator
"""

from WindPy import *
import pandas as pd
from pymongo import *
import json
from bson.son import SON
from numpy  import *

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


w.start()

#ratings = w.wss("112004.SZ,112019.SZ,112022.SZ,112024.SZ,112025.SZ", "latestissurercreditrating2,rate_fwdissuer,rate_chngissuer,rate_former","tradeDate=20170709;ratingAgency=101;type=1")

#w.wss("112024.SZ,112025.SZ", "latestissurercreditrating2,rate_fwdissuer,rate_chngissuer,rate_former","tradeDate=20170709;ratingAgency=101;type=1")

def datelist():
    season_date= ['0331','0630','0930','1231']
    years = ['2014','2015','2016','2017']
    rptDate_list = list()
    for year in years:
        for season in season_date:
            if year == '2017' and season in ['0930','1231']:
                continue
            rptdate = year+season
            rptDate_list.append(rptdate)
    
#print(rptDate_list)    

#    for rptdate in rptDate_list:
#        for stocks in stock_list[:2]:
#            print("unit=1;rptDate=%s;rptType=1"%rptdate)
    return rptDate_list


w.wss("112025.SZ", "latestissurercreditrating2","tradeDate=20160630;ratingAgency=101;type=1")
dates = datelist()
ratelist = list()
#for rptdate in dates:
##    print("latestissurercreditrating2","tradeDate=\'%s\';ratingAgency=101;type=1"%(rptdate))
#    rate = w.wss("112025.SZ", "latestissurercreditrating2","tradeDate=%s;ratingAgency=101;type=1"%(rptdate))
#    print(rptdate,rate.Data)


a = array(stock_list[:5040])
last_two = ','.join(stock_list[5040:])
b = a.reshape(20,252)
m,n = b.shape

db = client.bonds
collection = db.ratings
for rptdate in dates:
    datart = w.wss(last_two, "latestissurercreditrating2","tradeDate=%s;ratingAgency=101;type=1"%(rptdate))
    if datart.ErrorCode != 0:
        print(datart)
        client.close()
        break;
    mm = mat(datart.Data)
    df = pd.DataFrame(mm.T, index = datart.Codes, columns = datart.Fields)
    df['code'] = datart.Codes
    df['rptDate'] = [rptdate for code in datart.Codes]
    
    insert_record = json.loads(df.to_json(orient='records'))
    ret = collection.insert_many(insert_record)
    
    for x in range(m):
        stocks = ",".join(b[x])
        print(stocks)
        datart = w.wss(stocks, "latestissurercreditrating2","tradeDate=%s;ratingAgency=101;type=1"%(rptdate))
    
#        datart = w.wss(stocks, "comp_name,city,listingornot,qfa_tot_oper_rev,qfa_oper_rev,qfa_interest_inc,qfa_insur_prem_unearned,qfa_handling_chrg_comm_inc,qfa_tot_prem_inc,qfa_reinsur_inc,qfa_prem_ceded,qfa_unearned_prem_rsrv,qfa_net_inc_agency business,qfa_net_inc_underwriting-business,qfa_net_inc_customerasset-management business,qfa_other_oper_inc,qfa_net_int_inc,qfa_net_fee_and_commission_inc,qfa_net_other_oper_inc,qfa_tot_oper_cost,qfa_oper_cost,qfa_grossmargin,qfa_interest_exp,qfa_handling_chrg_comm_exp,qfa_oper_exp,qfa_taxes_surcharges_ops,qfa_selling_dist_exp,qfa_gerl_admin_exp,qfa_fin_exp_is,qfa_impair_loss_assets,qfa_prepay_surr,qfa_net_claim_exp,qfa_net_insur_cont_rsrv,qfa_dvd_exp_insured,qfa_reinsurance_exp,qfa_claim_exp_recoverable,qfa_Insur_rsrv_recoverable,qfa_reinsur_exp_recoverable,qfa_other_oper_exp,qfa_net_gain_chg_fv,qfa_net_invest_inc,qfa_inc_invest_assoc_jv_entp,qfa_net_gain_fx_trans,qfa_opprofit,qfa_non_oper_rev,qfa_non_oper_exp,qfa_net_loss_disp_noncur_asset,qfa_tot_profit,qfa_tax,qfa_unconfirmed_invest_loss_is,qfa_net_profit_is,qfa_minority_int_inc,qfa_np_belongto_parcomsh,qfa_other_compreh_inc,qfa_tot_compreh_inc,qfa_tot_compreh_inc_min_shrhldr,qfa_tot_compreh_inc_parent_comp","unit=1;rptDate=%s;rptType=1"%rptdate)
        if datart.ErrorCode != 0:
            print(datart)
            client.close()
            break;
            
        mm = mat(datart.Data)
        df = pd.DataFrame(mm.T, index = datart.Codes, columns = datart.Fields)
        df['code'] = datart.Codes
        df['rptDate'] = [rptdate for code in datart.Codes]
        
        insert_record = json.loads(df.to_json(orient='records'))
        ret = collection.insert_many(insert_record)
        print(ret,stocks,rptdate)
