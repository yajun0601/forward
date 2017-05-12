#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 11:43:12 2017

@author: yajun
"""

import pandas as pd
from numpy  import *
from pymongo import *
import json

client = MongoClient()
client = MongoClient("mongodb://192.168.10.60:27017/")
db = client.stocks

input_data = db.bond_profit
data = pd.DataFrame(list(input_data.find()))


#dmonetary_cap,tradable_fin_assets,notes_rcv,acct_rcv,oth_rcv,prepay,dvd_rcv,int_rcv,inventories,consumptive_bio_assets,deferred_exp,hfs_assets,non_cur_assets_due_within_1y,settle_rsrv,loans_to_oth_banks,margin_acct,prem_rcv,rcv_from_reinsurer,rcv_from_ceded_insur_cont_rsrv,red_monetary_cap_for_sale,tot_acct_rcv,oth_cur_assets,tot_cur_assets,fin_assets_avail_for_sale,held_to_mty_invest,invest_real_estate,long_term_eqy_invest,long_term_rec,fix_assets,proj_matl,const_in_prog,fix_assets_disp,productive_bio_assets,oil_and_natural_gas_assets,intang_assets,r_and_d_costs,goodwill,long_term_deferred_exp,deferred_tax_assets,loans_and_adv_granted,oth_non_cur_assets,tot_non_cur_assets,cash_deposits_central_bank,agency_bus_assets,rcv_invest,asset_dep_oth_banks_fin_inst,precious_metals,rcv_ceded_unearned_prem_rsrv,rcv_ceded_claim_rsrv,rcv_ceded_life_insur_rsrv,rcv_ceded_lt_health_insur_rsrv,insured_pledge_loan,cap_mrgn_paid,independent_acct_assets,time_deposits,subr_rec,mrgn_paid,seat_fees_exchange,clients_cap_deposit,clients_rsrv_settle,oth_assets,derivative_fin_assets,tot_assets,
#st_borrow,tradable_fin_liab,notes_payable,acct_payable,adv_from_cust,empl_ben_payable,taxes_surcharges_payable,tot_acct_payable,int_payable,dvd_payable,oth_payable,acc_exp,deferred_inc_cur_liab,hfs_liab,non_cur_liab_due_within_1y,st_bonds_payable,borrow_central_bank,deposit_received_ib_deposits,loans_oth_banks,fund_sales_fin_assets_rp,handling_charges_comm_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,oth_cur_liab,tot_cur_liab,lt_borrow,bonds_payable,lt_payable,lt_empl_ben_payable,specific_item_payable,provisions,deferred_tax_liab,deferred_inc_non_cur_liab,oth_non_cur_liab,tot_non_cur_liab,liab_dep_oth_banks_fin_inst,agency_bus_liab,cust_bank_dep,claims_payable,dvd_payable_insured,deposit_received,insured_deposit_invest,unearned_prem_rsrv,out_loss_rsrv,life_insur_rsrv,lt_health_insur_v,independent_acct_liab,prem_received_adv,pledge_loan,st_finl_inst_payable,oth_liab,derivative_fin_liab,tot_liab,
#cols = 'cap_stk,other_equity_instruments,other_equity_instruments_PRE,cap_rsrv,surplus_rsrv,undistributed_profit,tsy_stk,other_compreh_inc_bs,special_rsrv,prov_nom_risks,cnvd_diff_foreign_curr_stat,unconfirmed_invest_loss_bs,minority_int,eqy_belongto_parcomsh,tot_equity'
#tot_liab_shrhldr_eqy tot_equity
#cols = 'cash_recp_sg_and_rs,recp_tax_rends,other_cash_recp_ral_oper_act,net_incr_insured_dep,net_incr_dep_cob,net_incr_loans_central_bank,net_incr_fund_borr_ofi,net_incr_int_handling_chrg,cash_recp_prem_orig_inco,net_cash_received_reinsu_bus,net_incr_disp_tfa,net_incr_disp_fin_assets_avail,net_incr_loans_other_bank,net_incr_repurch_bus_fund,net_cash_from_seurities,stot_cash_inflows_oper_act,net_incr_lending_fund,net_fina_instruments_measured_at_fmv,cash_pay_goods_purch_serv_rec,cash_pay_beh_empl,pay_all_typ_tax,other_cash_pay_ral_oper_act,net_incr_clients_loan_adv,net_incr_dep_cbob,cash_pay_claims_orig_inco,handling_chrg_paid,comm_insur_plcy_paid,stot_cash_outflows_oper_act,net_cash_flows_oper_act,cash_recp_disp_withdrwl_invest,cash_recp_return_invest,net_cash_recp_disp_fiolta,net_cash_recp_disp_sobu,other_cash_recp_ral_inv_act,stot_cash_inflows_inv_act,cash_pay_acq_const_fiolta,cash_paid_invest,net_incr_pledge_loan,net_cash_pay_aquis_sobu,other_cash_pay_ral_inv_act,stot_cash_outflows_inv_act,net_cash_flows_inv_act,cash_recp_cap_contrib,cash_rec_saims,cash_recp_borrow,other_cash_recp_ral_fnc_act,proc_issue_bonds,stot_cash_inflows_fnc_act,cash_prepay_amt_borr,cash_pay_dist_dpcp_int_exp,dvd_profit_paid_sc_ms,other_cash_pay_ral_fnc_act,stot_cash_outflows_fnc_act,net_cash_flows_fnc_act,eff_fx_flu_cash,net_incr_cash_cash_equ_dm,cash_cash_equ_beg_period,cash_cash_equ_end_period,net_profit_cs,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,amort_lt_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,loss_scr_fa,loss_fv_chg,fin_exp_cs,invest_loss,decr_deferred_inc_tax_assets,incr_deferred_inc_tax_liab,decr_inventories,decr_oper_payable,incr_oper_payable,unconfirmed_invest_loss_cs,others,im_net_cash_flows_oper_act,conv_debt_into_cap,conv_corp_bonds_due_within_1y,fa_fnc_leases,end_bal_cash,beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,net_incr_cash_cash_equ_im'
#COLS='CASH_RECP_SG_AND_RS,RECP_TAX_RENDS,OTHER_CASH_RECP_RAL_OPER_ACT,NET_INCR_INSURED_DEP,NET_INCR_DEP_COB,NET_INCR_LOANS_CENTRAL_BANK,NET_INCR_FUND_BORR_OFI,NET_INCR_INT_HANDLING_CHRG,CASH_RECP_PREM_ORIG_INCO,NET_CASH_RECEIVED_REINSU_BUS,NET_INCR_DISP_TFA,NET_INCR_DISP_FIN_ASSETS_AVAIL,NET_INCR_LOANS_OTHER_BANK,NET_INCR_REPURCH_BUS_FUND,NET_CASH_FROM_SEURITIES,STOT_CASH_INFLOWS_OPER_ACT,NET_INCR_LENDING_FUND,NET_FINA_INSTRUMENTS_MEASURED_AT_FMV,CASH_PAY_GOODS_PURCH_SERV_REC,CASH_PAY_BEH_EMPL,PAY_ALL_TYP_TAX,OTHER_CASH_PAY_RAL_OPER_ACT,NET_INCR_CLIENTS_LOAN_ADV,NET_INCR_DEP_CBOB,CASH_PAY_CLAIMS_ORIG_INCO,HANDLING_CHRG_PAID,COMM_INSUR_PLCY_PAID,STOT_CASH_OUTFLOWS_OPER_ACT,NET_CASH_FLOWS_OPER_ACT,CASH_RECP_DISP_WITHDRWL_INVEST,CASH_RECP_RETURN_INVEST,NET_CASH_RECP_DISP_FIOLTA,NET_CASH_RECP_DISP_SOBU,OTHER_CASH_RECP_RAL_INV_ACT,STOT_CASH_INFLOWS_INV_ACT,CASH_PAY_ACQ_CONST_FIOLTA,CASH_PAID_INVEST,NET_INCR_PLEDGE_LOAN,NET_CASH_PAY_AQUIS_SOBU,OTHER_CASH_PAY_RAL_INV_ACT,STOT_CASH_OUTFLOWS_INV_ACT,NET_CASH_FLOWS_INV_ACT,CASH_RECP_CAP_CONTRIB,CASH_REC_SAIMS,CASH_RECP_BORROW,OTHER_CASH_RECP_RAL_FNC_ACT,PROC_ISSUE_BONDS,STOT_CASH_INFLOWS_FNC_ACT,CASH_PREPAY_AMT_BORR,CASH_PAY_DIST_DPCP_INT_EXP,DVD_PROFIT_PAID_SC_MS,OTHER_CASH_PAY_RAL_FNC_ACT,STOT_CASH_OUTFLOWS_FNC_ACT,NET_CASH_FLOWS_FNC_ACT,EFF_FX_FLU_CASH,NET_INCR_CASH_CASH_EQU_DM,CASH_CASH_EQU_BEG_PERIOD,CASH_CASH_EQU_END_PERIOD,NET_PROFIT_CS,PROV_DEPR_ASSETS,DEPR_FA_COGA_DPBA,AMORT_INTANG_ASSETS,AMORT_LT_DEFERRED_EXP,DECR_DEFERRED_EXP,INCR_ACC_EXP,LOSS_DISP_FIOLTA,LOSS_SCR_FA,LOSS_FV_CHG,FIN_EXP_CS,INVEST_LOSS,DECR_DEFERRED_INC_TAX_ASSETS,INCR_DEFERRED_INC_TAX_LIAB,DECR_INVENTORIES,DECR_OPER_PAYABLE,INCR_OPER_PAYABLE,UNCONFIRMED_INVEST_LOSS_CS,OTHERS,IM_NET_CASH_FLOWS_OPER_ACT,CONV_DEBT_INTO_CAP,CONV_CORP_BONDS_DUE_WITHIN_1Y,FA_FNC_LEASES,END_BAL_CASH,BEG_BAL_CASH,END_BAL_CASH_EQU,BEG_BAL_CASH_EQU,NET_INCR_CASH_CASH_EQU_IM'

years = ['20131231','20141231','20151231']
#cols = 'tot_oper_rev,oper_rev,int_inc,insur_prem_unearned,handling_chrg_comm_inc,tot_prem_inc,reinsur_inc,prem_ceded,unearned_prem_rsrv_withdraw,net_inc_agency business,net_inc_underwriting-business,net_inc_customerasset-management business,other_oper_inc,net_int_inc,net_fee_and_commission_inc,net_other_oper_inc,tot_oper_cost,oper_cost,int_exp,handling_chrg_comm_exp,oper_exp,taxes_surcharges_ops,selling_dist_exp,gerl_admin_exp,fin_exp_is,impair_loss_assets,prepay_surr,net_claim_exp,net_insur_cont_rsrv,dvd_exp_insured,reinsurance_exp,claim_exp_recoverable,Insur_rsrv_recoverable,reinsur_exp_recoverable,other_oper_exp,net_inc_other_ops,net_gain_chg_fv,net_invest_inc,inc_invest_assoc_jv_entp,net_gain_fx_trans,opprofit,non_oper_rev,non_oper_exp,net_loss_disp_noncur_asset,tot_profit,tax,unconfirmed_invest_loss_is,net_profit_is,minority_int_inc,np_belongto_parcomsh,other_compreh_inc,tot_compreh_inc,tot_compreh_inc_min_shrhldr,tot_compreh_inc_parent_comp'
cols = 'TOT_OPER_REV,OPER_REV,INT_INC,INSUR_PREM_UNEARNED,HANDLING_CHRG_COMM_INC,TOT_PREM_INC,REINSUR_INC,PREM_CEDED,UNEARNED_PREM_RSRV_WITHDRAW,NET_INC_AGENCY BUSINESS,NET_INC_UNDERWRITING-BUSINESS,NET_INC_CUSTOMERASSET-MANAGEMENT BUSINESS,OTHER_OPER_INC,NET_INT_INC,NET_FEE_AND_COMMISSION_INC,NET_OTHER_OPER_INC,TOT_OPER_COST,OPER_COST,INT_EXP,HANDLING_CHRG_COMM_EXP,OPER_EXP,TAXES_SURCHARGES_OPS,SELLING_DIST_EXP,GERL_ADMIN_EXP,FIN_EXP_IS,IMPAIR_LOSS_ASSETS,PREPAY_SURR,NET_CLAIM_EXP,NET_INSUR_CONT_RSRV,DVD_EXP_INSURED,REINSURANCE_EXP,CLAIM_EXP_RECOVERABLE,INSUR_RSRV_RECOVERABLE,REINSUR_EXP_RECOVERABLE,OTHER_OPER_EXP,NET_INC_OTHER_OPS,NET_GAIN_CHG_FV,NET_INVEST_INC,INC_INVEST_ASSOC_JV_ENTP,NET_GAIN_FX_TRANS,OPPROFIT,NON_OPER_REV,NON_OPER_EXP,NET_LOSS_DISP_NONCUR_ASSET,TOT_PROFIT,TAX,UNCONFIRMED_INVEST_LOSS_IS,NET_PROFIT_IS,MINORITY_INT_INC,NP_BELONGTO_PARCOMSH,OTHER_COMPREH_INC,TOT_COMPREH_INC,TOT_COMPREH_INC_MIN_SHRHLDR,TOT_COMPREH_INC_PARENT_COMP'

for ss in cols.split(','):
    for year in years:
        print("TOT_OPER_REV_R[\'" + ss + year +"\'] = " + "data[\'"+ss+"\'] / "+"data['TOT_OPER_REV']")
    
    
TOT_OPER_REV_R = pd.DataFrame()
TOT_OPER_REV_R['code'] = data['code'] 
TOT_OPER_REV_R['rptDate'] = data['rptDate']
TOT_OPER_REV_R['TOT_OPER_REV_R'] = data['TOT_OPER_REV'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['OPER_REV_R'] = data['OPER_REV'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['INT_INC_R'] = data['INT_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['INSUR_PREM_UNEARNED_R'] = data['INSUR_PREM_UNEARNED'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['HANDLING_CHRG_COMM_INC_R'] = data['HANDLING_CHRG_COMM_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TOT_PREM_INC_R'] = data['TOT_PREM_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['REINSUR_INC_R'] = data['REINSUR_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['PREM_CEDED_R'] = data['PREM_CEDED'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['UNEARNED_PREM_RSRV_WITHDRAW_R'] = data['UNEARNED_PREM_RSRV_WITHDRAW'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_INC_AGENCY BUSINESS_R'] = data['NET_INC_AGENCY BUSINESS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_INC_UNDERWRITING-BUSINESS_R'] = data['NET_INC_UNDERWRITING-BUSINESS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_INC_CUSTOMERASSET-MANAGEMENT BUSINESS_R'] = data['NET_INC_CUSTOMERASSET-MANAGEMENT BUSINESS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['OTHER_OPER_INC_R'] = data['OTHER_OPER_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_INT_INC_R'] = data['NET_INT_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_FEE_AND_COMMISSION_INC_R'] = data['NET_FEE_AND_COMMISSION_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_OTHER_OPER_INC_R'] = data['NET_OTHER_OPER_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TOT_OPER_COST_R'] = data['TOT_OPER_COST'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['OPER_COST_R'] = data['OPER_COST'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['INT_EXP_R'] = data['INT_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['HANDLING_CHRG_COMM_EXP_R'] = data['HANDLING_CHRG_COMM_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['OPER_EXP_R'] = data['OPER_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TAXES_SURCHARGES_OPS_R'] = data['TAXES_SURCHARGES_OPS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['SELLING_DIST_EXP_R'] = data['SELLING_DIST_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['GERL_ADMIN_EXP_R'] = data['GERL_ADMIN_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['FIN_EXP_IS_R'] = data['FIN_EXP_IS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['IMPAIR_LOSS_ASSETS_R'] = data['IMPAIR_LOSS_ASSETS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['PREPAY_SURR_R'] = data['PREPAY_SURR'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_CLAIM_EXP_R'] = data['NET_CLAIM_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_INSUR_CONT_RSRV_R'] = data['NET_INSUR_CONT_RSRV'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['DVD_EXP_INSURED_R'] = data['DVD_EXP_INSURED'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['REINSURANCE_EXP_R'] = data['REINSURANCE_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['CLAIM_EXP_RECOVERABLE_R'] = data['CLAIM_EXP_RECOVERABLE'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['INSUR_RSRV_RECOVERABLE_R'] = data['INSUR_RSRV_RECOVERABLE'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['REINSUR_EXP_RECOVERABLE_R'] = data['REINSUR_EXP_RECOVERABLE'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['OTHER_OPER_EXP_R'] = data['OTHER_OPER_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_INC_OTHER_OPS_R'] = data['NET_INC_OTHER_OPS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_GAIN_CHG_FV_R'] = data['NET_GAIN_CHG_FV'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_INVEST_INC_R'] = data['NET_INVEST_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['INC_INVEST_ASSOC_JV_ENTP_R'] = data['INC_INVEST_ASSOC_JV_ENTP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_GAIN_FX_TRANS_R'] = data['NET_GAIN_FX_TRANS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['OPPROFIT_R'] = data['OPPROFIT'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NON_OPER_REV_R'] = data['NON_OPER_REV'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NON_OPER_EXP_R'] = data['NON_OPER_EXP'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_LOSS_DISP_NONCUR_ASSET_R'] = data['NET_LOSS_DISP_NONCUR_ASSET'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TOT_PROFIT_R'] = data['TOT_PROFIT'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TAX_R'] = data['TAX'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['UNCONFIRMED_INVEST_LOSS_IS_R'] = data['UNCONFIRMED_INVEST_LOSS_IS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NET_PROFIT_IS_R'] = data['NET_PROFIT_IS'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['MINORITY_INT_INC_R'] = data['MINORITY_INT_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['NP_BELONGTO_PARCOMSH_R'] = data['NP_BELONGTO_PARCOMSH'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['OTHER_COMPREH_INC_R'] = data['OTHER_COMPREH_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TOT_COMPREH_INC_R'] = data['TOT_COMPREH_INC'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TOT_COMPREH_INC_MIN_SHRHLDR_R'] = data['TOT_COMPREH_INC_MIN_SHRHLDR'] / data['TOT_OPER_REV']
TOT_OPER_REV_R['TOT_COMPREH_INC_PARENT_COMP_R'] = data['TOT_COMPREH_INC_PARENT_COMP'] / data['TOT_OPER_REV']
insert_record = json.loads(TOT_OPER_REV_R.to_json(orient='records'))
ret = db.TOT_OPER_REV_R.insert_many(insert_record)
print(ret)



def NET_CASH_FLOWS_FNC_ACT_R():
    input_data = db.bond_cashflow
    data = pd.DataFrame(list(input_data.find()))
    cols = 'CASH_RECP_CAP_CONTRIB,CASH_REC_SAIMS,CASH_RECP_BORROW,OTHER_CASH_RECP_RAL_FNC_ACT,PROC_ISSUE_BONDS,STOT_CASH_INFLOWS_FNC_ACT,CASH_PREPAY_AMT_BORR,CASH_PAY_DIST_DPCP_INT_EXP,DVD_PROFIT_PAID_SC_MS,OTHER_CASH_PAY_RAL_FNC_ACT,STOT_CASH_OUTFLOWS_FNC_ACT,NET_CASH_FLOWS_FNC_ACT'
    for ss in cols.split(','):
        print("NET_CASH_FLOWS_FNC_ACT_R[\'" + ss +"_R\'] = " + "data[\'"+ss+"\'] / "+"data['NET_CASH_FLOWS_FNC_ACT']")
    NET_CASH_FLOWS_FNC_ACT_R = pd.DataFrame()
    NET_CASH_FLOWS_FNC_ACT_R['code'] = data['code'] 
    NET_CASH_FLOWS_FNC_ACT_R['rptDate'] = data['rptDate']
    NET_CASH_FLOWS_FNC_ACT_R['CASH_RECP_CAP_CONTRIB_R'] = data['CASH_RECP_CAP_CONTRIB'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['CASH_REC_SAIMS_R'] = data['CASH_REC_SAIMS'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['CASH_RECP_BORROW_R'] = data['CASH_RECP_BORROW'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['OTHER_CASH_RECP_RAL_FNC_ACT_R'] = data['OTHER_CASH_RECP_RAL_FNC_ACT'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['PROC_ISSUE_BONDS_R'] = data['PROC_ISSUE_BONDS'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['STOT_CASH_INFLOWS_FNC_ACT_R'] = data['STOT_CASH_INFLOWS_FNC_ACT'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['CASH_PREPAY_AMT_BORR_R'] = data['CASH_PREPAY_AMT_BORR'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['CASH_PAY_DIST_DPCP_INT_EXP_R'] = data['CASH_PAY_DIST_DPCP_INT_EXP'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['DVD_PROFIT_PAID_SC_MS_R'] = data['DVD_PROFIT_PAID_SC_MS'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['OTHER_CASH_PAY_RAL_FNC_ACT_R'] = data['OTHER_CASH_PAY_RAL_FNC_ACT'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['STOT_CASH_OUTFLOWS_FNC_ACT_R'] = data['STOT_CASH_OUTFLOWS_FNC_ACT'] / data['NET_CASH_FLOWS_FNC_ACT']
    NET_CASH_FLOWS_FNC_ACT_R['NET_CASH_FLOWS_FNC_ACT_R'] = data['NET_CASH_FLOWS_FNC_ACT'] / data['NET_CASH_FLOWS_FNC_ACT']
    insert_record = json.loads(NET_CASH_FLOWS_FNC_ACT_R.to_json(orient='records'))
    ret = db.NET_CASH_FLOWS_FNC_ACT_R.insert_many(insert_record)
    print(ret)

def NET_CASH_FLOWS_INV_ACT_R():
    cols = 'CASH_RECP_DISP_WITHDRWL_INVEST,CASH_RECP_RETURN_INVEST,NET_CASH_RECP_DISP_FIOLTA,NET_CASH_RECP_DISP_SOBU,OTHER_CASH_RECP_RAL_INV_ACT,STOT_CASH_INFLOWS_INV_ACT,CASH_PAY_ACQ_CONST_FIOLTA,CASH_PAID_INVEST,NET_INCR_PLEDGE_LOAN,NET_CASH_PAY_AQUIS_SOBU,OTHER_CASH_PAY_RAL_INV_ACT,STOT_CASH_OUTFLOWS_INV_ACT,NET_CASH_FLOWS_INV_ACT'
    for ss in cols.split(','):
        print("NET_CASH_FLOWS_INV_ACT_R[\'" + ss +"_R\'] = " + "data[\'"+ss+"\'] / "+"data['NET_CASH_FLOWS_INV_ACT']")
    NET_CASH_FLOWS_INV_ACT_R = pd.DataFrame()
    NET_CASH_FLOWS_INV_ACT_R['code'] = data['code'] 
    NET_CASH_FLOWS_INV_ACT_R['rptDate'] = data['rptDate']
    NET_CASH_FLOWS_INV_ACT_R['CASH_RECP_DISP_WITHDRWL_INVEST_R'] = data['CASH_RECP_DISP_WITHDRWL_INVEST'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['CASH_RECP_RETURN_INVEST_R'] = data['CASH_RECP_RETURN_INVEST'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['NET_CASH_RECP_DISP_FIOLTA_R'] = data['NET_CASH_RECP_DISP_FIOLTA'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['NET_CASH_RECP_DISP_SOBU_R'] = data['NET_CASH_RECP_DISP_SOBU'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['OTHER_CASH_RECP_RAL_INV_ACT_R'] = data['OTHER_CASH_RECP_RAL_INV_ACT'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['STOT_CASH_INFLOWS_INV_ACT_R'] = data['STOT_CASH_INFLOWS_INV_ACT'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['CASH_PAY_ACQ_CONST_FIOLTA_R'] = data['CASH_PAY_ACQ_CONST_FIOLTA'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['CASH_PAID_INVEST_R'] = data['CASH_PAID_INVEST'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['NET_INCR_PLEDGE_LOAN_R'] = data['NET_INCR_PLEDGE_LOAN'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['NET_CASH_PAY_AQUIS_SOBU_R'] = data['NET_CASH_PAY_AQUIS_SOBU'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['OTHER_CASH_PAY_RAL_INV_ACT_R'] = data['OTHER_CASH_PAY_RAL_INV_ACT'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['STOT_CASH_OUTFLOWS_INV_ACT_R'] = data['STOT_CASH_OUTFLOWS_INV_ACT'] / data['NET_CASH_FLOWS_INV_ACT']
    NET_CASH_FLOWS_INV_ACT_R['NET_CASH_FLOWS_INV_ACT_R'] = data['NET_CASH_FLOWS_INV_ACT'] / data['NET_CASH_FLOWS_INV_ACT']
    insert_record = json.loads(NET_CASH_FLOWS_INV_ACT_R.to_json(orient='records'))
    ret = db.NET_CASH_FLOWS_INV_ACT_R.insert_many(insert_record)
    print(ret)

def NET_CASH_FLOWS_OPER_ACT_R():
    cols = 'CASH_RECP_SG_AND_RS,RECP_TAX_RENDS,OTHER_CASH_RECP_RAL_OPER_ACT,NET_INCR_INSURED_DEP,NET_INCR_DEP_COB,NET_INCR_LOANS_CENTRAL_BANK,NET_INCR_FUND_BORR_OFI,NET_INCR_INT_HANDLING_CHRG,CASH_RECP_PREM_ORIG_INCO,NET_CASH_RECEIVED_REINSU_BUS,NET_INCR_DISP_TFA,NET_INCR_DISP_FIN_ASSETS_AVAIL,NET_INCR_LOANS_OTHER_BANK,NET_INCR_REPURCH_BUS_FUND,NET_CASH_FROM_SEURITIES,STOT_CASH_INFLOWS_OPER_ACT,NET_INCR_LENDING_FUND,NET_FINA_INSTRUMENTS_MEASURED_AT_FMV,CASH_PAY_GOODS_PURCH_SERV_REC,CASH_PAY_BEH_EMPL,PAY_ALL_TYP_TAX,OTHER_CASH_PAY_RAL_OPER_ACT,NET_INCR_CLIENTS_LOAN_ADV,NET_INCR_DEP_CBOB,CASH_PAY_CLAIMS_ORIG_INCO,HANDLING_CHRG_PAID,COMM_INSUR_PLCY_PAID,STOT_CASH_OUTFLOWS_OPER_ACT,NET_CASH_FLOWS_OPER_ACT'
    for ss in cols.split(','):
        print("NET_CASH_FLOWS_OPER_ACT_R[\'" + ss +"_R\'] = " + "data[\'"+ss+"\'] / "+"data['NET_CASH_FLOWS_OPER_ACT']")
    
    NET_CASH_FLOWS_OPER_ACT_R = pd.DataFrame()
    NET_CASH_FLOWS_OPER_ACT_R['code'] = data['code'] 
    NET_CASH_FLOWS_OPER_ACT_R['rptDate'] = data['rptDate']
    NET_CASH_FLOWS_OPER_ACT_R['CASH_RECP_SG_AND_RS_R'] = data['CASH_RECP_SG_AND_RS'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['RECP_TAX_RENDS_R'] = data['RECP_TAX_RENDS'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['OTHER_CASH_RECP_RAL_OPER_ACT_R'] = data['OTHER_CASH_RECP_RAL_OPER_ACT'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_INSURED_DEP_R'] = data['NET_INCR_INSURED_DEP'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_DEP_COB_R'] = data['NET_INCR_DEP_COB'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_LOANS_CENTRAL_BANK_R'] = data['NET_INCR_LOANS_CENTRAL_BANK'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_FUND_BORR_OFI_R'] = data['NET_INCR_FUND_BORR_OFI'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_INT_HANDLING_CHRG_R'] = data['NET_INCR_INT_HANDLING_CHRG'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['CASH_RECP_PREM_ORIG_INCO_R'] = data['CASH_RECP_PREM_ORIG_INCO'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_CASH_RECEIVED_REINSU_BUS_R'] = data['NET_CASH_RECEIVED_REINSU_BUS'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_DISP_TFA_R'] = data['NET_INCR_DISP_TFA'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_DISP_FIN_ASSETS_AVAIL_R'] = data['NET_INCR_DISP_FIN_ASSETS_AVAIL'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_LOANS_OTHER_BANK_R'] = data['NET_INCR_LOANS_OTHER_BANK'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_REPURCH_BUS_FUND_R'] = data['NET_INCR_REPURCH_BUS_FUND'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_CASH_FROM_SEURITIES_R'] = data['NET_CASH_FROM_SEURITIES'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['STOT_CASH_INFLOWS_OPER_ACT_R'] = data['STOT_CASH_INFLOWS_OPER_ACT'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_LENDING_FUND_R'] = data['NET_INCR_LENDING_FUND'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_FINA_INSTRUMENTS_MEASURED_AT_FMV_R'] = data['NET_FINA_INSTRUMENTS_MEASURED_AT_FMV'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['CASH_PAY_GOODS_PURCH_SERV_REC_R'] = data['CASH_PAY_GOODS_PURCH_SERV_REC'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['CASH_PAY_BEH_EMPL_R'] = data['CASH_PAY_BEH_EMPL'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['PAY_ALL_TYP_TAX_R'] = data['PAY_ALL_TYP_TAX'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['OTHER_CASH_PAY_RAL_OPER_ACT_R'] = data['OTHER_CASH_PAY_RAL_OPER_ACT'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_CLIENTS_LOAN_ADV_R'] = data['NET_INCR_CLIENTS_LOAN_ADV'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_INCR_DEP_CBOB_R'] = data['NET_INCR_DEP_CBOB'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['CASH_PAY_CLAIMS_ORIG_INCO_R'] = data['CASH_PAY_CLAIMS_ORIG_INCO'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['HANDLING_CHRG_PAID_R'] = data['HANDLING_CHRG_PAID'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['COMM_INSUR_PLCY_PAID_R'] = data['COMM_INSUR_PLCY_PAID'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['STOT_CASH_OUTFLOWS_OPER_ACT_R'] = data['STOT_CASH_OUTFLOWS_OPER_ACT'] / data['NET_CASH_FLOWS_OPER_ACT']
    NET_CASH_FLOWS_OPER_ACT_R['NET_CASH_FLOWS_OPER_ACT_R'] = data['NET_CASH_FLOWS_OPER_ACT'] / data['NET_CASH_FLOWS_OPER_ACT']
    
    insert_record = json.loads(NET_CASH_FLOWS_OPER_ACT_R.to_json(orient='records'))
    ret = db.NET_CASH_FLOWS_OPER_ACT_R.insert_many(insert_record)
    print(ret)

    
def TOT_EQUITY_RARIO():
    input_data = db.bondBalance
    data = pd.DataFrame(list(input_data.find()))
    cols = 'cap_stk,other_equity_instruments,other_equity_instruments_PRE,cap_rsrv,surplus_rsrv,undistributed_profit,tsy_stk,other_compreh_inc_bs,special_rsrv,prov_nom_risks,cnvd_diff_foreign_curr_stat,unconfirmed_invest_loss_bs,minority_int,eqy_belongto_parcomsh,tot_equity'
    for ss in cols.upper().split(','):
        print("TOT_EQUITY_RARIO[\'" + ss +"_R\'] = " + "data[\'"+ss+"\'] / "+"data['TOT_EQUITY']")
    
    TOT_EQUITY_RARIO = pd.DataFrame()
    TOT_EQUITY_RARIO['CAP_STK_R'] = data['CAP_STK'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['OTHER_EQUITY_INSTRUMENTS_R'] = data['OTHER_EQUITY_INSTRUMENTS'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['OTHER_EQUITY_INSTRUMENTS_PRE_R'] = data['OTHER_EQUITY_INSTRUMENTS_PRE'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['CAP_RSRV_R'] = data['CAP_RSRV'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['SURPLUS_RSRV_R'] = data['SURPLUS_RSRV'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['UNDISTRIBUTED_PROFIT_R'] = data['UNDISTRIBUTED_PROFIT'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['TSY_STK_R'] = data['TSY_STK'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['OTHER_COMPREH_INC_BS_R'] = data['OTHER_COMPREH_INC_BS'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['SPECIAL_RSRV_R'] = data['SPECIAL_RSRV'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['PROV_NOM_RISKS_R'] = data['PROV_NOM_RISKS'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['CNVD_DIFF_FOREIGN_CURR_STAT_R'] = data['CNVD_DIFF_FOREIGN_CURR_STAT'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['UNCONFIRMED_INVEST_LOSS_BS_R'] = data['UNCONFIRMED_INVEST_LOSS_BS'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['MINORITY_INT_R'] = data['MINORITY_INT'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['EQY_BELONGTO_PARCOMSH_R'] = data['EQY_BELONGTO_PARCOMSH'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['TOT_EQUITY_R'] = data['TOT_EQUITY'] / data['TOT_EQUITY']
    TOT_EQUITY_RARIO['code'] = data['code'] 
    TOT_EQUITY_RARIO['rptDate'] = data['rptDate']
    insert_record = json.loads(TOT_EQUITY_RARIO.to_json(orient='records'))
    ret = db.TOT_EQUITY_RARIO.insert_many(insert_record)
    print(ret)


def TOT_LIAB_RARIO():
    cols = 'st_borrow,tradable_fin_liab,notes_payable,acct_payable,adv_from_cust,empl_ben_payable,taxes_surcharges_payable,tot_acct_payable,int_payable,dvd_payable,oth_payable,acc_exp,deferred_inc_cur_liab,hfs_liab,non_cur_liab_due_within_1y,st_bonds_payable,borrow_central_bank,deposit_received_ib_deposits,loans_oth_banks,fund_sales_fin_assets_rp,handling_charges_comm_payable,payable_to_reinsurer,rsrv_insur_cont,acting_trading_sec,acting_uw_sec,oth_cur_liab,tot_cur_liab,lt_borrow,bonds_payable,lt_payable,lt_empl_ben_payable,specific_item_payable,provisions,deferred_tax_liab,deferred_inc_non_cur_liab,oth_non_cur_liab,tot_non_cur_liab,liab_dep_oth_banks_fin_inst,agency_bus_liab,cust_bank_dep,claims_payable,dvd_payable_insured,deposit_received,insured_deposit_invest,unearned_prem_rsrv,out_loss_rsrv,life_insur_rsrv,lt_health_insur_v,independent_acct_liab,prem_received_adv,pledge_loan,st_finl_inst_payable,oth_liab,derivative_fin_liab,tot_liab'
    for ss in cols.upper().split(','):
        print("TOT_LIAB_RARIO[\'" + ss +"_R\'] = " + "data[\'"+ss+"\'] / "+"data['TOT_LIAB']")
    
    TOT_LIAB_RARIO = pd.DataFrame()
            

    TOT_LIAB_RARIO['code'] = data['code'] 
    TOT_LIAB_RARIO['rptDate'] = data['rptDate']
    TOT_LIAB_RARIO['ST_BORROW_R'] = data['ST_BORROW'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['TRADABLE_FIN_LIAB_R'] = data['TRADABLE_FIN_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['NOTES_PAYABLE_R'] = data['NOTES_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['ACCT_PAYABLE_R'] = data['ACCT_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['ADV_FROM_CUST_R'] = data['ADV_FROM_CUST'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['EMPL_BEN_PAYABLE_R'] = data['EMPL_BEN_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['TAXES_SURCHARGES_PAYABLE_R'] = data['TAXES_SURCHARGES_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['TOT_ACCT_PAYABLE_R'] = data['TOT_ACCT_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['INT_PAYABLE_R'] = data['INT_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DVD_PAYABLE_R'] = data['DVD_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['OTH_PAYABLE_R'] = data['OTH_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['ACC_EXP_R'] = data['ACC_EXP'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DEFERRED_INC_CUR_LIAB_R'] = data['DEFERRED_INC_CUR_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['HFS_LIAB_R'] = data['HFS_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['NON_CUR_LIAB_DUE_WITHIN_1Y_R'] = data['NON_CUR_LIAB_DUE_WITHIN_1Y'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['ST_BONDS_PAYABLE_R'] = data['ST_BONDS_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['BORROW_CENTRAL_BANK_R'] = data['BORROW_CENTRAL_BANK'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DEPOSIT_RECEIVED_IB_DEPOSITS_R'] = data['DEPOSIT_RECEIVED_IB_DEPOSITS'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['LOANS_OTH_BANKS_R'] = data['LOANS_OTH_BANKS'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['FUND_SALES_FIN_ASSETS_RP_R'] = data['FUND_SALES_FIN_ASSETS_RP'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['HANDLING_CHARGES_COMM_PAYABLE_R'] = data['HANDLING_CHARGES_COMM_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['PAYABLE_TO_REINSURER_R'] = data['PAYABLE_TO_REINSURER'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['RSRV_INSUR_CONT_R'] = data['RSRV_INSUR_CONT'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['ACTING_TRADING_SEC_R'] = data['ACTING_TRADING_SEC'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['ACTING_UW_SEC_R'] = data['ACTING_UW_SEC'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['OTH_CUR_LIAB_R'] = data['OTH_CUR_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['TOT_CUR_LIAB_R'] = data['TOT_CUR_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['LT_BORROW_R'] = data['LT_BORROW'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['BONDS_PAYABLE_R'] = data['BONDS_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['LT_PAYABLE_R'] = data['LT_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['LT_EMPL_BEN_PAYABLE_R'] = data['LT_EMPL_BEN_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['SPECIFIC_ITEM_PAYABLE_R'] = data['SPECIFIC_ITEM_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['PROVISIONS_R'] = data['PROVISIONS'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DEFERRED_TAX_LIAB_R'] = data['DEFERRED_TAX_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DEFERRED_INC_NON_CUR_LIAB_R'] = data['DEFERRED_INC_NON_CUR_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['OTH_NON_CUR_LIAB_R'] = data['OTH_NON_CUR_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['TOT_NON_CUR_LIAB_R'] = data['TOT_NON_CUR_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['LIAB_DEP_OTH_BANKS_FIN_INST_R'] = data['LIAB_DEP_OTH_BANKS_FIN_INST'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['AGENCY_BUS_LIAB_R'] = data['AGENCY_BUS_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['CUST_BANK_DEP_R'] = data['CUST_BANK_DEP'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['CLAIMS_PAYABLE_R'] = data['CLAIMS_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DVD_PAYABLE_INSURED_R'] = data['DVD_PAYABLE_INSURED'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DEPOSIT_RECEIVED_R'] = data['DEPOSIT_RECEIVED'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['INSURED_DEPOSIT_INVEST_R'] = data['INSURED_DEPOSIT_INVEST'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['UNEARNED_PREM_RSRV_R'] = data['UNEARNED_PREM_RSRV'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['OUT_LOSS_RSRV_R'] = data['OUT_LOSS_RSRV'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['LIFE_INSUR_RSRV_R'] = data['LIFE_INSUR_RSRV'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['LT_HEALTH_INSUR_V_R'] = data['LT_HEALTH_INSUR_V'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['INDEPENDENT_ACCT_LIAB_R'] = data['INDEPENDENT_ACCT_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['PREM_RECEIVED_ADV_R'] = data['PREM_RECEIVED_ADV'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['PLEDGE_LOAN_R'] = data['PLEDGE_LOAN'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['ST_FINL_INST_PAYABLE_R'] = data['ST_FINL_INST_PAYABLE'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['OTH_LIAB_R'] = data['OTH_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['DERIVATIVE_FIN_LIAB_R'] = data['DERIVATIVE_FIN_LIAB'] / data['TOT_LIAB']
    TOT_LIAB_RARIO['TOT_LIAB_R'] = data['TOT_LIAB'] / data['TOT_LIAB']
        
    insert_record = json.loads(TOT_LIAB_RARIO.to_json(orient='records'))
    ret = db.TOT_LIAB_RARIO.insert_many(insert_record)
    print(ret)



def TOT_ASSETS_RARIO():
    cols = 'monetary_cap,tradable_fin_assets,notes_rcv,acct_rcv,oth_rcv,prepay,dvd_rcv,int_rcv,inventories,consumptive_bio_assets,deferred_exp,hfs_assets,non_cur_assets_due_within_1y,settle_rsrv,loans_to_oth_banks,margin_acct,prem_rcv,rcv_from_reinsurer,rcv_from_ceded_insur_cont_rsrv,red_monetary_cap_for_sale,tot_acct_rcv,oth_cur_assets,tot_cur_assets,fin_assets_avail_for_sale,held_to_mty_invest,invest_real_estate,long_term_eqy_invest,long_term_rec,fix_assets,proj_matl,const_in_prog,fix_assets_disp,productive_bio_assets,oil_and_natural_gas_assets,intang_assets,r_and_d_costs,goodwill,long_term_deferred_exp,deferred_tax_assets,loans_and_adv_granted,oth_non_cur_assets,tot_non_cur_assets,cash_deposits_central_bank,agency_bus_assets,rcv_invest,asset_dep_oth_banks_fin_inst,precious_metals,rcv_ceded_unearned_prem_rsrv,rcv_ceded_claim_rsrv,rcv_ceded_life_insur_rsrv,rcv_ceded_lt_health_insur_rsrv,insured_pledge_loan,cap_mrgn_paid,independent_acct_assets,time_deposits,subr_rec,mrgn_paid,seat_fees_exchange,clients_cap_deposit,clients_rsrv_settle,oth_assets,derivative_fin_assets,tot_assets'
    for ss in cols.upper().split(','):
        print("TOT_ASSETS_RARIO[\'" + ss +"_R\'] = " + "data[\'"+ss+"\'] / "+"data['TOT_ASSETS']")
      
    TOT_ASSETS_RARIO = pd.DataFrame()
        
    TOT_ASSETS_RARIO['MONETARY_CAP_R'] = data['MONETARY_CAP'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['TRADABLE_FIN_ASSETS_R'] = data['TRADABLE_FIN_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['NOTES_RCV_R'] = data['NOTES_RCV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['ACCT_RCV_R'] = data['ACCT_RCV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['OTH_RCV_R'] = data['OTH_RCV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['PREPAY_R'] = data['PREPAY'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['DVD_RCV_R'] = data['DVD_RCV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['INT_RCV_R'] = data['INT_RCV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['INVENTORIES_R'] = data['INVENTORIES'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['CONSUMPTIVE_BIO_ASSETS_R'] = data['CONSUMPTIVE_BIO_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['DEFERRED_EXP_R'] = data['DEFERRED_EXP'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['HFS_ASSETS_R'] = data['HFS_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['NON_CUR_ASSETS_DUE_WITHIN_1Y_R'] = data['NON_CUR_ASSETS_DUE_WITHIN_1Y'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['SETTLE_RSRV_R'] = data['SETTLE_RSRV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['LOANS_TO_OTH_BANKS_R'] = data['LOANS_TO_OTH_BANKS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['MARGIN_ACCT_R'] = data['MARGIN_ACCT'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['PREM_RCV_R'] = data['PREM_RCV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RCV_FROM_REINSURER_R'] = data['RCV_FROM_REINSURER'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RCV_FROM_CEDED_INSUR_CONT_RSRV_R'] = data['RCV_FROM_CEDED_INSUR_CONT_RSRV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RED_MONETARY_CAP_FOR_SALE_R'] = data['RED_MONETARY_CAP_FOR_SALE'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['TOT_ACCT_RCV_R'] = data['TOT_ACCT_RCV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['OTH_CUR_ASSETS_R'] = data['OTH_CUR_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['TOT_CUR_ASSETS_R'] = data['TOT_CUR_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['FIN_ASSETS_AVAIL_FOR_SALE_R'] = data['FIN_ASSETS_AVAIL_FOR_SALE'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['HELD_TO_MTY_INVEST_R'] = data['HELD_TO_MTY_INVEST'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['INVEST_REAL_ESTATE_R'] = data['INVEST_REAL_ESTATE'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['LONG_TERM_EQY_INVEST_R'] = data['LONG_TERM_EQY_INVEST'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['LONG_TERM_REC_R'] = data['LONG_TERM_REC'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['FIX_ASSETS_R'] = data['FIX_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['PROJ_MATL_R'] = data['PROJ_MATL'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['CONST_IN_PROG_R'] = data['CONST_IN_PROG'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['FIX_ASSETS_DISP_R'] = data['FIX_ASSETS_DISP'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['PRODUCTIVE_BIO_ASSETS_R'] = data['PRODUCTIVE_BIO_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['OIL_AND_NATURAL_GAS_ASSETS_R'] = data['OIL_AND_NATURAL_GAS_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['INTANG_ASSETS_R'] = data['INTANG_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['R_AND_D_COSTS_R'] = data['R_AND_D_COSTS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['GOODWILL_R'] = data['GOODWILL'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['LONG_TERM_DEFERRED_EXP_R'] = data['LONG_TERM_DEFERRED_EXP'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['DEFERRED_TAX_ASSETS_R'] = data['DEFERRED_TAX_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['LOANS_AND_ADV_GRANTED_R'] = data['LOANS_AND_ADV_GRANTED'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['OTH_NON_CUR_ASSETS_R'] = data['OTH_NON_CUR_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['TOT_NON_CUR_ASSETS_R'] = data['TOT_NON_CUR_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['CASH_DEPOSITS_CENTRAL_BANK_R'] = data['CASH_DEPOSITS_CENTRAL_BANK'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['AGENCY_BUS_ASSETS_R'] = data['AGENCY_BUS_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RCV_INVEST_R'] = data['RCV_INVEST'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['ASSET_DEP_OTH_BANKS_FIN_INST_R'] = data['ASSET_DEP_OTH_BANKS_FIN_INST'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['PRECIOUS_METALS_R'] = data['PRECIOUS_METALS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RCV_CEDED_UNEARNED_PREM_RSRV_R'] = data['RCV_CEDED_UNEARNED_PREM_RSRV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RCV_CEDED_CLAIM_RSRV_R'] = data['RCV_CEDED_CLAIM_RSRV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RCV_CEDED_LIFE_INSUR_RSRV_R'] = data['RCV_CEDED_LIFE_INSUR_RSRV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['RCV_CEDED_LT_HEALTH_INSUR_RSRV_R'] = data['RCV_CEDED_LT_HEALTH_INSUR_RSRV'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['INSURED_PLEDGE_LOAN_R'] = data['INSURED_PLEDGE_LOAN'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['CAP_MRGN_PAID_R'] = data['CAP_MRGN_PAID'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['INDEPENDENT_ACCT_ASSETS_R'] = data['INDEPENDENT_ACCT_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['TIME_DEPOSITS_R'] = data['TIME_DEPOSITS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['SUBR_REC_R'] = data['SUBR_REC'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['MRGN_PAID_R'] = data['MRGN_PAID'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['SEAT_FEES_EXCHANGE_R'] = data['SEAT_FEES_EXCHANGE'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['CLIENTS_CAP_DEPOSIT_R'] = data['CLIENTS_CAP_DEPOSIT'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['CLIENTS_RSRV_SETTLE_R'] = data['CLIENTS_RSRV_SETTLE'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['OTH_ASSETS_R'] = data['OTH_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['DERIVATIVE_FIN_ASSETS_R'] = data['DERIVATIVE_FIN_ASSETS'] / data['TOT_ASSETS']
    TOT_ASSETS_RARIO['TOT_ASSETS_R'] = data['TOT_ASSETS'] / data['TOT_ASSETS']
    
    TOT_ASSETS_RARIO['code'] = data['code'] 
    TOT_ASSETS_RARIO['rptDate'] = data['rptDate']
    
    insert_record = json.loads(TOT_ASSETS_RARIO.to_json(orient='records'))
    ret = db.TOT_ASSETS_RARIO.insert_many(insert_record)
    print(ret)
    
if __name__ == "__main__":
    print('finished')