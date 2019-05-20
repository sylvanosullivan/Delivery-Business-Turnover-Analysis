import pandas as pd
import numpy as np
import time
import pickle
import sklearn
import xgboost as xgb
import os

# with open('DEMO_TEST_2019_FIRSTMONTHS','rb') as f:
#     demo_test = pickle.load(f)
#     f.close()

# demo_test_test = demo_test.drop(['churned','churned_2weeks'],axis=1)


#FULL PIPELINE

import src.orders_merge as orders_merge

import os


def make_scores(testdf):
    with open('ba_project/project_1_prod/XGBoost_may_model_FM','rb') as f:
        xgbFM = pickle.load(f)
        f.close()
    with open('ba_project/project_1_prod/XGBoost_may_model_AH','rb') as f:
        xgbAH = pickle.load(f)
        f.close()
    
    ahscores = xgbAH.predict_proba(testdf)
    fmscores = xgbFM.predict_proba(testdf)
    
    superscores = (ahscores * .7) + (fmscores * .3)
    
    print_supers = np.around(superscores[:,1],decimals=3)
    tc = testdf.copy()
    tc['risk_scores'] = print_supers
    print('all risk levels:')
    print('\t ID \t \t risk level')
    for row in tc.itertuples(index=True):
        print('\t' , row[0], '\t', np.around(row.risk_scores,decimals=2))
    
    print('HIGH RISK LEVELS:')
    print('\t ID \t \t risk level')
    for row in tc.itertuples(index=True):
        if row.risk_scores > .6:
            print('\t' , row[0], '\t', np.around(row.risk_scores,decimals=2))
    
    return tc



#om = one-month data, ah = all-hsitory data
def process_df():
    
    om, ah = orders_merge.process_df()
    time.sleep(1)
    
    
    droplist = ['CareerTotalDaysWorked', 'CareerTotalDeliveries','churned','churned_2weeks','DepartDate','ID','firstPickup','lastPickup','firstmonth',
                   'PackageCharge','DefaultDrvComm','ClientRefNo3','ClientRefNo2','Caller','toValidate',
                    'InsuranceCharge','DrvSurcharge', 'VehicleId','trip_delay','trip_time','WarehousingCharge']
    droplist = [i for i in droplist if i in om.columns]
    
    testdf=om.drop(droplist,axis=1)
    
    with open('/home/ubuntu/ba_project/project_1_prod/XGBoost_may_model_AH','rb') as f:
        xgbFM = pickle.load(f)
        f.close()
    with open('/home/ubuntu/ba_project/project_1_prod/XGBoost_may_model_AH','rb') as f:
        xgbAH = pickle.load(f)
        f.close()
    
    ahscores = xgbAH.predict_proba(testdf)
    fmscores = xgbFM.predict_proba(testdf)
    
    superscores = (ahscores * .7) + (fmscores * .3)
    
    print_supers = np.around(superscores[:,1],decimals=3)
#     tc = testdf.copy()
    om['risk_scores'] = print_supers
    print('all risk levels:')
    print('\t ID \t \t risk level')
    for row in om.itertuples(index=True):
        print('\t' , row[0], '\t', np.around(row.risk_scores,decimals=2))
    
    print('HIGH RISK LEVELS:')
    print('\t ID \t \t risk level')
    for row in om.itertuples(index=True):
        if row.risk_scores > .6:
            print('\t' , row[0], '\t', np.around(row.risk_scores,decimals=2))
    
    return om
    
    
    
if __name__=='__main__':
    print('assessing new hires since 01/01/19... \n')
    om, ah = orders_merge.process_df()
#     time.sleep(1)

    outputdf = make_scores(om)
    
    print('done')
    print('adding to DB...')
#     time.sleep(2)
    print('done')
    
    print('goodbye')