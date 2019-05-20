import pandas as pd
import numpy as np
import dask.dataframe as dd
import dask.array as da
import pickle
import time
import sklearn
import statsmodels.api
import datetime
import time
import os
import src.order_pipeline as order_pipeline
import src.pipeline_courier_data as pipeline_courier_data






    
def first_import(): 
    
    ####importing hiring data or not####
    
    
    while True:
        accepted = ['t','T','f','F']
        
        new_hire = input('Any new hires since last run? If yes, please enter "t", else enter "f": ')
        if not new_hire in accepted:
            print('response not accepted, try again. \n')
            continue
        else:
            new_hire = new_hire.lower()
            break
    
    
    
    if new_hire=='f':
        courier_demos = None
    else:
        cdU = input('enter url or filepath of hiring data csv or excel file: ')
        
        courier_demos = pipeline_courier_data.process_df(cdU)

    
    
    ####process flags on order data
    
    
    
    
    
    ordersurl = input('enter url or filepath of order data csv or excel file, \
            or enter "f" to skip importing order data: ')
    
    print(' $$$$$$$ creating processed orders dataframe. \
        $$$$$$$ \n please wait \n $$$$$$$')
    
    orders = order_pipeline.process_df(ordersurl)

    if type(orders) == str:
        return 'no order data entered, aborting process', _
            
    print(' $$$$$$$ imports complete. $$$$$$$ \n \
            now performing final process and merge steps.\n \
            this will only take a moment. \n \
          please wait \n $$$$$$$')
    return orders, new_hire, courier_demos
    


    

def middle_process(orders,new_hire):    
    
    
    orders.sort_values('PickupArrival',inplace=True)
    orders['trip_time'] = orders['PODcompletion'] - orders['PickupArrival']
    orders['trip_delay'] = orders['PickupArrival'] - orders['PickupTargetTo']

    fj2 = orders.groupby('dID').agg({'PickupArrival':['first','last'],
                                    })
    fj2.columns = fj2.columns.get_level_values(0)

    fj2.columns = ['firstPickup','lastPickup']
    fj2['churned'] = np.where(fj2['lastPickup'] < (fj2['firstPickup'] + np.timedelta64(1,'M')),1,0)
    fj2['churned_2weeks'] = np.where(fj2['lastPickup'] < (fj2['firstPickup'] + np.timedelta64(14,'D')),1,0)
    fj2['firstmonth'] = fj2['firstPickup'] + np.timedelta64(1,'M')
    
    tdelay=orders.groupby('dID')['trip_delay'].apply(np.mean)
    ttime=orders.groupby('dID')['trip_time'].apply(np.mean)
    timedf = pd.merge(tdelay,ttime,on='dID')
    fj2 = pd.merge(fj2,timedf,on='dID')



    # fj_reset.columns = ['dID','firstTS','lastTS','MilageAvg','PZoneAvg','DZoneAvg','DrvCommAvg','firstTSplusmonth','churned']
        
    order_plus_dates = pd.merge(orders,fj2,left_on='dID', right_index=True)
    first_month_plus_dates = order_plus_dates.loc[order_plus_dates['firstmonth'] > order_plus_dates['firstPickup']].copy()
    
    return first_month_plus_dates,  order_plus_dates,fj2



def final_process(orders, new_hire, courier_demos,churn_tracker):
    
    
     
    meancols = [
        'MileageTotal','DrvCommTotal',
    #     clientID,mode
     'toValidate',
     'DefaultDrvComm',
     'DrvAfterHours',
     'DrvWaitTime',
     'DrvPackage',
    #  'DrvStopOff',
     'DrvExtras',
     'DrvStopOffExtras',
     'DrvSurcharge',
     'IRT',
     'ClientRefNo',
     'ClientSpecInstr',
     'Caller',
     'Phone',
     'Email',
     'SpecInstr',
     'PContact',
     'PPhone',
     'PSpecInstr',
     'DContact',
     'DPhone',
     'DSpecInstr',
     'RoundTrip',
    #  'VehicleID',mode
     'sWeight',
     'sValue',
     'RoundTripCharge',
     'MiscCharge',
     'PackageCharge',
     'WeightCharge',
     'InsuranceCharge',
     'CODCharge',
#      'ZoningSchemeID',
     'PODname',
     'OverRide',
     'Comments',
        'RTR_job',
     'ClientRefNo2',
        'MassImportDataSetID',
     'ClientRefNo3',
     'ClientRefNo4',
    #  'ClientDiscountAmt',
    #  'ClientDiscountDrvAmt',
    #  'OnlineDiscountAmt',
    #  'OnlineDiscountDrvAmt',
     'WarehousingCharge',
     'sPieces',
     'COD_Confirmed',
     'DrvCollect_Confirmed',
     'TollCharge',
     'HourlyCharge',
    #  'DrvTolls', no info
     'DrvHourly',
    #     need to make deltas related to time-of-day with below
    #  'PickupTargetFrom',
    #  'PickupTargetTo',
    #  'DeliveryTargetFrom',
    #  'DeliveryTargetTo',
    #  'PickupArrival',
    #  'PickupDeparture',
    #  'DeliveryArrival',
    #  'DeliveryDeparture',
    #  'PODcompletion',
     'Status',
    'PZone',
    'DZone']
    
    #features found through testing to have a non-zero contribution to both the XGboost and SGD models
    
    
    meaningful_features = [
        'ID', #need to insert this at the front of table
        'ZoningSchemeID', 'ClientRefNo3', 'DrvSurcharge', 'RTR_job','ClientRefNo2',
           'OverRide', 
#                            'daily_avg', #commenting out to focus on job quality
                           'DrvAfterHours', 'ClientRefNo4',
           'DriverCategoryID2', 'DrvExtras', 'DContact', 'PSpecInstr', 'Phone',
           'Comments', 'SpecInstr', 'RoundTrip', 'toValidate',
           'driver_extra_charges_avg', 'sValue', 'Caller', 'PContact', 'Status',
           'PPhone', 'DSpecInstr', 'DPhone', 'DrvPackage', 'ClientRefNo',
           'Email', 'sWeight', 'MileageTotal', 'DefaultDrvComm',
           'PZone', 'DZone', 'ClientSpecInstr', 'DepartDate', 'DrvCommTotal',
           'state_is_NY', 'sPieces', 'zone_diff', 'IRT',
           'start_month_quarter_second', 'start_season_winter', 'birth_decade',
           'start_season_spring', 'InsuranceCharge', 'WeightCharge',
           'start_month_quarter_first', 'start_season_summer', 'VehicleId',
           'start_month_quarter_fourth', 'start_season_fall', 'state_is_NJ',
           'start_month_quarter_third', 'DrvWaitTime', 'PackageCharge',
                          'CareerTotalDaysWorked',
        'CareerTotalDeliveries','ttime','tdelay',
        'firstPickup','lastPickup','firstmonth','churned'#adding in totaldaysworked for tracking purposes
    ]
    
#     meandict = {k:'mean' for k in meancols if k in meaningful_features}
    meandict = {k:'mean' for k in meancols}
    fjl = orders.groupby('dID').agg(meandict)
    fjl['zone_diff'] = fjl['DZone'] - fjl['PZone']
    fjl=pd.merge(fjl,churn_tracker, right_index=True,left_index=True)
    
    
    
    
    
    
    if new_hire == 'f':
        mftrunc = [x for x in meaningful_features if x in fjl.columns]
        fjl['driver_extra_charges_avg'] = fjl['DrvCommTotal'] - fjl['DefaultDrvComm']
        return fjl
        
#         return fjl[mftrunc] 
    
    
    else:

        cdm = pd.merge(fjl, courier_demos,left_index=True,right_on='ID')

        cdm['driver_extra_charges_avg'] = cdm['DrvCommTotal'] - cdm['DefaultDrvComm']
        cdm = cdm.dropna(axis=0)
#         cdm = cdm.loc[cdm['CareerTotalDeliveries'] > cdm['CareerTotalDaysWorked']]
        cdm = cdm.loc[\
          ((cdm['CareerTotalDaysWorked'] > 27) & (cdm['churned']==0)) \
         | ((cdm['CareerTotalDaysWorked'] <= 27) & (cdm['churned']==1)) \
         ]


        mftrunc = [x for x in meaningful_features if x in cdm.columns]
        return cdm
#         return cdm[mftrunc]



def process_df():
    imported,new_hire,courier_demos = first_import()
    time.sleep(2)
    FM, AH,churn_tracker = middle_process(imported,new_hire)
    time.sleep(2)

    final_one_month_orders = final_process(FM,new_hire,courier_demos,churn_tracker)
    final_all_history_orders = final_process(AH,new_hire,courier_demos,churn_tracker)
    time.sleep(2)

    print(' =$=$=$=$=$=$=$=$=$=$=$=$=$=$=$=$=$=$ ---->> total process done <<---$=$=$=$=$=$=$=$=$=$=$=$=$=$=$=$=$=$= ')
    return final_one_month_orders, final_all_history_orders

    