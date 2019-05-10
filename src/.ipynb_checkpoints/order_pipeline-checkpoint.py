
import pandas as pd
import numpy as np
import time


# import ray
# ray.shutdown()
# ray.init(object_store_memory=40000000000,redis_max_memory=20000000000, num_cpus=8,
#          huge_pages=True, plasma_directory="/lessperm/hugepages")
# import modin.pandas as pd
# pdm.set_option('display.max_columns', 500)
# pdm.set_option('display.max_rows', 500)


import pickle


def drop_if(df,drop_list):
        updated_drop_list = []
        for col in drop_list:
            if col in df.columns:
                updated_drop_list.append(col)
        return updated_drop_list


def batch_convert_to_num(df,numeric_cols):
    df2 = df.copy()
    for col in df.columns:
        if col in numeric_cols:
            df2[col] = pd.to_numeric(df.loc[:,col],errors='coerce').copy()
            
#     drop_list = drop_if(df,numeric_cols)
    
#     df.drop(drop_list,axis=1,inplace=True)
    
    
#     for i in range(len(df.columns)):
#         if df.columns[i][-2:] =='_1':
#             df.columns[i] = df.columns[i][:-2]
    
    return df2

# def resets(cols):
#     for i in range(len(cols)):
#         if cols[i][-2:] =='_1':
#             cols[i] = cols[i][:-2]
#     return cols

# def batch_convert_to_time(df,time_cols):
#     for col in df.columns:
#         if col in time_cols:
#             df['{}_1'.format(col)] = pd.to_datetime(df.loc[:,col],yearfirst=True,errors='coerce').copy()
  
#     drop_list = drop_if(df,time_cols)
#     df.drop(drop_list,axis=1,inplace=True)
    
# #     for i in range(len(df.columns)):
# #         if df.columns[i][-2:] =='_1':
# #             df.columns[i] = df.columns[i][:-2]
#     return df

def get_nan_counts(df):
    coldf = pd.DataFrame({'colname':df.columns,'nancount': 0,'nancount2':0})

    for i in range(len(df.columns)):
        coldf.loc[i,'nancount']+= df[df.columns[i]].isna().sum()
    
    return coldf


def drop_high_nan(df, collist,colcol,nancol,thresh):
    for row in collist.itertuples():
        if row.nancount > thresh:
#             if row.colname != 'extra2':
            df = df.drop(row.colname,axis=1)

    return df


def convert_to_cats(df,obs2):
    for col in df.columns:
        if col in obs2:
            df[col] = np.where(df[col]==df[col],1,0)
    return df

def make_triple_cat(entry,cat1,cat2):
    if entry == cat1:
        return 1
    elif entry == cat2:
        return 0
    else:
        return np.nan
    
    
def make_quad_cat(entry,cat1,cat2,cat3):
    if entry == cat1:
        return 1
    elif entry == cat2:
        return 0
    elif entry == cat3:
        return -1
    else:
        return np.nan






def process_df(dataframe_url):
    
    
    timestart = time.time()
    checkpoint = time.time()
    cols = [
        'DriverID',
         'OrderTrackingID',
         'isFlagged',
         'AltOrderID',
         'TerminalID',
         'RouteID',
         'ClientID',
         'OnlineUserID',
         'toValidate',
         'AssignCSRID',
         'Status',
         'aTimeStamp',
         'DefaultDrvComm',
         'DrvAfterHours',
         'DrvWeight',
         'DrvWaitTime',
         'DrvPackage',
         'DrvStopOff',
         'DrvExtras',
         'DrvStopOffExtras',
         'DrvSurcharge',
         'DrvAdjAmt',
         'DrvCommTotal',
         'DriverPaid',
         'DriverCheckNo',
         'AutoRateBatchID',
         'BVRBatchID',
         'InvBatchID',
         'PaymentDate',
         'PaymentBalance',
         'TempID',
         'isLocked',
         'LockedBy',
         'LastUpdated',
         'IRT',
         'oDate',
         'OrderStartTime',
         'RandID',
         'EmployeeID',
         'ClientRefNo',
         'ClientSpecInstr',
         'Caller',
         'Department',
         'Phone',
         'Email',
         'SpecInstr',
         'PContact',
         'PPhone',
         'PStreet',
         'PStreet2',
         'PCity',
         'PState',
         'PZip',
         'PZone',
         'PSpecInstr',
         'DContact',
         'DPhone',
         'DStreet',
         'DStreet2',
         'DCity',
         'DState',
         'DZip',
         'DZone',
         'DSpecInstr',
         'ServiceID',
         'RoundTrip',
         'StopOffs',
         'VehicleID',
         'sWeight',
         'sValue',
         'DrvCollectAmt',
         'DrvCollectLoc',
         'COD',
         'OrderCharge',
         'RoundTripCharge',
         'MiscChargeDtl',
         'MiscCharge',
         'PackageCharge',
         'WeightCharge',
         'InsuranceCharge',
         'CODCharge',
         'AfterHoursCharge',
         'WaitTimeCharge',
         'StopOffCharge',
         'TotalCharge',
         'TotalExtras',
         'StopOffExtras',
         'TotalSurcharges',
         'Subtotal',
         'Tax1Amount',
         'Tax2Amount',
         'GrandTotal',
         'ETA',
         'ZoningSchemeID',
         'ZoningSchemeName',
         'PricingSchemeID',
         'PricingSchemeName',
         'PODname',
         'dEmployeeID',
         'dTimeStamp',
         'pWaitTime',
         'dWaitTime',
         'ApplyHolidayTariff',
         'OverRide',
         'Comments',
         'ClientRefNo2',
         'PAddressID',
         'DAddressID',
         'OtherChg1',
         'OtherDrv1',
         'OtherChg2',
         'OtherDrv2',
         'MassImportDataSetID',
         'FlightInfo',
         'SequenceNo',
         'SequenceNo2',
         'DrvSubAmtFromSTD',
         'DrvRoundTrip',
         'WirelessPOD',
         'SignatureRequired',
         'ClientRefNo3',
         'ClientRefNo4',
         'isMarked',
         'PODnameRT',
         'ClientDiscountAmt',
         'ClientDiscountDrvAmt',
         'OnlineDiscountAmt',
         'OnlineDiscountDrvAmt',
         'OverRideMileage',
         'PTimeZoneID',
         'DTimeZoneID',
         'PTimeZoneText',
         'DTimeZoneText',
         'CODloc',
         'PMapCode',
         'DMapCode',
         'WarehousingCharge',
         'PickupDispatched',
         'DeliveryDispatched',
         'OrderTrackingID_Hint',
         'PODsignature',
         'InvoiceID',
         'MileageTotal',
         'BlockCompletion',
         'PODsignatureRT',
         'POPsignature',
         'POPname',
         'sPieces',
         'TurnInBOL',
         'TurnInBOL_Confirmed',
         'COD_Confirmed',
         'DrvCollect_Confirmed',
         'GUID',
         'TollCharge',
         'HourlyCharge',
         'DrvTolls',
         'DrvHourly',
         'DCsegmentID',
         'DCsegment_Override',
         'PickupTargetFrom',
         'PickupTargetTo',
         'DeliveryTargetFrom',
         'DeliveryTargetTo',
         'PickupArrival',
         'PickupDeparture',
         'DeliveryArrival',
         'DeliveryDeparture',
         'PODcompletion',
         'POD_RTcompletion',
         'OverriddenDrvCommPercent',
         'PLocRefNo',
         'DLocRefNo',
         'PickupETA',
         'DeliveryETA',
         'IsGuestOrder',
        'extra1',
        'extra2']
    
    
    
    numeric_list = [
    'OrderTrackingID',
    'ClientID',
    'DefaultDrvComm',
    'DrvAfterHours',
    'DrvWeight',
    'DrvWaitTime',
    'DrvPackage',
    'DrvStopOff',
    'DrvExtras',
    'DrvStopOffExtras',
    'DrvSurcharge',
    'DrvCommTotal',
    'PaymentBalance',
    'RandID',
    'EmployeeID',
    'ServiceID',
    'StopOffs',
    'sWeight',
    'sValue',
    'DrvCollectAmt',
    'DrvCollectLoc',
    'OrderCharge',
    'RoundTripCharge',
    'MiscChargeDtl',
    'MiscCharge',
    'PackageCharge',
    'WeightCharge',
    'InsuranceCharge',
    'CODCharge',
    'AfterHoursCharge',
    'WaitTimeCharge',
    'StopOffCharge',
    'TotalCharge',
    'TotalExtras',
    'StopOffExtras',
    'TotalSurcharges',
    'Subtotal',
    'Tax1Amount',
    'Tax2Amount',
    'GrandTotal',
    'ZoningSchemeID',
    'dEmployeeID',
    'pWaitTime',
    'dWaitTime',
    'PAddressID',
    'DAddressID',
    'OtherChg1',
    'OtherDrv1',
    'OtherChg2',
    'OtherDrv2',
    'ClientDiscountAmt',
    'ClientDiscountDrvAmt',
    'OnlineDiscountAmt',
    'OnlineDiscountDrvAmt',
    'WarehousingCharge',
    'MileageTotal',
    'sPieces',
    'TollCharge',
    'HourlyCharge',
    'DrvTolls',
    'DrvHourly',
    'OverriddenDrvCommPercent',
    'PZone', 'DZone']

    
    drop_list = [
    'AltOrderID',
    'TerminalID',
    'RouteID',
    'OnlineUserID',
    'AssignCSRID',
    'DrvWeight',
    'DrvAdjAmt',
    'DriverPaid',
    'DriverCheckNo',
    'AutoRateBatchID',
    'BVRBatchID',
    'InvBatchID',
    'isLocked',
    'LockedBy',
    'LastUpdated',
    'Department',
    'ETA',
    'ZoningSchemeName',
    'PricingSchemeName',
    'ApplyHolidayTariff',
    'FlightInfo',
    'SequenceNo',
    'SequenceNo2',
    'DrvSubAmtFromSTD',
    'DrvRoundTrip',
    'WirelessPOD',
    'SignatureRequired',
    'PODnameRT',
    'OverRideMileage',
    'PTimeZoneID',
    'DTimeZoneID',
    'PTimeZoneText',
    'DTimeZoneText',
    'CODloc',
    'PMapCode',
    'DMapCode',
    'PickupDispatched',
    'DeliveryDispatched',
    'OrderTrackingID_Hint',
    'InvoiceID',
    'BlockCompletion',
    'PODsignatureRT',
    'POPsignature',
    'POPname',
    'TurnInBOL',
    'TurnInBOL_Confirmed',
    'DCsegmentID',
    'DCsegment_Override',
    'POD_RTcompletion',
    'OverriddenDrvCommPercent',
    'PLocRefNo',
    'DLocRefNo',
    'PickupETA',
    'DeliveryETA',
    'IsGuestOrder',
    'TempID','oDate','RandID',
    'DriverID','DriverIDnew',
    'extra1','extra2',
     'COD','isMarked']

    datetime_list = [
    'aTimeStamp',
    'PaymentDate',
    'dTimeStamp',
    'PickupTargetFrom',
    'PickupTargetTo',
    'DeliveryTargetFrom',
    'DeliveryTargetTo',
    'PickupArrival',
    'PickupDeparture',
    'DeliveryArrival',
    'DeliveryDeparture',
    'PODcompletion',
    'OrderStartTime'
    ]
    
    categoricals_list = [
    'isFlagged',
    'toValidate',
    'Status',
    'TempID',
    'IRT',
    'RoundTrip',
    'VehicleID',
    'COD',
    'OverRide',
    'MassImportDataSetID',
    'ClientRefNo3',
    'ClientRefNo4',
    'isMarked',
    ]


    print('===================importing dataframe...please wait===================')
    print('===================this program is extremely un-optimized...be patient===================')

    if dataframe_url == 'None':
        print('IMPORTING PICKLE...REMOVE AFTER DEMO')
        with open('orders_unprocessed','rb') as f:
            r_m = pickle.load(f)
            f.close()
        
    
#     if dataframe_url == 'None':
#         print('using demo default')
#         r_m = pd.read_csv('ALL_2019_HIRES_UNPROCESSED_csv.csv',
#                       header=None, 
#                       skiprows=[0],
#                       names=cols,
#                       sep=',',
#                       dtype='object')
        
        
    elif dataframe_url == 'f':
        return 'no orders imported, aborting this step'
    else:    
        try:
            r_m = pd.read_csv(dataframe_url,
                      header=None, 
                      skiprows=[0],
                      names=cols,
                      sep=',',
                      dtype='object')
            
        except UnicodeDecodeError:
            r_m = pd.read_excel(dataframe_url,
                      header=None, 
                      skiprows=[0],
                      names=cols,
                      sep=',',
                                low_memory=False,
                      dtype='object')
    
    
    
    print('\n ===================import complete. ===================')
    print('time elapsed since program start {}'.format(time.time()-timestart))
    print('time elapsed since last checkpoint {}'.format(time.time()-checkpoint))
    checkpoint = time.time()
    print('===================new time-checkpoint created...===================')
    print('===================processing...===================')

#     r_m = pd.DataFrame(r)
    r_m2 = r_m.loc[r_m['OrderTrackingID'] == r_m['OrderTrackingID']].copy()


    # for i in range(len(r_m2.columns)):
    #     if coldf.loc[i,'nancount2'] !=0:
    #         break
    #     coldf.loc[i,'nancount2']+= r_m2[r_m2.columns[i]].isna().sum()


    # coldf.sort_values('nancount2',ascending=False)

    r_m2['DriverIDnew'] = pd.to_numeric(r_m2.loc[:,'DriverID'], errors='coerce').copy()

    r_m2['DriverIDnew'] = r_m2.loc[:,'DriverIDnew'].astype('int',errors='ignore').copy()

    rm3 = r_m2[~r_m2['DriverIDnew'].isna()].copy()

    rm3['dID'] = rm3.loc[:,'DriverID'].astype('float').copy()

#     rm3 = rm3.drop(['DriverID','DriverIDnew'],axis=1).copy(deep=True)


    #about 300k rows imported incorrectly due to non-escaped commas.
    #this is why the extra1 and extra2 features had to be added.

    bad_import = rm3[rm3['extra1'] == rm3['extra1']]
    print("# of faulty rows which will be discarded: ", bad_import.shape[0])

    badindex = bad_import.index

    print('time elapsed since program start {}'.format(time.time()-timestart))
    print('time elapsed since last checkpoint {}'.format(time.time()-checkpoint))
    checkpoint = time.time()
    print('=================== new time-checkpoint created... ===================')
    print('=================== ...processing... ===================')
    



    rm4 = rm3[rm3['extra2']!=rm3['extra2']]
    rm5 = rm4[rm4['extra1']!=rm4['extra1']]

    
    
    drop_list[:] = [x for x in drop_list if x in rm5.columns]

    
    rm5.drop(drop_list,axis=1,inplace=True)
         
    print('converting numeric columns to numeric. this will take up to 3 minutes.')
    
    
    rm52 = batch_convert_to_num(rm5,numeric_list)
    print('conversion done. time elapsed since program start {}'.format(time.time()-timestart))
    print('time elapsed since last checkpoint {}'.format(time.time()-checkpoint))
    checkpoint = time.time()
    print('===================new time-checkpoint created...===================')
    print('===================processing...dropping high-nan columns===================')
    
    nancounts = get_nan_counts(rm52)    

    rm62 = drop_high_nan(rm52,nancounts,'colname','nancount',1846495)

    
    print('dropped high-nan columns, turning sparse columns into present-or-absent categoricals')

    rm6 = rm62.copy()

         
    rm62['Status'] = rm6.Status.apply(make_triple_cat, cat1='P',cat2= 'S')
    rm62 = rm62.drop(['isFlagged'],axis=1)
    rm62['toValidate'] = rm6.toValidate.apply(make_triple_cat, cat1='0',cat2= '1')
    rm62['IRT'] = rm6.IRT.apply(make_quad_cat, cat1='D',cat2= 'P', cat3='N')
    rm62['RoundTrip'] = rm6.RoundTrip.apply(make_triple_cat, cat1='N',cat2= 'Y')
    #post-script-1: adding a categorical for RTR y/n
    orders['RTR_job'] = np.where(orders['ClientID'].isin([26300]),1,0)
    #26300 seems to be the internal DB ID for RTR

    print('encoding vehicle IDs')
         
    vIds = [6, 15, 5, 9, 2, 4, 16, 1, 8, 0, 7, 10, 3]
    vIdsstr = ['6', '15', '5', '9', '2', '4', '16', '1', '8', '0', '7', '10', '3']
    #new cats based on vehicleID: is_bike, is_amazon_or_fk
    
    rm62.VehicleID = np.where(rm6.VehicleID.isin(vIdsstr), rm6.VehicleID, np.nan)
    rm62['VehicleID'] = pd.to_numeric(rm62['VehicleID'])
      
    rm62.OverRide = rm6.OverRide.apply(make_quad_cat, cat1='0',cat2='1',cat3='2')
    
    print('last steps')
    print('time elapsed since program start {}'.format(time.time()-timestart))
    print('time elapsed since last checkpoint {}'.format(time.time()-checkpoint))
    checkpoint = time.time()
    print('===================new time-checkpoint created...===================')
    print('===================processing...converting columns to datetime and categories, this can take up to 3 minutes ===================')

         #drop_list was originally here
    
#     rm72 = rm62.drop(drop_list,axis=1)
    # objects = rm72.columns[rm72.dtypes == 'object']

    obs2 = ['ClientRefNo', 'ClientSpecInstr', 'Caller', 'Phone', 'Email',
           'SpecInstr', 'PContact', 'PPhone','PSpecInstr', 'DContact', 'DPhone', 'DSpecInstr',
           'PODname', 'dTimeStamp', 'Comments', 'ClientRefNo2',
           'MassImportDataSetID', 'ClientRefNo3', 'ClientRefNo4',
           'COD_Confirmed', 'DrvCollect_Confirmed']

    
    rm73 = convert_to_cats(rm62,obs2)

#     rm73.drop(['extra2'],axis=1,inplace=True)
    # list(rm73.columns)
    
    rm73[datetime_list] = rm73[datetime_list].apply(pd.to_datetime, errors='coerce',yearfirst=True)

    rm73['nacounts'] = rm73.isnull().sum(axis=1)
    rm74 = rm73[rm73['nacounts'] < 10]



    status = 'DONE. At this point, we have: \n converted rows to datetime \n \
    conveted rows to numeric \n \
    converted rows to categoricals (1,0) or (-1,0,1) \n \
    dropped nearly unused columns \n \
    dropped rows which have a number of null values well above the average. \n \
    Now...we are ready to analyze. We will move to the next file - \n \
    Order_analysis_master \n'

    print('total time elapsed since function call: {}'.format(time.time()-timestart))
    

    print(status)
    
    return rm74


if __name__=="__main__":
#     dataframeurl = input('please enter location of dataframe: ')
    
        
    # dataframeurl='CourierData.xls'
    df = process_df()
    timein = time.strftime("%Y-%m-%d-%H-%M-%S")
    print('the time is now {}'.format(timein))
    print('WRITING PROCESSED ORDERS TO NEW CSV. BEWARE: THIS CURRENTLY TAKES UP TO 20 MINUTES TO COMPLETE.')
#     df.to_csv('orders_processed_{}'.format(timein))
    print('the time is now {}'.format(time.strftime("%Y-%m-%d-%H-%M-%S")))

    print('processed csv saved as orders_processed_{} in this folder'.format(timein))

