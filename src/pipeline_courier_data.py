
import pandas as pd
import numpy as np
import time
import os


def batch_todt(df, targetcols):
    for col in targetcols:
        df[col] = pd.to_datetime(df[col])
    return df

def season_enc(entry):
    winter = [12,1,2]
    spring = [3,4,5]
    summer = [6,7,8]
    fall = [9,10,11]

    season_dict = {'winter' :[12,1,2],'spring':[3,4,5],'summer':[6,7,8],'fall':[9,10,11]}

    for k,v in season_dict.items():
        if entry in v:
            return k
    return 'UNKNOWN_START'

def quarter_of_month_enc(entry):
    week_dict = {'first': [1,2,3,4,5,6,7,8], 'second':[9,10,11,12,13,14,15,16],'third':[17,18,19,20,21,22,23,24],'fourth':[*range(25,35)]}

    for k,v in week_dict.items():
        if entry in v:
            return k
    return 'UNKNOWN_START'



def get_decade(entry):
    if str(entry) == 'NaT':
        return -1

    date = str(entry)
    decade = int(date[2])
    if int(date[:4]) > 1999:
        decade += 10
    return decade


def state_encoder(entry):
    list = ['NY','NJ','PA','CT','Unknown']
    if entry in list:
        return entry
    else:
        return 'OTHER'


def drop_if(df,drop_list):
        updated_drop_list = []
        for col in drop_list:
            if col in df.columns:
                updated_drop_list.append(col)
        return updated_drop_list


def process_df(dataframe_url):
    
    if dataframe_url == 'f':
        return 'no hiring data imported, aborting this step'
    elif dataframe_url =='demo':
        print('using demo default')

        cd1 = pd.read_excel('CourierData.xls')
    else:    
        try:
            cd1 = pd.read_csv(dataframe_url)
        except UnicodeDecodeError:
            cd1 = pd.read_excel(dataframe_url)


    #create daily avg figure
    cd1['daily_avg'] = cd1['CareerTotalDeliveries'].astype('float') / cd1['CareerTotalDaysWorked']
    cd1 = cd1.loc[cd1['daily_avg'] > 1.5]
    #encode the presence of depart date as a 1/0
    cd1['DepartDate'] = np.where(cd1['DepartDate']==cd1['DepartDate'],1,0)

    #pare down birthdate to birth decade, change this to age at hire later
    cd1['birth_decade'] = cd1['BirthDate'].apply(get_decade)
    # decadelist = [x for x in cd1['birth_decade'] if x > 0]
    # decade_mean = np.around(np.mean(decadelist),decimals=0)
    #decade_mean is equal to 8
    cd1['birth_decade'] = np.where(cd1['birth_decade']==-1,8,cd1['birth_decade'])
    #there are 127 driver entries for whom their 'birthdate' is listed as being in the futre, sometimes as far as 2078.
    #since the number is relatively small, we will ignore this for now
    #and set their decades to 8, the supposed mean
    cd1['birth_decade'] = np.where(cd1['birth_decade']>10,8,cd1['birth_decade'])
    if 'birth_decade' in cd1.columns:
        print('birth decade added')
    else:
        print('uh oh')

#     cd1['City'] = cd1['City'].fillna('Unknown')
    # we are ignoring city and ZIP for now, state should do
    cd1['State'] = cd1['State'].fillna('Unknown')
    cd1['state_is'] = cd1['State'].apply(state_encoder)
    cd1 = pd.get_dummies(cd1, columns = ['state_is'], drop_first=False)

    cd2 = cd1.copy(deep=True)

    cd2['start_q'] = pd.DatetimeIndex(cd2['StartDate']).quarter
    cd2['start_mo'] = pd.DatetimeIndex(cd2['StartDate']).month
    cd2['start_day'] = pd.DatetimeIndex(cd2['StartDate']).day
    cd2['start_season']=cd2['start_mo'].apply(season_enc)
    cd2['start_month_quarter']=cd2['start_day'].apply(quarter_of_month_enc)

    print('start-date info added')
    cd2 = pd.get_dummies(cd2,columns=['start_season','start_month_quarter'],drop_first=False)


    drop_list = [
        'City','State','ZipCode',
        'start_mo','start_day',
        'DriverStatus', 'BirthDate','StartDate'
    ]


    cd3 = cd2.drop(drop_if(cd2,drop_list),axis=1)
    
    #vehicleID categorization, ported from orders_pipeline
    cd3['is_bike'] = np.where(cd2.VehicleId.isin([5,6,13,11,12]),1,0)
    cd3['is_amazon_or_fk']=np.where(cd2.VehicleId.isin([15,16,7]),1,0)
    cd3['is_trucking']=np.where(cd2.VehicleId.isin([1,2,3,4,8,9,
                                                  12,14,10]),1,0)


    #done processing courier data
    #this should work for newly imported couriers
    print('DONE')
    return cd3





#example usage:
# df = process_courier_dataframe('/home/ubuntu/ba_data/CourierData.xls')


if __name__=="__main__":
    dataframeurl = input('please enter location of dataframe: ')
    # dataframeurl='CourierData.xls'
    df = process_df(dataframeurl)
    timein = time.strftime("%Y-%m-%d-%H-%M-%S")
    print(timein)
    df.to_csv('courier_processed_{}'.format(timein))
    print('processed csv saved as courier_processed_{} in this folder'.format(timein))
