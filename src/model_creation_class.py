#Model creation and manipulation class

class Model_Creation(object):
    
    
    """
    
    this should contain all the necessary ingredients for creating the models.
    First, we need to define a split in the data.
    Second, we need to fit the models.
    Third, we need to pickle and return the models (both)
    Fourth, we need to pickle and return the split sets, or at least, the test sets, for additional testing
    
    """
    
    def __init__(self,df):
        self.df = df
        droplist = ['CareerTotalDaysWorked', 'CareerTotalDeliveries','churned','churned_2weeks','DepartDate','ID','firstPickup','lastPickup','firstmonth',
                   'PackageCharge','DefaultDrvComm','ClientRefNo3','ClientRefNo2','Caller','toValidate',
                    'InsuranceCharge','DrvSurcharge', 'VehicleId','trip_delay','trip_time','WarehousingCharge']
        self.droplist = droplist
        from sklearn.model_selection import train_test_split
        self.tts = train_test_split
        import pandas as pd
        self.pd = pd

    
    def other_vars(self,new_hire,courier_demos):
        self.new_hire = new_hire
        self.courier_demos = courier_demos
    
    def random_split(self):
        import orders_merge
        FM,AH,churn_tracking = orders_merge.middle_process(self.df,self.new_hire)
        final_first_month_orders = orders_merge.final_process(FM,self.new_hire,self.courier_demos,churn_tracking)
        final_all_history_orders = orders_merge.final_process(AH,self.new_hire,self.courier_demos,churn_tracking)
    
        ah_y = final_all_history_orders['churned']
        ah_x = final_all_history_orders.drop(self.droplist,axis=1)
        
        fm_y = final_first_month_orders['churned']
        fm_x = final_first_month_orders.drop(self.droplist,axis=1)
        
        self.ah_xtrain_random,self.ah_xtest_random,self.ah_ytrain_random,self.ah_ytest_random = self.tts(ah_x,ah_y)
        self.fm_xtrain_random,self.fm_xtest_random,self.fm_ytrain_random,self.fm_ytest_random = self.tts(fm_x,fm_y)
        
        

    def date_split(self,year,month,day):
        import src.orders_merge
        self.om=orders_merge
        
        FM,AH, churn_tracking= self.om.middle_process(self.df,self.new_hire)
        self.ah = AH
        self.churn_tracking = churn_tracking
        
        FM_train = FM.loc[FM['firstPickup'] < self.pd.Timestamp(year=year,month=month,day=day)]
        FM_test = FM.loc[FM['firstPickup'] > self.pd.Timestamp(year=year,month=month,day=day)]
        
        AH_train = AH.loc[AH['firstPickup'] < self.pd.Timestamp(year=year,month=month,day=day)]
        AH_test = AH.loc[AH['firstPickup'] > self.pd.Timestamp(year=year,month=month,day=day)]
        
        final_first_month_orders_train = self.om.final_process(FM_train,self.new_hire,self.courier_demos,churn_tracking)
        final_all_history_orders_train = self.om.final_process(AH_train,self.new_hire,self.courier_demos,churn_tracking)
        
#         FM_test,AH_test,churn_tracking_test = orders_merge.middle_process(test,self.new_hire)
        final_first_month_orders_test = self.om.final_process(FM_test,self.new_hire,self.courier_demos,churn_tracking)
        final_all_history_orders_test = self.om.final_process(AH_test,self.new_hire,self.courier_demos,churn_tracking)

        #First-month train and test sets
        self.fm_xtrain_date = final_first_month_orders_train.drop(self.droplist,axis=1)
        self.fm_xtest_date = final_first_month_orders_test.drop(self.droplist,axis=1)
        
        self.fm_ytrain_date = final_first_month_orders_train['churned']
        self.fm_ytest_date = final_first_month_orders_test['churned']
        
        #all-history train and test sets
        self.ah_xtrain_date = final_all_history_orders_train.drop(self.droplist,axis=1)
        self.ah_xtest_date = final_all_history_orders_test.drop(self.droplist,axis=1)
        
        self.ah_ytrain_date = final_all_history_orders_train['churned']
        self.ah_ytest_date = final_all_history_orders_test['churned']
        
        #save date-separated DFs for visualization
        self.fm_train = final_first_month_orders_train
        self.fm_test = final_first_month_orders_test
        
        self.processed_df = self.pd.concat([final_first_month_orders_train,final_first_month_orders_test],
                                            keys=['train','test'])
        
        
    def create_model_random_split(self):
        import xgboost as xgb
        self.fm_params = {'max_depth': 2, 'min_samples_leaf': 1, 'min_samples_split': 100, 'subsample': 0.3, 
             'n_estimators':100,'learning_rate':0.2, 'max_features': 'sqrt',
                }
        
        self.xgboost_fm_random= xgb.XGBClassifier(**self.fm_params)
        self.fm_random_fitted = self.xgboost_fm_random.fit(self.fm_xtrain_random,self.fm_ytrain_random)

        
        self.xgboost_ah_random= xgb.XGBClassifier(**self.fm_params)
        self.ah_random_fitted = self.xgboost_ah_random.fit(self.ah_xtrain_random,self.ah_ytrain_random)

        
        
        
    def create_model_date_split(self):
        import xgboost as xgb
        self.fm_params = {'max_depth': 2, 'min_samples_leaf': 1, 'min_samples_split': 100, 'subsample': 0.3, 
             'n_estimators':100,'learning_rate':0.2, 'max_features': 'sqrt',
                }

        self.xgboost_fm_datesplit= xgb.XGBClassifier(**self.fm_params)
        self.fm_datesplit_fitted = self.xgboost_fm_datesplit.fit(self.fm_xtrain_date,self.fm_ytrain_date)

        self.xgboost_ah_datesplit= xgb.XGBClassifier(**self.fm_params)
        self.ah_datesplit_fitted = self.xgboost_ah_datesplit.fit(self.ah_xtrain_date,self.ah_ytrain_date)
