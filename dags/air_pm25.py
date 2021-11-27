from numpy import NaN, nan
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd 

default_args = {
    'start_date': datetime(2021, 1, 1)
}

#**********************************************************
@dag('air_pm25', schedule_interval='@daily', default_args=default_args, catchup=False)
#**********************************************************

def taskflow():  
    #**********************************************************   
    @task
    def Extract_Data_Station():
        import requests
        import json 
        from pandas import json_normalize
        #.....................................................      
        url      = "http://air4thai.pcd.go.th/services/getNewAQI_JSON.php"   
        response = requests.get(url)
        dictr    = response.json()                
        print(dictr)
        print('//........Complete @Extract_data_Station...........//')
        #.....................................................
        return dictr
        
    #**********************************************************    
    @task 
    def Transform_Data_Station(val) : 
        import json 
        from pandas import json_normalize
        #.....................................................        
        recs            = val['stations'] 
        df              = json_normalize(recs)
        dfStn           = df['stationID']
        dfStnn          = df['nameTH']
        dfLastDate      = df['LastUpdate.date']
        dfLastTime      = df['LastUpdate.time']
        dfLastPMvalue   = df['LastUpdate.PM25.value']
        dfLastAQILevel  = df['LastUpdate.AQI.Level']
        dfLastAQIaqi    = df['LastUpdate.AQI.aqi']
        dfResult        = pd.concat([dfStn,dfStnn ,dfLastDate,dfLastTime, dfLastPMvalue,dfLastAQILevel,dfLastAQIaqi] , axis=1)   
        #.....................................................    
        df2 = dfResult 
        df2.columns = [ 'stationID'      ,'nameTH'    , 'LastUpdate_date' 
                       ,'LastUpdate_time'      , 'LastUpdate_PM25_value' 
                       ,'LastUpdate_AQI_Level' , 'LastUpdate_AQI_aqi'   ]
        #.....................................................              
        df2 = df2.drop(df2[df2['LastUpdate_PM25_value']  == '-'].index)
        df2 = df2.reset_index(drop=True)
        print(df2)
        #.....................................................               
        print('//........Complete @Transform_Data_Station...........//')
        #.....................................................
        return df2.to_json()  
        
    
    #**********************************************************
    @task
    def Load_to_MYSQL(val) : 
        import json 
        from pandas import json_normalize
        from airflow.providers.mysql.hooks.mysql import MySqlHook
        import re
        #.....................................................
        dfWrite     = pd.read_json(val)
        hook        = MySqlHook(mysql_conn_id = 'mysql_testdb')
        sqltext     = """ insert into  `TablePM25`  ( 
                         stationID              , nameTH    
                        ,LastUpdate_date        , LastUpdate_time
                        ,LastUpdate_PM25_value  , LastUpdate_AQI_Level 
                        ,LastUpdate_AQI_aqi          ) 
                        values (%s ,%s ,%s ,%s ,%s ,%s ,%s) """         
        ncount      = len(dfWrite['stationID'])
        #.....................................................
        for irow in range ( ncount ) : 
            
            sNameTH = dfWrite['nameTH'][irow].lstrip()
            hook.run(sql= sqltext 
                     , parameters = (dfWrite['stationID'][irow]
                                    ,sNameTH
                                    ,dfWrite['LastUpdate_date'][irow]
                                    ,dfWrite['LastUpdate_time'][irow]
                                    ,dfWrite['LastUpdate_PM25_value'][irow]
                                    ,dfWrite['LastUpdate_AQI_Level'][irow]
                                    ,dfWrite['LastUpdate_AQI_aqi'][irow]
                     )
                     , autocommit=True)   
            print ('Rec.#',irow,'  >> Complete..')        
        #.....................................................
        return 1 
    
    
    #**********************************************************
    Load_to_MYSQL(Transform_Data_Station(Extract_Data_Station())) 
    #**********************************************************

dag = taskflow()