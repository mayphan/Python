import ast
import mysql.connector
import os,sys,logging,json
from datetime import datetime,timedelta
import pandas as pd
import time,tzlocal,logging

def time_convrt(msg): 
    # print(msg)   
    time_stmp=(msg["time"]/1000)
    timezon=tzlocal.get_localzone()
    local_time = str(datetime.fromtimestamp(time_stmp, timezon)).split(" ")  
    # print(local_time)
    return local_time

def upload_raw_data(prev_date,file_path):

    try:
        curr_date=datetime.now().strftime('%Y-%m-%d')
        # prev_date= (datetime.now() + timedelta(days=-4)).strftime('%Y-%m-%d')
        # prev_date_prev= (datetime.now() + timedelta(days=-2)).strftime('%Y-%m-%d')
        # file_path=os.path.join("E:\\","Company","truck_data","Viriciti","JOSN_files",)
        file_name=os.path.join(file_path,"{}_viriciti_data.txt".format(prev_date))
        # print(file_name)
        # print(prev_date)
        count=0
        data_to_list=[]
        
        with open(file_name,'r') as file_:
            read_msg=file_.read().split(';')
        # print(len(read_msg)
        
        
        
        # for val in reversed(read_msg):
        for val in read_msg:
            
            if val=="":
                pass
            else:
                dictionary =  json.loads(val)      
                data_to_list.append(dictionary)
        # print(data_to_list[0]["time"])

        cnctn = mysql.connector.connect(host='tpsan1srv01',
            user='root',
            passwd='FifthRiver7#',
            db='telematics')
        cursor = cnctn.cursor()  

        for msg in data_to_list:
            # print(msg)
            datetime_pst=time_convrt(msg)     
            day_pst="{}".format(datetime_pst[0])
            time_pst="{}".format(datetime_pst[1].split("-")[0])
            label_msg=msg["label"].split(".")[1]
            print(day_pst,time_pst)
            # cursor.execute("INSERT INTO `telematics`.`viriciti_raw_data_testing` (vehicle_name,date_,time_,label,value) VALUES (%s,%s,%s,%s,%s)",
            # (msg["vid"],day_pst,time_pst,label_msg,msg["value"]))
            cursor.execute("INSERT INTO `telematics`.`viriciti_raw_data` (vehicle_name,date_,time_,label,value) VALUES (%s,%s,%s,%s,%s)",
            (msg["vid"],day_pst,time_pst,label_msg,msg["value"]))
            cnctn.commit()
        cursor.close()
        cnctn.close()
        print("Done")
    except:
        try:
            print("------------------------------------------------------File not present for today-------------------------------------------")
            LogDirectory = os.path.join("E:\\","Company","truck_data","Viriciti","JOSN_files","logs")
            LogFilepath = os.path.join(LogDirectory ,"DB_{}.log".format(curr_date))            
            logging.basicConfig(filename=LogFilepath,filemode='a+',level=logging.CRITICAL)       
            exc_type, exc_value, exc_traceback = sys.exc_info()                    
            logging.critical('{}:{} raised for message: {}'.format(exc_type,exc_value,msg))

        except:
            logging.exception("Exception occurred in upload_to_DB script")
            pass
        

# upload_raw_data()


   
    
        
       
      
                      
            
    