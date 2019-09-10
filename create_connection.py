import mysql.connector
import os,sys,logging,json
import pandas as pd
import openpyxl,time
from openpyxl import load_workbook
from list_col_viri import viri_label_list
from datetime import datetime,timedelta
from list_col_viri import viri_label_list




def create_connectn(para1,para2,db_qry,para4,para5):
    # para1:current date,para2:previous date,para4:vehicleName,para5:timestmp
    cnctn = mysql.connector.connect(host='tpsan1srv01',
        user='root',
        passwd='FifthRiver7#',
        db='telematics')
    cursor = cnctn.cursor() 
    # print(para2,para4)
    try:    
        # cursor.execute(db_qry,{"p": "%{}".format(para1)})
        cursor.execute(db_qry,{"p": "%{}".format(para2)})
    except:
        try:
            # cursor.execute(db_qry,{"p": "%{}".format(curr_date),"p1": "%{}".format(para)})
            cursor.execute(db_qry,{"p": "%{}".format(para2),"p1": "%{}".format(para4)})
        except:
            try:
                cursor.execute(db_qry,{"p": "%{}".format(para2),"p1": "%{}".format(para4),"p2": "%{}".format(para5)})

            except:
                print("some connection exception")
    
          
    
    
    # Put it all to a data frame
    sql_df = pd.DataFrame(cursor.fetchall())  
    # print(sql_df) 
    sql_df.columns = cursor.column_names 
    # print(sql_df.columns)
    cursor.execute("Select `viriciti_name`,`fleet_name`,`logger` from `telematics`.`viriciti_vehicle_mapping`")
    vehicle_df=pd.DataFrame(cursor.fetchall())
    # print(vehicle_df)
    vehicle_df.columns = cursor.column_names
    # print(vehicle_df.viriciti_name,vehicle_df.fleet_name)
    veh_nam=dict(zip(vehicle_df.viriciti_name,vehicle_df.fleet_name))
    # print(veh_nam)
    veh_logr=dict(zip(vehicle_df.viriciti_name,vehicle_df.logger)) 
    cnctn.commit()  
    cursor.close()
    cnctn.close()    
    
    return sql_df,vehicle_df,veh_logr,veh_nam,sql_df.columns
