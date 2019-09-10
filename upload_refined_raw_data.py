import mysql.connector
import logging
import os,sys,logging,json
from datetime import datetime,timedelta
import pandas as pd
import openpyxl,time
from openpyxl import load_workbook
from functools import reduce
from create_connection import create_connectn



# def find_indx(len_sql_time,indx_list):
#     if len(indx_list)<1:
#         indx_list.insert(0,len_sql_time)
#         return indx_list[0]
#     else:
#         val=indx_list[0]+len_sql_time
#         indx_list.insert(0,val)
#         return indx_list[0]

def find_indx(len_sql_time,indx_list):
    indx_list.append(len_sql_time)    
    try:
        idx=int(reduce(lambda x, y:x+y, indx_list)) 
    except:
        idx=len_sql_time
    return idx
    


def create_formt1(list1):
    with open("E:/Company/truck_data/Viriciti/DB/sqlqry1.txt","w+") as file_:
        file_.write('(')
        for i in range(0,len(list1)):
            file_.write(list1[i])            
            if i<len(list1)-1:
                file_.write(",")
            else:
                file_.write(')')
    with open("E:/Company/truck_data/Viriciti/DB/sqlqry1.txt","r+") as file_1:
        formt_1=file_1.read()
    return formt_1


def create_formt2(list2):
    with open("E:/Company/truck_data/Viriciti/DB/sqlqry2.txt","w+") as file_:
        file_.write('(')
        for i in range(0,len(list2)):
            file_.write('%s')            
            if i<len(list2)-1:
                file_.write(",")
            else:
                file_.write(')')                
    with open("E:/Company/truck_data/Viriciti/DB/sqlqry2.txt","r+") as file_1:
        formt_2=file_1.read()
    return formt_2



def conct_DB(list1,list2):
    # print("in DB_cnct")
    # print(list1,list2)
    cnctn = mysql.connector.connect(host='tpsan1srv01',
    user='root',
    passwd='FifthRiver7#',
    db='telematics')
    cursor = cnctn.cursor()

    formt1=create_formt1(list1) 
    formt2=create_formt2(list2)
    # query="INSERT INTO `telematics`.`viriciti_refined_raw` {} VALUES {}".format(formt1,formt2)  

    query="INSERT INTO `telematics`.`viriciti_refined_raw` {} VALUES {}".format(formt1,formt2)   
    cursor.execute(query,(list2))
    cnctn.commit()
    cursor.close()
    cnctn.close()       
     
    

def write_to_refine_tabl(sql_df_time,veh_,veh_nam):
    # print(sql_df_time['time_'][0])
    dict_val={}
    key_lst,val_lst=[],[]    
    # print("got in")
    frst=0
    # for frst in range(0,1):
    dict_val['VID']=sql_df_time['vehicle_name'][0]
    dict_val['VEHICLE_NAME']=veh_nam[veh_]
    dict_val['DATE']=sql_df_time['date_'][0]
    dict_val['TIME']=sql_df_time['time_'][0]

    

    for len_ in range(0,len(sql_df_time)):
        dict_val[sql_df_time['label'][len_]]=sql_df_time['value'][len_]

    for key, val in dict_val.items():
         key_lst.append(key.upper())
         val_lst.append(val)

    key_lst=key_lst
    val_lst=val_lst
    # print(key_lst)
    # print(val_lst)
    conct_DB(key_lst,val_lst)

 


def recur_functn(veh_,sql_df,veh_df,veh_nam,curr_date,prev_date,first_timstmp,first_col,indx,indx_list,ro,strt_time):    
    # print(veh_name_list)
    # for row_ in range(indx,1):
    for row_ in range(indx,len(sql_df)):
        # try:
        if indx<len(sql_df):
            if sql_df['time_'][row_]==first_timstmp:
                # print(sql_df['time_'][row_])
                # db_qry="SELECT `vehicle_name`,`date_`,`time_`,`label`,`value` FROM `telematics`.`viriciti_raw_data_testing` WHERE `date_` LIKE %(p)s and `vehicle_name` LIKE %(p1)s and `time_` LIKE %(p2)s ORDER BY date_ ASC,time_ ASC"
                db_qry="SELECT `vehicle_name`,`date_`,`time_`,`label`,`value` FROM `telematics`.`viriciti_raw_data` WHERE `date_` LIKE %(p)s and `vehicle_name` LIKE %(p1)s and `time_` LIKE %(p2)s ORDER BY date_ ASC,time_ ASC"
                sql_df_time,veh_df_time,veh_logr_time,veh_nam,sql_df.columns=create_connectn(curr_date,prev_date,db_qry,veh_,sql_df['time_'][row_])
                # print(sql_df_time)
                # print(veh_nam,veh_df_time)
                len_sql_time=len(sql_df_time)
                # indx=len(sql_df_time)
                indx=find_indx(len_sql_time,indx_list)
                # print("----------------------------------------------------------------------------------------------------------------------")
                print("------------------------------------------------------Writing rows in DB from index: {}--------------------------------------------".format(indx))
                # print("----------------------------------------------------------------------------------------------------------------------")
                # print(sql_df_time['time_'])                  
                # write_to_csv(sql_df_time,ro,veh_,veh_nam)
                write_to_refine_tabl(sql_df_time,veh_,veh_nam)
                ro+=1
                try:   
                    first_timstmp=sql_df['time_'][indx]  
                except:
                    break 
                
                return recur_functn(veh_,sql_df,veh_df,veh_nam,curr_date,prev_date,first_timstmp,first_col,indx,indx_list,ro,strt_time)
            else:
                pass
        else:
            break
        # except:
        #     pass
    time_tkn=time.time()-strt_time
    print("RRD created ,took seconds:",time_tkn)
    # print('--------------------------------------------------------------------------------------------------------------------------------')
    # last_label_val(file_name,dir_path,prev_date)


def insert_data(veh_,sql_df,veh_df,veh_nam,curr_date,prev_date,len_veh_list,strt_time):
    
    first_timstmp=sql_df['time_'][0]
    first_col=sql_df['label'][0]
    count_col=0
    count_label=0
    ro=2
    indx_list=[]
    # res=search_vehicl(sql_df[vehicle_name],veh_,0,len(sql_df)-1)
    # if res!=-1:
    recur_data=recur_functn(veh_,sql_df,veh_df,veh_nam,curr_date,prev_date,first_timstmp,first_col,0,indx_list,ro,strt_time)


def write_RRD(prev_date,curr_date,veh_,len_veh_list):
    sys.setrecursionlimit(100000)    
    strt_time=time.time()
   
    try:
        # db_qry="SELECT `vehicle_name`,`date_`,`time_`,`label`,`value` FROM `telematics`.`viriciti_raw_data` WHERE `date_` LIKE %(p)s and `vehicle_name` LIKE %(p1)s ORDER BY date_ ASC,time_ ASC"
        db_qry="SELECT `vehicle_name`,`date_`,`time_`,`label`,`value` FROM `telematics`.`viriciti_raw_data` WHERE `date_` LIKE %(p)s and `vehicle_name` LIKE %(p1)s ORDER BY date_ ASC,time_ ASC"
        # print(veh_)
        sql_df,veh_df,veh_logr,veh_nam,sql_df.columns=create_connectn(curr_date,prev_date,db_qry,veh_,0)

        insert_data(veh_,sql_df,veh_df,veh_nam,curr_date,prev_date,len_veh_list,strt_time)
    
    except:
        try:
            LogDirectory = os.path.join("E:\\","Company","truck_data","Viriciti","JOSN_files","logs", )
            LogFilepath = os.path.join(LogDirectory ,"Socket_{}.log".format(prev_date))            
            logging.basicConfig(filename=LogFilepath,filemode='a+',level=logging.CRITICAL)       
            exc_type, exc_value, exc_traceback = sys.exc_info()      
            
            logging.critical('{}:{} raised for message: {}'.format(exc_type,exc_value,msg))

        except:
            logging.exception("Exception occurred in create_RRD script")
        
    




# -----------------------------------------------------
# try:

# write_RRD()
# except:
#     print("No data available for today!")
        
