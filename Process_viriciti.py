import mysql.connector
import os,sys,logging,json
import pandas as pd
import openpyxl
from openpyxl import load_workbook
from list_col_viri import viri_label_list
from datetime import datetime,timedelta
from list_col_viri import viri_label_list





curr_date=datetime.now().strftime('%m-%d-%Y')
prev_date= (datetime.now() + timedelta(days=-6)).strftime('%Y-%m-%d') 

db_qry="SELECT `vehicle_name`,`date_`,`time_`,`label`,`value` FROM `telematics`.`viriciti_raw_data_testing` WHERE `date_` LIKE %(p)s"
cnctn = mysql.connector.connect(host='tpsan1srv01',
        user='root',
        passwd='FifthRiver7#',
        db='telematics')
cursor = cnctn.cursor() 

cursor.execute(db_qry,{"p": "%{}".format(prev_date)})


# Put it all to a data frame
sql_df_veh = pd.DataFrame(cursor.fetchall())  
# print(sql_df) 
sql_df_veh.columns = cursor.column_names 

cnctn.commit()  
cursor.close()
cnctn.close()  