import time,tzlocal
import os,sys,time,logging
import mysql.connector
from datetime import datetime,timedelta,date
import logging
import websocket,json,tzlocal
from list_col_viri import viri_label_list
# from upload_to_DB import upload_raw_data

def get_vehicle_name(msg_):
    veh_name=msg_["vid"]
    # print(veh_name)   
    return veh_name

def time_convrt(msg_):     
    time_stmp=(msg_["time"]/1000)
    timezon=tzlocal.get_localzone()
    local_time = str(datetime.fromtimestamp(time_stmp, timezon)).split(" ")[0]  
    print(local_time)
    return local_time


def write_txt(msg_,fpath):    
    print(msg_)
    with open(fpath,'a+') as file_:     
        file_.write(str(msg_))
        file_.write(';')
        file_.write('\n')
  

def write_in_file(msg_,curr_date):
        
    # getting vehicle name at viriciti
    veh_name=get_vehicle_name(msg_)
    
    # path to file where data is written
    ext='txt'    
    file_name1='{}_viriciti_data.{}'.format(curr_date,ext)    
    file_path=os.path.join("\\\\transpowerusa.local\\","DFSRoot","Company","truck_data","Viriciti","JSON_files",veh_name,file_name1)     

    # unix to local date conversion
    date_=time_convrt(msg_) 
    print(curr_date,date_) 
   
    # check if date matches to today's date for received live data
    if curr_date!=date_:

        file_name2='received_date_{}_on_{}.{}'.format(date_,curr_date,ext)
        flagged_path=os.path.join("\\\\transpowerusa.local\\","DFSRoot","Company","truck_data","Viriciti","JSON_files",veh_name,"Flagged",file_name2) 
        # print(flagged_path)
        write_txt(msg_,flagged_path)
        
    else:  
        
        print(curr_date,date_)
        write_txt(msg_,file_path)  



def on_message(ws, msg): 
    try:       
        curr_date=date.today()
        # print(curr_date)
        PARAM={"transpower_001": viri_label_list,"transpower_007": viri_label_list,"transpower_009": viri_label_list,"transpower_010": viri_label_list,}    
        # PARAM={"transpower_001": ["proprietary_transpower.accelpedalfault"]}
        ws.send(json.dumps(PARAM))   
        if msg=='{"ready":true}':
            print("date:",curr_date)
            print("Message received from server:",msg)           
           
        else:
            msg_=json.loads(msg)     
            write_in_file(msg_,curr_date)
            # upload_raw_data(msg,curr_date)
    
    except:
        try:
            LogDirectory = os.path.join("\\\\transpowerusa.local\\","DFSRoot","Company","truck_data","Viriciti","JSON_files","logs", )
            LogFilepath = os.path.join(LogDirectory ,"Socket_{}.log".format(curr_date))            
            logging.basicConfig(filename=LogFilepath,filemode='a+',level=logging.CRITICAL)       
            exc_type, exc_value, exc_traceback = sys.exc_info()    
            logging.critical('{}:{} raised for message: {};'.format(exc_type,exc_value,msg))

        except:
            logging.exception("Exception occurred in connect_websocket script")

def on_error(ws, error):
    print(error)
        
def on_close(ws):
    print("Closing websocket connection...")
    print(ws.status_code)

def on_open(ws):
    print("Opening websocket connection...")  



if __name__ == '__main__':
    try:
       
        # API key
        headerValue={"x-api-key" : 'f55821da-5924-48ea-9248-8ee2986126e3'} 
        count=0
            
        # creting websocket connection
        ws = websocket.WebSocketApp("wss://sdk.viriciti.com/api/v1/live/ws",
            header=headerValue,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close)
        ws.on_open = on_open
                    
                    
        while True:
            try:
                ws.run_forever()
                result=ws.rcv()
            # print(result)
            except:
                pass
    except:
        print("some error")
            

   



# # ----------------------------------------------
# not used# initial tests


# if msg=='{"ready":true}':
#             print("date:",curr_date)
#             print("Message received from server:",msg)           
            # msg_='{"time":1566411763290,"value":42,"label":"proprietary_transpower.accelpedalposition","vid":"transpower_001"}'
            # msg_=json.loads(msg_)
            # # veh_name=get_vehicle_name(msg)   
            # write_in_file(msg_,curr_date)
            # print(veh_name) 

# ws.run_forever()
                # cnctn = mysql.connector.connect(host='tpsan1srv01',
                #     user='root',
                #     passwd='FifthRiver7#',
                #     db='telematics')
                # cursor = cnctn.cursor()
    #     # time.sleep(1)
    #     # except:
    #     #     try:
    #     #         LogDirectory = os.path.join("C:/Users/mayuri/Documents/Viriciti/logs_viri")
    #     #         LogFilepath = os.path.join(LogDirectory ,"{}.log".format("status_code_error"))    
    #     #         # logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)        
    #     #         logging.basicConfig(filename=LogFilepath,filemode='w+',level=logging.DEBUG)       
    #     #         exc_type, exc_value, exc_traceback = sys.exc_info()    
    #     #         logging.debug('{}:{}'.format(exc_type,exc_value))
    #     #     except:
    #     #         print("exception")
    # else:
    #     print("Server did not sent ready message")
    # ----------------------------------
# sio = socketio.Client()

# @sio.event
# def connect():
#     print('connection established')

# # @sio.event
# # def my_message(data):
# #     print('message received with ', data)
# #     sio.emit('my response', {'response': 'my response'})

# @sio.event
# def disconnect():
#     print('disconnected from server')

# Header={'key':'02ff7911-2737-46b3-896e-3b03d04693a6'}

# sio.connect('wss://sdk.viriciti.com/api/v1/live/io/',headers=Header)
# sio.wait()

# from websocket import create_connection
# ws = create_connection("ws://localhost:8080/websocket")
# print "Sending 'Hello, World'..."
# ws.send("Hello, World")
# print "Sent"
# print "Reeiving..."
# result =  ws.recv()
# print "Received '%s'" % result
# ws.close()
# -------------------------------------------------------------