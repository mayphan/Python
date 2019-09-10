import re,os,sys


def write_match_in_file(list_,json_msg):  
    try:  
        with open(json_msg[0],"a+") as file_:
            count=0        
            for val in list_: 
                file_.write('\n')           
                file_.write(val)
                file_.write(';')
        print("message written")
    except:
        print("File to write is not observed")

    

def msg_in_log(path1,path2,path_3,date_):
    # ,"value":1,"label":"proprietary_transpower.mincellvoltstringid","vid":"transpower_001"}
    pattrn=re.compile(r'\{"time":\w*,"value":\w*,"label":"proprietary_transpower\.\w*","vid":"\w*"\}')
    # formt="Socket_{}".format(date_)
    formt="Socket_{}".format(date_)
    dir_list=[os.path.join(path1, filename) for filename in os.listdir(path1) if date_ in filename and formt in filename ]
    json_msg_file=[os.path.join(path2, filename) for filename in os.listdir(path2) if date_ in filename]
    # print(dir_list,json_msg_file)
    if len(dir_list) <1 :
        print("-----------------------------------Socket Log file not present on date: {}---------------------------------------\n ".format(date_))
    else:
        # try:
        
        for fpath_ in dir_list:
            # print(fpath_)
            with open(fpath_) as fpath:
                msg_file= fpath.read()
                # print(msg_file)
                match=re.findall(pattrn,msg_file)
                # print(match)
                if len(match)>0:
                    print(json_msg_file)
                    # write_match_in_file(match,json_msg_file)  
                else:
                    # No expected message like {"time":1566411763290,"value":42,"label":"proprietary_transpower.accelpedalposition","vid":"transpower_001"}
                    print("Only error or other messages present in log file")                   
                    

        # except:
        #     print("Log file not present")


path_1=os.path.join("S:\\","truck_data","Viriciti","JOSN_files",'transpower_001',"logs")
path_2=os.path.join("S:\\","truck_data","Viriciti","JOSN_files",'transpower_001')
path_3=os.path.join("S:\\","truck_data","Viriciti","JOSN_files","logs")
date_='2019-09-03'
msg_in_log(path_1,path_2,path_3,date_)