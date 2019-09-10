import psutil
import smtplib
import sys
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email import encoders

def run_script():
    import write_toRRD
    str_="Started"
    return str_

def notify_status():
    nl='\n'
    bl='<br>'
    # recipients = ['mayuri@transpowerusa.com','wilson@transpowerusa.com']
    recipients = ['mayuri@transpowerusa.com']    
    emaillist = [elem.strip().split(',') for elem in recipients]

    msg = MIMEMultipart('mixed')  
    msg['Subject'] = "Notification:: Viriciti API Script is not running"
    msg['From'] = 'donoreply@transpowerusa.local'
    html= "Script is restarted.Check Web Socket script."
    part1=MIMEText(html,'html')
    msg.attach(part1)

    print("sending notifcation..........")
    server = smtplib.SMTP('smtp.naturalnetworks.us',25)
    server.sendmail(msg['From'], emaillist , msg.as_string())



def check_script_status():
    # collects all process and pid's running in pyhton executables in list
    pythons = [[" ".join(p.cmdline()), p.pid] for p in psutil.process_iter()
            if p.name().lower() in ["python.exe", "pythonw.exe"]]
    # print(pythons)
    py_script=[]
    flag=0
    try:
        for val in pythons:
            # print(val[0])
            # splitting on list elements to get scripts name
            val=val[0].split(".\\")[1]
            # print(val)
            # comparing each list element with required script(web-socket script) that needs to be checked 
            if val=='write_toRRD.py':
                flag=1
                # print("Script is running")
                break
            else:
                pass
                # print("Script not running")
                
        # print(flag)
    except:
        pass
    if flag==1:
        print("Script is not running")       
       
    else:
        print("Script is not running")
        # start web socket script
        stat=run_script()
        if stat=='Started':
            # send notification if web socket script failed
            notify_status()
        else:
            print("some issue observed while starting script")

check_script_status()
    


