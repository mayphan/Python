import datetime
import os

time_stmp=datetime.datetime.now().strftime('%m-%d-%Y-%H-%M-%S')
f_nam = datetime.datetime.now().strftime('%m-%d-%Y')
print((time_stmp))
os.system("touch 3464_log"+f_nam)

