import json
import os
import shutil

# Reading json file
with open("C:/Users/mayuri/json/my_js.json","r")as f:
    f1=json.load(f)
mov=os.listdir(f1['3646']['SRC'])
#for moving files from source folder to destination folder
count=len(mov)
for i in mov:
    if(count!=0):
        shutil.move(f1['3646']['SRC']+'\\'+i,f1['3646']['DST'])
    
