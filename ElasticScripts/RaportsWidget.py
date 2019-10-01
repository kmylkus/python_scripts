# -*- coding: utf-8 -*-
#!/usr/bin/python3.6
import datetime
import sys
import time
from CommonConfigElastic import *
from glob import glob

t = time.time()

#raports 
location = '/opt/midworkspace/Coverage/mysite/scripts/countingUsersAllAppWithConnectedSDK/results/'
files = glob(location + '*')
files_names = []
    #engine = create_engine('mysql://devuser:1qaz!QAZ@10.2.0.36/mid')
for file in files:
    files_names.append(file[86:-1] + 'v')
connection = engine.connect()
result = connection.execute('delete from user_reports;')
for i in files_names:
    query= ('insert into user_reports (rep_name) values (%s)')
    result = connection.execute(query,i)
connection.close()
print('****************************************')
print('Program time exucuting: ', time.time() -t)

