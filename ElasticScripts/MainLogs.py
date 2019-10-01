# -*- coding: utf-8 -*-
#!/usr/bin/python3.6
import datetime
import sys
import time
import CommonConfigElastic
from CommonConfigElastic import *
from glob import glob
from ElasticSearchFunctions import *
import get_keywords_dag_elastic
import update_sp1_ukwds_from_zasiegator
import pandas as pd
from elasticsearch_session import ElasticSession
#usage: python3.6 Main.py 20190204
import datetime



tasks = CommonConfigElastic.tasks
tod = datetime.datetime.now().strftime('%Y-%m-%d')
file_name = 'info_' + str(tod) + '.log'
logfile = open(file_name, 'w')    
d = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
logfile.write('{"date": "'+ str(d)+'","scripts": [ ')

# uses Elasticsearch
def main_uses_elasticsearch(tasks=[], es2=None, data=None, yesterday=None):
 if "detectNewApps" in tasks:
  logfile.write('{"name": "detectNewApps",')  
  try:
     t1 = time.time()
     print('Starting searching unique packagenames with index', data)
     print('****************************************')
     print('')
     print('Getting packageNames from: hits.networkUsage.apps.packageName.....')
     networkUsage_packageName= getUniquePN(es2, data, 'hits.networkUsage.apps','hits.networkUsage.apps.packageName',es_part_size)
     print('')
     print('Getting packageNames from: hits.cpuProcess.processRunning.name.....')
     processRunning_name =getUniquePN(es2, data,'hits.cpuProcess.processRunning', 'hits.cpuProcess.processRunning.name',es_part_size)
     print('')
     print('Getting packageNames from: hits.appsList.packageName.....')
     appsListP_name =getUniquePN(es2, data,'hits.appsList', 'hits.appsList.packageName',es_part_size)
 
     i = 0
     k = 0
     while i < len(processRunning_name):
         networkUsage_packageName.append(processRunning_name[i])
         i= i+1
     while k < len(appsListP_name):
         networkUsage_packageName.append(appsListP_name[k])
         k = k+1
     list_of_unique_dicts=list(np.unique(np.array(networkUsage_packageName)))#76112
     print('****************************************')
     print('Lenth of all names: '+str(len(networkUsage_packageName)) )  
     print('Lenth of unique names: '+str(len(list_of_unique_dicts)) ) 
 
     l = detectNewApps(list_of_unique_dicts)
     n_usr = detectUsers(es2, data,es_part_size)
     dur = time.time() -t1
     logfile.write('"details": {"status": true, "info": "Founded '+str(l)+' apps and '+str(n_usr)+' users", "time": "'+str(dur)+'"}},')
     print('Successfully finished detecting new users, apps with app_name and category_id.') 
 
  except:
     logfile.write('"details": {"status": false, "info": "Founded 0 apps and 0 users ", "time": "0"}},')
 
 #uses elasticsearch
 if "get_keywords_dag_elastic" in tasks: 
  logfile.write('{"name": "get_keywords_dag_elastic",')
  try:
     t1 = time.time()
     if CommonConfigElastic.test:
         db = 'devuser'
     else:
         db = 'mid2'
     logs = get_keywords_dag_elastic.main(yesterday=yesterday, geo_only=True, db=db,
                     db_df=CommonConfigElastic.db_df,
                     endpoint=CommonConfigElastic.es2_endpoint)
     dur = time.time() -t1
     #logs = '"mock loggs"'
     logfile.write('"details": {"status": true, "info": '+logs+', "time": "'+str(dur)+'"}},')
  except:
     logfile.write('"details": {"status": false, "info": "Something went wrong with get_keywords_dag_elastic method", "time": "0"}},')


import datetime
t = time.time()
if len(sys.argv)==2:
    a = sys.argv[1]
    datetime_object = datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
    yesterday = datetime.datetime.strftime(datetime_object, '%Y-%m-%d')
    #added argument
    data = 'sdk-regulartime-' + str(a)
else:
    tod = datetime.datetime.now()
    d = datetime.timedelta(days = 2)
    a = (tod - d).strftime('%Y%m%d')
    data = 'sdk-regulartime-' + str(a)
    yesterday = datetime.datetime.strftime(
    datetime.datetime.now() - datetime.timedelta(2), '%Y-%m-%d')
    df = pd.read_csv('overdue_indices.csv')
    indices_to_calculate = set(list(df['overdue_index']) + [data])
    ess = ElasticSession()
    ess.get()
    indices_in = set(list(ess.client.indices.get_alias().keys()))
    indices_in_to_calculate = indices_to_calculate&indices_in
    overdue_indices = indices_to_calculate - indices_in_to_calculate
    df = pd.DataFrame(list(overdue_indices), columns=['overdue_index'])
    df.to_csv('overdue_indices.csv', index=False)
    for index in indices_in_to_calculate:
       datetime_object = datetime.datetime.strptime(index.split('-')[-1], '%Y%m%d')
       yesterday = datetime.datetime.strftime(datetime_object, '%Y-%m-%d')
       main_uses_elasticsearch(tasks=tasks, es2=es2, data=index, yesterday=yesterday)
    
#logfile = open('info.log', 'w')

from subprocess import call
logfile.write('{"name": "generateReport_main",')

# does not use Elasticsearch
if "countingUsersAllAppWithConnectedSDK" in tasks:
 try:
    t1 = time.time()
    call(["python3", '/opt/midworkspace/mid/scripts/countingUsersAllAppWithConnectedSDK/main.py'])
    dur = time.time() -t1
    logfile.write('"details": {"status": true, "info": "Generated new raport", "time": "'+str(dur)+'"}},')
    print('Successfully finished detecting new apps with app_name and category_id.') 
 except:
    logfile.write('"details": {"status": false, "info": "Raport was not generated", "time": "0"}},')


# does not use Elasticsearch
if "updateReports" in tasks:
    logfile.write('{"name": "updateReports",')
    try:
       t1 = time.time()
       l = updateReports(location)
       print('Successfully updating reports in database.')
       dur = time.time() -t1
       logfile.write('"details": {"status": true, "info": "Updated '+str(l)+' reports", "time": "'+str(dur)+'"}},')
    except:
       logfile.write('"details": {"status": false, "info": "Something went wrong with updateReports method", "time": "0"}},')
    
    logfile.write('{"name": "updateProfilesBool",')

logfile.flush()

#does not use Elasticsearch
if "updateProfilesBool" in tasks:
    try:
       t1 = time.time()
       if (a.endswith('01') or CommonConfigElastic.updateProfilesBool == True):
           print('Starting updating profiles .........')
           l = updateProfilesBool(test_i=CommonConfigElastic.test)
           print('Successfully updating profiles in database.')
           dur = time.time() -t1
           logfile.write('"details": {"status": true, "info": "Updated '+str(l[0])+' profiles by def_cat_id_gp, '+str(l[1])+' profiles by def_app_id, '+str(l[2])+' profiles by def_cat_id_smf", "time": "'+str(dur)+'"}},')
       else:
           dur = time.time() -t1
           logfile.write('"details": {"status": true, "info": "Data in the table profiles_def will be updated  at the beginning of the month "}},')
    except ValueError:
       logfile.write('"details": {"status": false, "info": "Something went wrong with updateProfilesBool method", "time": "0"}},')
logfile.flush()

# does not use elasticsearch but little point of doing it if users not changed
# on the other hand running it once is enough
if "update_sp1_ukwds_from_zasiegator" in tasks:
 logfile.write('{"name": "update_sp1_ukwds_from_zasiegator",')
 try:
    t1 = time.time()
    logs = update_sp1_ukwds_from_zasiegator.main_main(test=CommonConfigElastic.test,
                db_df=CommonConfigElastic.db_df)
    dur = time.time() -t1
    logfile.write('"details": {"status": true, "info": '+logs+', "time": "'+str(dur)+'"}},')
 except:
    logfile.write('"details": {"status": false, "info": "Something went wrong with update_sp1_ukwds_from_zasiegator method", "time": "0"}},')

#does not use elasticsearch
if "insertApps_history" in tasks:
 logfile.write('{"name": "insertApps_history",')
 try:
    t1 = time.time()
    indx = a
    l = insertHistory(indx, 3000)
    print('Successfully updating apps_history in database.')
    dur = time.time() -t1
    logfile.write('"details": {"status": true, "info": "'+str(l)+' new records", "time": "'+str(dur)+'"}}')
 except:
    logfile.write('"details": {"status": false, "info": "Something went wrong with insertHistory method", "time": "0"}}')



logfile.write('],')
logfile.close()
file = open(file_name).read()
if 'false' in file :
    logfile = open(file_name, 'a')  
    logfile.write('"status": "false",')
else :
    logfile = open(file_name, 'a')  
    logfile.write('"status": "true",')
print('****************************************')
dur = time.time() -t
print('Program time exucuting: ', dur)
logfile.write('"time": "' + str(dur) + '"')
logfile.write('}')
logfile.close()
