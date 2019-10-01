import datetime
import sys
import time
from CommonConfigElastic import *

t = time.time()
if len(sys.argv)==2:
    a = sys.argv[1]
    data = 'sdk-' + str(a)
else:
    tod = datetime.datetime.now()
    d = datetime.timedelta(days = 2)
    a = (tod - d).strftime('%Y%m%d')
    data = 'sdk-' + str(a)
    #print(data)
print('')
print('Starting searching unique packagenames with index', data)
print('****************************************')
def getUniquePN(con, indx, packageName):
    apps_packagename_list = []
    response_num = con.search(request_timeout=1000,index=indx, body='''
{
"size": '''+str(1)+''',
 "aggs": {
   "uniqueValueCount": {
     "cardinality": {
       "field": "'''+str(packageName)+'''",
       "precision_threshold": 400000
     }
   }}}''')
    uniqueVC=response_num["aggregations"]["uniqueValueCount"]["value"]
    print('Total: '+ str(uniqueVC))
    numP=round(uniqueVC/ 8000 +1)
    print('Number of partisions: '+ str(numP))
    p = 0
    while p < numP:
        response = con.search(request_timeout=1000,index=indx, body='''
{
"size": 0,
 "aggs": {
   "uniqueValueCount": {
     "cardinality": {
       "field": "'''+str(packageName)+'''",
       "precision_threshold": 400000
     }
   },
   "id": {
     "terms": {
       "field": "'''+str(packageName)+'''",
       "include": {
         "partition": "'''+str(p)+'''",
         "num_partitions": "'''+str(numP)+'''"
       },
       "size": 10000
     }
   }
 }
}''')
        key = response["aggregations"]["id"]["buckets"]
        i = 0
        while i < len(key):
            a_pack = key[i].get("key")
            apps_packagename_list.append(a_pack)
            i=i+1
        p = p+1
        print('Loading: '+str(len(apps_packagename_list)))  
            
    return apps_packagename_list
print('')
print('Getting packageNames from: hits.networkUsage.apps.packageName.....')
networkUsage_packageName= getUniquePN(es, data, 'hits.networkUsage.apps.packageName')
print('')
print('Getting packageNames from: hits.cpuProcess.processRunning.name.....')
processRunning_name =getUniquePN(es, data,'hits.cpuProcess.processRunning.name')
print('')
print('Getting packageNames from: hits.appsList.packageName.....')
appsListP_name =getUniquePN(es, data,'hits.appsList.packageName')

i = 0
k = 0
while i < len(processRunning_name):
    networkUsage_packageName.append(processRunning_name[i])
    i= i+1
while k < len(appsListP_name):
    networkUsage_packageName.append(appsListP_name[i])
    k = k+1
list_of_unique_dicts=list(np.unique(np.array(networkUsage_packageName)))#76112
print('****************************************')
print('Lenth of all names: '+str(len(networkUsage_packageName)) )  
print('Lenth of unique names: '+str(len(list_of_unique_dicts)) ) 

connection = engine.connect()

result = connection.execute('select app_packagename from apps')
pack_name = []
r = result.fetchall()
for i in r:
    pack_name.append(i.app_packagename)

i = 0
k = 0
while i < len(list_of_unique_dicts):
    if list_of_unique_dicts[i] not in pack_name:
        query = '''INSERT INTO apps (app_packagename, app_name) VALUES (%s,'NaN' );''' 
        result = connection.execute(query,(list_of_unique_dicts[i]))
        query = '''select title from android where packagename = %s;'''
        connection24 = engine24.connect() 
        result = connection24.execute(query, (list_of_unique_dicts[i]))
        for row in result :
            app_name= row[0]
        if app_name not in ['', 'Null', None]:
            print(app_name)
            query = '''UPDATE apps SET app_name = %s WHERE app_packagename = %s;''' 
            try:
                result = connection.execute(query,(str(app_name),(list_of_unique_dicts[i])))
            except:
                print ('Incorrect string value...')
        connection24.close

  
        k = k +1
        print ('Inserting new app_packagename: ' + list_of_unique_dicts[i])
    i=i+1
if k ==0:
    print ('Not founded new packagename ')
connection.close
print('****************************************')
print('Program time exucuting: ', time.time() -t)

