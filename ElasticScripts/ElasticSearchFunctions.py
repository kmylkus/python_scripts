# -*- coding: utf-8 -*-
#!/usr/bin/python3.6
import datetime
import sys
import time
from CommonConfigElastic import *
from glob import glob
import csv
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine, func
import io
import numpy as np
import pymysql
from datetime import datetime, timedelta
import session

def last_month(ds):
    from datetime import datetime
    cur_ds = datetime.strptime(ds, '%Y%m%d')
    next_month = datetime(year=cur_ds.year, month=cur_ds.month, day=1)
    last_day_month = next_month - timedelta(days=1)
    d = datetime.strftime(last_day_month, '%Y%m%d')
    return d[:6]

def getUniquePN(con, indx,nested,  packageName, par ):
    apps_packagename_list = []
    response_num = con.search(request_timeout=1000,index=indx, body='''
                              {
  "size": 0,
    "aggs" : {
        "uniqueValueCount" : {
            "nested" : {
                "path" : "'''+str(nested)+'''"
            },
            "aggs" : {
                "uniqueValueCount" : {  "cardinality" : { "field" : "'''+str(packageName)+'''", "precision_threshold": 400000 } }
            }
        }
    }
}''')
    uniqueVC=response_num["aggregations"]["uniqueValueCount"]["uniqueValueCount"]["value"]
    print('Total: '+ str(uniqueVC))
    numP=round(uniqueVC/par +1)
    print('Number of partisions: '+ str(numP))
    p = 0
    while p < numP:
        response = con.search(request_timeout=1000,index=indx, body='''
        
        {
  "size": 0,
    "aggs" : {
        "id" : {
            "nested" : {
                "path" :"'''+str(nested)+'''"
            },
            "aggs" : {
                "id" : {  "terms" : { "field" :"'''+str(packageName)+'''", 
 "include": {
         "partition": "'''+str(p)+'''",
         "num_partitions": "'''+str(numP)+'''"
       },
       "size": 10000
     } } }
            }
        }
    }
}''')
        key = response["aggregations"]["id"]["id"]["buckets"]
        i = 0
        while i < len(key):
            a_pack = key[i].get("key")
            apps_packagename_list.append(a_pack)
            i=i+1
        p = p+1
        print('Loading: '+str(len(apps_packagename_list)))  
            
    return apps_packagename_list


#raports 

def updateReports(location):
    files = glob(location + '*')
    files_names = []
    #engine = create_engine('mysql://devuser:1qaz!QAZ@10.2.0.36/mid')
    for file in files:
        files_names.append(file[74:-1] + 'v')
    connection = engine.connect()
    result = connection.execute('delete from user_reports;')
    for i in files_names:
        query= ('insert into user_reports (rep_name) values (%s)')
        result = connection.execute(query,i)
    connection.close()
    return len(files)

def detectNewApps(list_of_unique_dicts):
    connection = engine.connect()
    connection24 = engine24.connect() 
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
            
            result = connection24.execute(query, (list_of_unique_dicts[i]))
            query_cat = '''select genreId from android where packagename = %s;''' 
            result_cat = connection24.execute(query_cat, (list_of_unique_dicts[i]))
            for row in result :
                app_name= row[0]
                if app_name not in ['', 'Null', None]:
                    print(app_name)
                    query = '''UPDATE apps SET app_name = %s WHERE app_packagename = %s;''' 
                    try:
                        result = connection.execute(query,(str(app_name),(list_of_unique_dicts[i])))
                    except:
                        print ('Incorrect string value for app_name...')
            for row in result_cat :
                n= row[0]  
                if n is not None:
                    query = '''UPDATE apps SET app_cat_id_gp1 = (SELECT cat_id_gp from category_gp where cat_code_gp=%s ) WHERE app_packagename = %s;''' 
                    try:
                        result = connection.execute(query,(str(n),list_of_unique_dicts[i]))
                    except:
                        print ('Incorrect value for category...')
            k = k+1
    
            print ('Inserting new app_packagename: ' + list_of_unique_dicts[i])
            
        i=i+1
    if k ==0:
        print ('Not founded new packagename ')
    connection.close
    connection24.close
    return k

def updateProfilesBool(test_i=False):
    if test_i==True:
        engine = create_engine('mysql://devuser:1qaz!QAZ@10.2.0.36/mid',pool_size=10, max_overflow=20)
    else:
        engine = create_engine('mysql://zasiegator:M6vQJndUzmPVpP6@10.2.0.22/mid2',pool_size=10, max_overflow=20)

    connection = engine.connect()
    result = connection.execute('''select usr_id from user;''')
    data = result.fetchall()
    i = 0
    global cid, aid,csmfid
    cid = 0
    aid = 0
    csmfid =0
    print ("len of data:")
    print (len(data))
    while i < len(data):
        id = dict(data[i])["usr_id"]
        res_cat = connection.execute('select def_prof_id from profiles_def where def_cat_id_gp in (select app_cat_id_gp1 from apps where app_id in (select ah_app_id from apps_history where ah_usr_id =' + str(id) + '));')
        res_app = connection.execute('select def_prof_id from profiles_def where def_app_id in (select ah_app_id from apps_history where ah_usr_id =' + str(id) + ');')
        res_cat_smf = connection.execute('select def_prof_id from profiles_def where def_cat_id_smf in (select app_cat_smf from apps where app_id in (select ah_app_id from apps_history where ah_usr_id =' + str(id) + '));')
        test=res_cat.fetchall()
        test2=res_app.fetchall()
        dane3=res_cat_smf.fetchall()
        global m,k,s
        m = 0
        s1 = session.MySQLSession()
        if test_i==True:        s1.get(host='devuser')
        elif test_i==False:        s1.get(host='mid2')
        #date_i = datetime.strptime(dates[0],'%Y-%m-%d').date()
        date_i = datetime.today().date()
        s1.D = s1.Base.classes.date
        date_id = s1.session.query(s1.D.date_id).filter(s1.D.date==date_i).first()
        if date_id is None:
            max_date = s1.session.query(func.max(s1.D.date)).one()[0]
            delta = date_i - max_date        # timedelta
            for i in range(1,delta.days + 1):
                    new_date_date = max_date + timedelta(i)
                    new_date = s1.D(date=new_date_date)
                    s1.session.add(new_date)
            s1.session.flush()
            s1.session.commit()
            date_id = s1.session.query(s1.D.date_id).filter(s1.D.date==date_i).first()
        date_id = date_id[0]
     
        while m<len(test) and len(test)!=0:
            
            def_id = dict(test[m])["def_prof_id"]
            res_app = connection.execute('INSERT IGNORE INTO  user_profile (up_usr_id,up_prof_id_%d) VALUES (%d, %d) ;' %(def_id, id, date_id))
            app = connection.execute('UPDATE user_profile SET up_prof_id_%d = %d WHERE up_usr_id = %d' %(def_id, date_id, id))
            m=m+1
            cid = cid +1
        k = 0
        while len(test2)!=0 and k<len(test2):
            def_id = dict(test2[k])["def_prof_id"]
            res_app = connection.execute('INSERT IGNORE INTO  user_profile (up_usr_id,up_prof_id_%d) VALUES (%d,%d) ' %(def_id, id, date_id))
            app = connection.execute('Update user_profile set up_prof_id_%d = %d where up_usr_id = %d' %(def_id, date_id, id))
            k=k+1
            aid = aid+1
        s = 0
        while s<len(dane3) and len(dane3)!=0:
            def_id = dict(dane3[s])["def_prof_id"]
            res_app = connection.execute('INSERT IGNORE INTO  user_profile (up_usr_id, up_prof_id_%d) VALUES (%d, %d) ;' %(def_id, id, date_id))
            app = connection.execute('Update user_profile set up_prof_id_%d = %d where up_usr_id = %d' %(def_id, date_id, id))
            s=s+1
            csmfid = csmfid +1
        i =i+1
        print('i = '+ str(i)+ 'from ' + str(len(data)))
    return cid, aid,csmfid

def getUnixTimeFJSon(list):
    import datetime
    i = 0
    dane = []
    while i < len(list):
        k = 0
        while k < len(list[i]['unique_set_2']['ts']['buckets']):
            t = 0
            while t < len(list[i]['unique_set_2']['ts']['buckets'][k]['unique_set_3']['name']['buckets']):
                x =list[i]['unique_set_2']['ts']['buckets'][k]['key']
                try:
                    timestamp = datetime.datetime.fromtimestamp(x/1000)
                    dane.append((timestamp.strftime('%Y-%m-%d %H:%M:%S'),list[i]['unique_set_2']['ts']['buckets'][k]['unique_set_3']['name']['buckets'][t]['key'],list[i]['key']))
                except:
                    g = 0
                    #print('Found negative unix time:..')
                t = t+1
            k = k+1
        i=i+1
    print("Len of data for csv " + str(len(dane)))
    my_df = pd.DataFrame(dane)
    my_df.to_csv('androidID-CPU_PackageName-ts.csv', index=False, header=['a','b','c'])
#50000
def getAllUsers_Apps():
    connection = engine.connect()
    connection2 = engine.connect()
    query = 'select app_id,app_packagename  from apps;'
    result = connection.execute(query)
    apps = []
    for r in result:
         apps.append([r[0],r[1]])
    my_df = pd.DataFrame(apps)
    my_df.to_csv('all_apps.csv', index=False, header=['app_id','app_pname'])

    query = 'select usr_id,usr_android_id from user;'
    result = connection.execute(query)
    users = []
    for r in result:
         users.append([r[0],r[1]])
    my_df = pd.DataFrame(users)
    my_df.to_csv('all_users.csv', index=False, header=['usr_id','android_id'])

def getUniqueUsers(con, indx, packageName, par):
    apps_packagename_list = []
    response_num = con.search(request_timeout=1000,index=indx, body='''
    
    {
"size": 0,
  "aggs" : {
      "uniqueValueCount" : {
          "cardinality" : {
                   "field" : "user.androidID",
       "precision_threshold": 400000
          }
  }
}}''')
    uniqueVC=response_num["aggregations"]["uniqueValueCount"]["value"]
    print('Total: '+ str(uniqueVC))
    numP=round(uniqueVC/ par +1)
    print('Number of partisions: '+ str(numP))
    p = 0
    while p < numP:
        response = con.search(request_timeout=10000,index=indx, body='''
        
        {
"size": 0,
  "aggs" : {
      "unique_set_1" : {
          "terms" : {
                   "field" : "user.androidID",  "include": {
         "partition": "'''+str(p)+'''",
         "num_partitions": "'''+str(numP)+'''"
       },
       "size": 100000
          },
          "aggregations" : {
              "unique_set_2": {
                  "terms": {"field": "user.advertisingID"}
              }
          }
      }
  }
}''')  
        #print(response["aggregations"]["unique_set_1"]["buckets"])
        key = response["aggregations"]["unique_set_1"]["buckets"]
        key_adv_id = response["aggregations"]["unique_set_1"]["buckets"]
        i = 0
        
        while i < len(key):
            key_adv_id = response["aggregations"]["unique_set_1"]["buckets"][i]["unique_set_2"]["buckets"]
            k = 0
            while k < len(key_adv_id):
                android_id = key[i].get("key")
                adv_id =  key_adv_id[k].get("key")
                apps_packagename_list.append([android_id, adv_id])
                k = k+1
            i=i+1
        p = p+1
        print('Loading: '+str(len(apps_packagename_list)))  
    return apps_packagename_list

def getAllUsers():
    connection = engine.connect()
    query = 'select usr_android_id from user;'
    result = connection.execute(query)
    users = []
    for r in result:
         users.append(r[0])
    return users

def insert_all_user_history():
    conn = pymysql.connect(host='10.2.0.22', user='zasiegator', passwd='M6vQJndUzmPVpP6', port=3306,
                           charset='utf8', connect_timeout=10000, db='mid2', local_infile=1)
    try:
        with conn.cursor() as cursor:
  
            sql = '''LOAD  DATA LOCAL INFILE '/opt/midworkspace/mid/scripts/elasticData/new_users.csv'
            
IGNORE INTO TABLE user 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(usr_android_id, usr_advertising_id)'''
            cursor.execute(sql)
        conn.commit()
    finally:
        conn.close()

def detectUsers(con, inx, par):
    usersDB = getAllUsers()
    print('Getting androidID from: "user.androidID"....')
    usersES= getUniqueUsers(con, inx, 'user.androidID',par)
    df_db = pd.DataFrame(usersDB, columns = ['android_id'])
    df_es = pd.DataFrame(usersES, columns = ['android_id', 'adv_id'])
    db =  pd.Index(df_db['android_id'])
    es = pd.Index(df_es['android_id'])
    diff = es.difference(db)
    df = pd.merge( df_es, pd.DataFrame(diff, columns = ['android_id']),  left_on='android_id', right_on='android_id', how = 'inner')
    df.to_csv('new_users.csv', index=False)
    insert_all_user_history()
    return len(df)
        
def insert_all_apps_history():
    conn = pymysql.connect(host='10.2.0.22', user='zasiegator', passwd='M6vQJndUzmPVpP6', port=3306,
                           charset='utf8', connect_timeout=10, db='mid2', local_infile=1)
    try:
        with conn.cursor() as cursor:
  
            sql = '''LOAD  DATA LOCAL INFILE '/opt/midworkspace/mid/scripts/elasticData/all_apps_history.csv'
IGNORE INTO TABLE apps_history
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ah_date, ah_app_id, ah_usr_id)'''
            cursor.execute(sql)

        conn.commit()
    finally:
        conn.close()
#par = 30000
def getUniquePNappsHistory(con, indx, par):
    android_list = []
    response_num = con.search(index=indx,request_timeout=10000, body='''
{
    "query": {
    "bool": {
      "must": [
        {
          "nested": {
            "path": "hits.cpuProcess.processRunning",
            "query": {
              "exists": {
                "field": "hits.cpuProcess.processRunning.name"
              }
            }
          }
        }
      ]
    }},
  "aggs": {
        "uniqueValueCount": {
          "cardinality": {
            "field": "user.androidID",
            "precision_threshold": 400000
          }
        }
      }
}''')
    uniqueVC=response_num["aggregations"]["uniqueValueCount"]["value"]
    print('Total: '+ str(uniqueVC))
    numP=round(uniqueVC/ par +1)
    print('Number of partisions: '+ str(numP))
    p = 0
    while p < numP:
        response = con.search(index=indx,request_timeout=1000000, body='''
        {"aggs": {"unique_set_1":
{"aggs": {"unique_set_2": {"aggs": {"ts": {"aggs": {"unique_set_3": {"aggs":{"name": {"terms": 
{"field": "hits.cpuProcess.processRunning.name", "size": 10000}}},
        "nested": {"path": "hits.cpuProcess.processRunning"}}},

      "terms": {"field": "hits.ts", "size": 10000}}},

    "nested": {"path": "hits"}}},

  "terms": {"field": "user.androidID", "include": {
          "partition": "'''+str(p)+'''",
         "num_partitions": "'''+str(numP)+'''"
       },
       "size": 10000 }}},
       
       "query": {
    "bool": {
      "must": [
        {
          "nested": {
            "path": "hits.cpuProcess.processRunning",
            "query": {
              "exists": {
                "field": "hits.cpuProcess.processRunning.name"
              }
            }
          }
        }
      ]
    }
  }}
''')
        key = response['aggregations']['unique_set_1']['buckets']
        i = 0
        while i < len(key):
            a_pack = key[i]
            android_list.append(a_pack)
            i=i+1
        p = p+1
        print('Loading: '+str(len(android_list)))  
    return android_list
getAllUsers_Apps()  
def insertHistory(d,par):
    data = 'sdk-regulartime-' + str(d)
    list= getUniquePNappsHistory(es2, data, par)
    getUnixTimeFJSon(list)
    df = pd.read_csv('androidID-CPU_PackageName-ts.csv')
    app = pd.read_csv('all_apps.csv')
    user = pd.read_csv('all_users.csv')
    dfapp = df.merge(app, left_on='b', right_on='app_pname', how='inner')
    dfappuser =dfapp.merge(user, left_on='c', right_on='android_id', how='inner')
    finally_df = dfappuser.drop(columns=['b', 'c','app_pname', 'android_id'])
    finally_df.to_csv('all_apps_history.csv', index=False)
    insert_all_apps_history()
    return str(len(finally_df))

