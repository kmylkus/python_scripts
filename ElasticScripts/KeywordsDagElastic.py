# -*- coding: utf-8 -*-
from __future__ import print_function
from builtins import range
#from airflow.operators import PythonOperator
#from airflow.models import DAG
from datetime import datetime, timedelta
import os, glob, json, sys, argparse, pymysql, csv, ujson
# pymongo,
import pandas as pd
#from pymongo import MongoClient
import sqlalchemy as sqla
from sqlalchemy import func, create_engine, or_, and_, MetaData, desc, distinct
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from tqdm import tqdm
import session
import elasticsearch_session as es
from datetime import date, timedelta



one_day_ago = datetime.combine(
        datetime.today() - timedelta(hours=24), datetime.min.time())
args = {'owner': 'soutys', 'start_date': one_day_ago}



'''
dag = DAG(
    dag_id='get_keywords_dag', default_args=args,
    schedule_interval= '30 09 * * *'
    )
'''


def food_app_in(line,food_apps):
    for food_app in food_apps:
        if food_app in line:
            return True
    return False


def getUser(dict):
    tup = ()
    info_keys = ['androidID', 'advertisingID', 'partnerID', 'appID', 'rand']
    for key in info_keys:
        tup = tup+(dict.get(key),)
    return tup


def getJson(x):
    try:
#        print (json.loads(x)) 
        dict = json.loads(x)['user']
        return dict 
    except ValueError: return {}


def date_users(date):
    for infile in glob.glob('/media/data/spicymobile3/data/calc/days/sdk/'+date+'/*json*.bz2'):
        sdk_rdd = sc.textFile(infile).filter(lambda x: food_app_in(x,food_apps))
        sdk_json = sdk_rdd.map(lambda x: getJson(x))
        # to teraz moglibysmy ogarnac to jakos po dniach i potem unikowac po dniach i wrzucac tego dfa do processed
        user_pipeline = sdk_json.map(lambda x: ( getUser(x) )).distinct()
        user_res=user_pipeline.collect()
        user_res_df=pd.DataFrame(user_res)
        #        try:
        #            user_res_df.drop_duplicates(inplace=True)
        #        except ValueError: print ('ValueError')
        os.system('mkdir -p /media/data/spicymobile3/data/calc/processed/days/sdk/'+date)
        out_csv = '/media/data/spicymobile3/data/calc/processed/days/sdk/' + date \
                  + '/' + infile.split('.json')[0].split('/')[-1] + '_foodies.csv'
        user_res_df.to_csv(out_csv,index=False)
        del user_res_df
        #'rand' mozna jak nie ma devicekey to identyfikowac po hashcodzie hashcoda
        # bedzie trzeba zrobic piaty i jakies exception handling
        # try except ValueError
        
        #wziac te funkcje z get adhoc keywords?
    return





def get_rands_away(db, j, from_db=False):
    ''' returns rands away from given location '''
    return


def get_rands_nearby(db, j, from_db=False):
    ''' gets rands nearby locations given in json '''
        
    kwd_name=j['kwd_name']
    locations_collection = db['points']
    # przydaloby sie to do jakiejs funkcji dac    
    if locations_collection.count() == 0:
        for doc in tqdm(db['hits_unwinded'].find(
                {"hits.geolocation.location.coordinates": { "$exists" : True  }})):
                doc['hits']['geolocation']['location']['coordinates']=doc[
                        'hits']['geolocation']['location']['coordinates'][::-1]
                db['points'].insert(doc)
        #pipeline = [{"$match" : {"hits.geolocation.location": { "$exists" : True  }}}, { "$out" : 'points' }]
        #db['hits_unwinded'].aggregate(pipeline,allowDiskUse=True, bypassDocumentValidation = True)
        # trzeba to do db points wziac
        db.points.create_index([
    ('hits.geolocation.location', '2dsphere')])

    geoloc_path = j.get('locations_json')
    # trzeba pomyśleć nad profilem wiec i miasto
    # moze miasto najpierw
    if geoloc_path is not None:
        geoloc_f = open(geoloc_path, 'r')
        lines = geoloc_f.readlines()
        if db['pointsNear' + kwd_name].count() == 0:
            for line in tqdm(lines):
                j = ujson.loads(line)
                coordinates = [ j['geometry']['location']['lng'], j['geometry']['location']['lat']]
                          
                points = db['points'] 
                pipeline = [{"$geoNear": {"near": { "type": "Point",
                                                   "coordinates": coordinates },
                                          "distanceField": "dist.calculated",
                                          "maxDistance": 100,
                                          "includeLocs": "dist.location",
                                          "spherical": True}},{"$out" : 'pointsNear'}]
                points.aggregate(pipeline)
             
            for u in db.pointsNear.find(): db['pointsNear'+kwd_name].save(u)

    geoloc_table = j.get('locations_table')

    
    if geoloc_table is not None and kwd_name in ['duze_miasto']:
        # so maybe start with the ones in big cities
        q = session.query(CT).filter(CT.ct_pop_size>=500000)
        res = q.all()
        #res = [i[0] for i in res]
        for ct in res:
            lat = float(ct.ct_cpkt_N)
            lng = float(ct.ct_cpkt_E)
            coordinates = [lng, lat]
            radius = float(ct.ct_r)*1000
            points = db['points'] 
            pipeline = [{"$geoNear": {"near": { "type": "Point",
                                                "coordinates": coordinates },
                                       "distanceField": "dist.calculated",
                                       "maxDistance": radius,
                                       "includeLocs": "dist.location",
                                       "spherical": True}},{"$out" : 'pointsNear'}]
            points.aggregate(pipeline)
             
            for u in db.pointsNear.find(): db['pointsNear'+kwd_name].save(u)


    elif geoloc_table is not None and kwd_name == 'wies':
        q = session.query(CT)
        res = q.all()
        #res = [i[0] for i in res]
        for ct in tqdm(res):
            lat = float(ct.ct_cpkt_N)
            lng = float(ct.ct_cpkt_E)
            coordinates = [lng, lat]
            radius = float(ct.ct_r)*1000
            points = db['points']
            all_geoloc = set(points.distinct('user.advertisingID'))

            pipeline = [{"$geoNear": {"near": { "type": "Point",
                                                "coordinates": coordinates },
                                       "distanceField": "dist.calculated",
                                       "maxDistance": radius,
                                       "includeLocs": "dist.location",
                                       "spherical": True}},{"$out" : 'pointsNear'}]
            points.aggregate(pipeline)
            for adv_id in db.pointsNear.distinct('user.advertisingID'):
                doc = {'user': {'advertisingID' : adv_id}}
                db['pointsNearAnyCity'].save(doc)
        city_people = set(db['pointsNearAnyCity'].distinct('user.advertisingID'))
        village_people = all_geoloc - city_people
        for adv_id in tqdm(village_people):
            doc = {'user': {'advertisingID' : adv_id}}
            db['pointsNear' + kwd_name].save(doc)
    
    rand_set = db['pointsNear'+kwd_name].distinct('user.advertisingID')

    return set(rand_set) 

# wies bedzie trudniejsza get rands outside?
#wies



class InFile(dict):

    def get(self, limit_to=None, from_json=False, host='spicymobile1', db_df=None):
        lines = None
        if from_json:
            f = open('/mnt/md0/SpicyMobile/data/Profile/profile.json','r')
            lines = f.readlines()
        s = session.MySQLSession()
        self.db_df=db_df
        s.get(host=host, db_df=self.db_df)
        print ('session connected')
        if host=='spicymobile1':
                d = {'P':'partner', 'APP':'application', 'DVE':'device',
             'UKWD':'user_keywords', 'KWD':'keywords',
             'APPC': 'application_category',
             'APFC': 'application_function_category',} #'CT':'city'}
        elif host=='devuser':
                d = {'P':'profiles', 'APP':'apps',
                     'APPC':'category_gp', 'APFC': 'category_smf'}        
        for i in d.keys(): setattr(s, i, getattr(s.Base.classes, d[i]))
        s.PD = s.metadata.tables['profiles_def']
        self.s = s
        apps_dict = self.get_apps_dict(lines=lines,limit_to=limit_to)
        return
        
    def get_apps_set(self, j):
        ''' get apps set from loaded json '''
        #{"appc_catcode" : "MAPS_AND_NAVIGATION"}
        s = self.s
        result_set = set()
        d = {'fcats': s.APP.app_apfc_id,
             'cats': s.APP.app_appc_id,
             'appc_catcode': s.APPC.appc_catcode}
        res_set = set()
        for i in set(j.keys()) - set([u'kwd_name', u'desc', u'locations_json',
                                      u'locations_table']):
            if i=='apps': res_set |= set(j[i])            
            else:
                q = s.session.query(s.APP.app_packagename)
                if i=='appc_catcode': q = q.join(s.APPC,s.APP.app_appc_id==s.APPC.appc_id)
                print (j)
                q = q.filter(d[i].in_(j[i]))
                res = q.all()
                i_set = set([k[0] for k in res])
                res_set |= i_set
        return res_set
        
    def get_apps_dict(self, lines=None, limit_to=None):
        apps_dict = {}
        if lines is not None:
                for line in lines:
                    j = ujson.loads(line)
                    if (limit_to is None) or (j['kwd_name'] in limit_to):
                        if j['kwd_name'] not in ['locations_json', 'locations_table']:
                            apps_dict[j['kwd_name']] = self.get_apps_set(j)
                #wykluczyc te, ktore nie maja aplikacji
                self.apps_dict = {}
                for key in apps_dict.keys():
                    if len(apps_dict[key]) >= 1:
                        self.apps_dict[key] = apps_dict[key]
        else:
                s = self.s
                df = pd.DataFrame(s.session.query(s.PD).all())
                df = df.where(pd.notnull(df), None)
                df2 = pd.DataFrame(s.session.query(s.P.prof_id, s.P.prof_name).all())
                df = pd.merge(df,df2, left_on='def_prof_id', right_on='prof_id')
                for i, row in df.iterrows():
                    prof_id = row.prof_name
                    if prof_id not in apps_dict.keys():
                        apps_dict[ prof_id ] = set()
                    if row.def_app_id is not None:
                        app = s.session.query(s.APP.app_packagename).filter(
                                s.APP.app_id==int(row.def_app_id)).one()[0]
                        apps_dict[ prof_id ].add(app)

                    if row.def_cat_id_gp is not None:
                        apps = s.session.query(s.APP.app_packagename).filter(
                                s.APP.app_cat_id_gp1==int(row.def_cat_id_gp)).all()
                        apps = [i[0] for i in apps]
                        apps_dict[ prof_id ] |= set(apps)

                        apps = s.session.query(s.APP.app_packagename).filter(
                                s.APP.app_cat_id_gp2==int(row.def_cat_id_gp)).all()
                        apps = [i[0] for i in apps]
                        apps_dict[ prof_id ] |= set(apps)

                    if row.def_cat_id_smf is not None:
                        apps = s.session.query(s.APP.app_packagename).filter(
                                s.APP.app_cat_smf==int(row.def_cat_id_smf)).all()
                        apps = [i[0] for i in apps]
                        apps_dict[ prof_id ] |= set(apps)
        self.apps_dict = apps_dict
        return self.apps_dict


class Pipeline(dict):

    def get(self, db='mid'):
        ''' inits MySQL session '''
        global Base,engine,session,metadata,P, APP, DVE, UKWD, KWD, CT
        s = session.MySQLSession()
        if db=='mid':
            s.get(host='devuser', db_df=self.db_df)
        elif db=='mid2':
            s.get(host='mid2', db_df=self.db_df)                
        else:
                s.get(host='spicymobile1', db_df=self.db_df)
                print ('session connected')
                d = {'P':'partner', 'APP':'application', 'DVE':'device',
                     'UKWD':'user_keywords', 'KWD':'keywords',
                     'APFC': 'application_function_category',} #'CT':'city'}
                for i in d.keys(): setattr(s,i, getattr(s.Base.classes, d[i]))
        #self.client = MongoClient()
        infile = InFile({})
        infile.get(host='devuser', db_df=self.db_df)
        self.apps_dict = infile.apps_dict
        #self.db = self.client['days-sdk-' + yesterday]
        self.s = s
        return

    def get_users_set_mongo(self, apps_set, dates):
        ''' gets users set given apps set '''
        
        res_set = set()
        if len(apps_set) > 0:
            for date in tqdm(sorted(dates)):
                col = self.client['days-sdk-' + date]['users_sets']
                for users_set in col.find({"_id" : { "$in" : list(apps_set) }}):
                    res_set |= set(users_set['users_set'])
        return res_set 

    def get_users_set_elastic(self, apps_set, dates, field = "hits.networkUsage.apps.packageName"):
        ''' gets users set given apps set '''
        
        ess = es.ElasticSession({}); ess.get(endpoint=self.endpoint)
        res_set = set()
        packagenames = list(apps_set)
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'packagenames' : packagenames, 'user_id' : 'androidID',
                  'indices' : indices}
        kwargs['field'] = field
        #print (indices)
        res_nu = ess.get_app_users_indices(**kwargs)
        '''
        if len(apps_set) > 0:
            for date in tqdm(sorted(dates)):
                col = self.client['days-sdk-' + date]['users_sets']
                for users_set in col.find({"_id" : { "$in" : list(apps_set) }}):
                    res_set |= set(users_set['users_set'])
        '''
        return res_nu 


    def get_users_set_aprh(self, apps_set, dates):
        s_spicy3 = session.MySQLSession()
        s_spicy3.get(host='spicymobile3_m', db_df=self.db_df)
        s_spicy3.A = s_spicy3.Base.classes.application
        s_spicy3.APRH = s_spicy3.Base.classes.application_running_history
        s_spicy3.DVE = s_spicy3.Base.classes.device
        packagenames = list(apps_set)
        app_ids = s_spicy3.session.query(s_spicy3.A.app_id).filter(
                s_spicy3.A.app_packagename.in_(packagenames)).all()
        app_ids = [i[0] for i in app_ids]
        # "pl.allegro"
        dve_ids = s_spicy3.session.query(s_spicy3.APRH.aprh_dve_id).filter(
                and_(
                s_spicy3.APRH.aprh_app_id.in_(app_ids),
                func.date(s_spicy3.APRH.aprh_hourperiod).in_(dates)
                )).all()
        dve_ids = [i[0] for i in dve_ids]
        res_nu = s_spicy3.session.query(s_spicy3.DVE.dve_devicekey).filter(
                s_spicy3.DVE.dve_id.in_(dve_ids)).all()
        res_nu = [i[0] for i in res_nu]
        return res_nu

    def get_rand_set(self, db, adv_set):
        ''' returns rand set given androids set '''
        and_set = set()
        col_users = db['users']
        if (col_users.count() == 0):
            pipeline = [{"$group" : {"_id": '$user'}}, { "$out" : 'users' }]
            db.days.aggregate(pipeline, allowDiskUse=True, bypassDocumentValidation = True);
        for user in col_users.find({"_id.advertisingID" : { "$in" : list(adv_set) }}):
            and_set.add(user['_id']['rand'])
        return and_set

    def add_keyword(self, and_set, kwd_name, db='mid', dates=[]):
        ''' adds keyword to database '''
        kwd_name = kwd_name.replace('_',' ')# np. duze_miasto -> 'duze miasto'
        s = self.s
        if db in ['mid','mid2']:
            s.U = s.Base.classes.user
            s.UP = s.Base.classes.user_profile
            s.P = s.Base.classes.profiles
            s.A = s.Base.classes.apps
            s.D = s.Base.classes.date


            q = s.session.query(s.U.usr_id, s.U.usr_android_id).filter(
                s.U.usr_android_id.in_(list(and_set)))
            res = q.all()
            #trzeba zeby dodawalo nieobecnych userow do bazy
            usr_ids = [str(i[0]) for i in res]
            usr_android_ids = [str(i[1]) for i in res]
            
            usr_android_ids_not_in_u = list(set(and_set) - set(usr_android_ids))
            print(len(usr_android_ids_not_in_u))
            # teraz dla tych wszystkich trzeba wziac advertising id
            # tylko jak?
            # moze skryptem
            # unikalne pary user.androidID
            # ile mozna przetransportowac w JSONie androidIDs,
            # czy jest jakis limit?
            # najpierw zrobic cardinality i do tego dostosowac liczbe partycji ...
            if not CommonConfigElastic.aprh:
                ess = es.ElasticSession({}); ess.get(endpoint=self.endpoint)
                indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
                df = ess.get_missing_users(indices=indices, size=10000, and_set=usr_android_ids_not_in_u)
                #moze by to rozbic jakos
                df_apps = pd.DataFrame(s.session.query(s.A.app_id, s.A.app_packagename).all())
                df_merged = pd.merge(df, df_apps, how='left', left_on='usr_app_id',
                                 right_on='app_packagename')
                
                df_merged = df_merged.where(pd.notnull(df_merged), None)
                
                max_usr_id = s.session.query(func.max(s.U.usr_id)).one()[0]
                for i,row in df_merged.iterrows():
                    max_usr_id += 1
                    new_U = s.U(usr_id=max_usr_id, usr_android_id=row.usr_android_id,
                                usr_advertising_id=row.usr_advertising_id,
                                usr_app_id=row.app_id)
                    s.session.add(new_U)
                s.session.flush()
                s.session.commit()

            q = s.session.query(s.U.usr_id, s.U.usr_android_id).filter(
                s.U.usr_android_id.in_(list(and_set)))
            res = q.all()
            #trzeba zeby dodawalo nieobecnych userow do bazy
            usr_ids = [str(i[0]) for i in res]

            KWD_I = s.session.query(s.P).filter(s.P.prof_name==kwd_name).one()
            #tu updejtujemy, a musimy jeszcze zainsertowac
            res_new = s.session.query(s.UP, s.UP.up_usr_id).filter(
                s.UP.up_usr_id.in_(usr_ids),
                getattr(s.UP, 'up_prof_id_'+str(KWD_I.prof_id))==None).all()
            df = pd.DataFrame(res_new, columns=['up','up_usr_id'])
            new_count = df.shape[0]
                        
            res = s.session.query(s.UP, s.UP.up_usr_id).filter(
                s.UP.up_usr_id.in_(usr_ids)).all()#,
                #getattr(s.UP, 'up_prof_id_'+str(KWD_I.prof_id))==None).all()
            df = pd.DataFrame(res_new, columns=['up','up_usr_id'])

            #date_i = dates[0]
            date_i = datetime.strptime(dates[0],'%Y-%m-%d').date()
            date_id = s.session.query(s.D.date_id).filter(s.D.date==date_i).first()
            if date_id is None:
                max_date = s.session.query(func.max(s.D.date)).one()[0]
                delta = date_i - max_date        # timedelta
                for i in range(1,delta.days + 1):
                        new_date_date = max_date + timedelta(i)
                        new_date = s.D(date=new_date_date)
                        s.session.add(new_date)
                s.session.flush()
                s.session.commit()
                date_id = s.session.query(s.D.date_id).filter(s.D.date==date_i).first()
            date_id = date_id[0]

            for i, row in df.iterrows():
                setattr(row.up, 'up_prof_id_'+str(KWD_I.prof_id), date_id)
                #tu zmienic jedynke na id
                s.session.add(row.up)
                #new_count += 1
            s.session.flush()
            s.session.commit()
            
            #tu insertujemy
            res = s.session.query(s.U.usr_id).outerjoin(s.UP,s.U.usr_id==s.UP.up_usr_id
                ).filter(s.U.usr_android_id.in_(list(and_set)), s.UP.up_usr_id==None).all()
            usrs_not_in_up = [i[0] for i in res]
                
            for usr_id in tqdm(usrs_not_in_up):
                if usr_id is not None and usr_id!='None':
                    # print (usr_id)
                    new_UP = s.UP(up_usr_id=usr_id)
                    setattr(new_UP, 'up_prof_id_'+str(KWD_I.prof_id), date_id)
                    s.session.add(new_UP)
                    new_count += 1
            s.session.flush()
            s.session.commit()
        else:
            #nie zapisujemy bezposrednio do jedynki chociaz jest taka mozliwosc
            q = s.session.query(s.DVE.dve_usr_id).filter(s.DVE.dve_devicekey.in_(list(and_set)))
            usr_ids = [str(i[0]) for i in q.all()]
            res = s.session.query(s.UKWD,s.UKWD.ukwd_usr_id).filter(
                s.UKWD.ukwd_usr_id.in_(usr_ids)).all()
            df = pd.DataFrame(res, columns=['ukwd','ukwd_usr_id'])
            u = 'ukwd'
            usr_ids_in_ukwd, usrs_in_ukwd = [list(df[i]) for i in [u+'_usr_id', u]]
            KWD_I = s.session.query(s.KWD).filter(s.KWD.kwd_name==kwd_name).one()         
            usrs_not_in_ukwd = set(usr_ids) - set(usr_ids_in_ukwd) - set([None])
          
            for usr in usrs_in_ukwd:
                zestaw = set(ujson.loads(usr.ukwd_keywords_ids))
                if KWD_I.kwd_id not in zestaw:
                    zestaw.add(KWD_I.kwd_id)
                    usr.ukwd_keywords_ids = json.dumps(list(zestaw))
                    s.session.add(usr)
                
            for usr_id in usrs_not_in_ukwd:
                if usr_id is not None and usr_id!='None':
                    # print (usr_id)
                    new_UKWD = s.UKWD(ukwd_usr_id=usr_id,ukwd_keywords_ids=json.dumps([KWD_I.kwd_id]))    
                    s.session.add(new_UKWD)
                    # s.session.flush()
            s.session.flush()
            s.session.commit()
        return new_count
    
    def get_big_city_users_set_elastic_geo(self, dates):
        ess = es.ElasticSession({}); ess.get(endpoint=self.endpoint)
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_big_city_users_indices(**kwargs)        
        return res_nu

    def get_small_city_users_set_elastic(self, dates):
        ess = es.ElasticSession({}); ess.get(endpoint=self.endpoint)
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_small_city_users_indices(**kwargs)        
        return res_nu

    def get_village_users_set_elastic(self, dates):
        ess = es.ElasticSession({}); ess.get(endpoint=self.endpoint)
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_village_users_indices(**kwargs)        
        return res_nu

    def get_smartfon_premium_users_set_elastic(self, dates):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_smartfon_premium_users_indices(**kwargs)        
        return res_nu

    def get_duzo_aplikacji_users_set_elastic(self, dates, threshold=60):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices, 'threshold' : threshold}
        res_nu = self.ess.get_duzo_aplikacji_users_indices(**kwargs)
        return res_nu

    def get_tablet_users_set_elastic(self, dates):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_tablet_users_indices(**kwargs)
        return res_nu

    def get_roaming_users_set_elastic(self, dates):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_roaming_users_indices(**kwargs)
        return res_nu

    def get_weak_battery_users_set_elastic(self, dates, threshold=2):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices, 'threshold' : threshold}
        res_nu = self.ess.get_weak_battery_users_indices(**kwargs)
        return res_nu

    def get_weak_signal_users_indices(self, dates, threshold=2):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices, 'threshold' : threshold}
        res_nu = self.ess.get_weak_signal_users_indices(**kwargs)
        return res_nu

    def get_weak_memory_users_indices(self, dates, threshold = 0.1):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices, 'threshold' : threshold}
        res_nu = self.ess.get_weak_memory_users_indices(**kwargs)
        return res_nu

    def get_high_mobile_download_indices(self, dates, threshold=1*10**9):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices, 'threshold' : threshold}
        res_nu = self.ess.get_high_mobile_download_indices(**kwargs)
        return res_nu

    def get_NFC_users_indices(self, dates):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_NFC_users_indices(**kwargs)
        return res_nu

    def get_headphone_users_indices(self, dates):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        kwargs = {'user_id' : 'androidID', 'indices' : indices}
        res_nu = self.ess.get_headphone_users_indices(**kwargs)
        return res_nu

    def get_store_users(self, dates, keyword='Rossmann'):
        indices = ','.join(['sdk-regulartime-' + date_i.replace('-','') for date_i in dates])
        p = '/mnt/md0/SpicyMobile/Code/spicymobile/BigData/keywords/google-maps-services-python/'
        paths = {'Rossmann' : "Rossmann_locations.json",
                 'Biedronka': "Biedronka_locations.json"}

        kwargs = {'user_id' : 'androidID', 'indices' : indices, 'path' : paths[keyword]}
        res_nu = self.ess.get_store_users_indices(**kwargs)
        return res_nu
    
    def get_Rossmann_users(self, dates):
        return self.get_store_users(dates, keyword='Rossmann')

    def get_Biedronka_users(self, dates):
        return self.get_store_users(dates, keyword='Biedronka')
    
    def run(self, limit_to=None, yesterday=None, geo_only=False, db='mid', db_df=None,
            endpoint=None):
        '''  '''
        # musze to przepisac zeby bylo geo only
        # i zeby dodawalo keywordy do bazy zasiegatora
        # dodac 3 kolumny do tabeli user_profiles w bazie mid
        #niech loguje ilu nowym przypisalo
        #zcheckoutowac brancha develop?
        self.db_df = db_df
        self.get(db=db)
        self.endpoint = endpoint
        self.ess = es.ElasticSession({});
        if not CommonConfigElastic.aprh:
            self.ess.get(endpoint=self.endpoint)

        log = ''
        logfile = open('info_get_keywords_dag_elastic.log', 'w')   
        d = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        log = log +  '{"date": "'+ str(d) +'",'
        logfile.write('{"date": "'+ str(d) +'",')
        print('****************************************')
        print('')
        rows = []
        geoloc_dict = {}
        if yesterday is None: yesterday = datetime.strftime(
            datetime.now() - timedelta(2), '%Y-%m-%d')
        os.system('mkdir -p CSVs/'+yesterday)
        for key in ['Rossmann', 'Biedronka', 'duze miasto', 'male miasto',
                    'wies', 'smartfon premium',
                    'duzo aplikacji', 'uzytkownicy tabletow','roamingowcy',
                    'slaba bateria', 'slaby sygnal', 'slaba pamiec',
                    'duzo danych pobiera przez GSM', 'sluchawkowcy', 'NFC'
                    ]:
                print('Getting ' + key + ' users.....')
                try:
                    and_set = []
                    if key == 'duze miasto':
                        and_set = self.get_big_city_users_set_elastic_geo([yesterday])
                    elif key == 'male miasto':
                        and_set = self.get_small_city_users_set_elastic([yesterday])
                    elif key == 'wies':
                        and_set = self.get_village_users_set_elastic([yesterday])
                    elif key == 'smartfon premium':
                        and_set = self.get_smartfon_premium_users_set_elastic([yesterday])
                    elif key == 'duzo aplikacji':                        
                        and_set = self.get_duzo_aplikacji_users_set_elastic([yesterday],
                                threshold=60)
                    elif key == 'uzytkownicy tabletow':
                        and_set = self.get_tablet_users_set_elastic([yesterday])
                    elif key == 'roamingowcy':
                        and_set = self.get_roaming_users_set_elastic([yesterday])
                    elif key == 'slaba bateria':
                        and_set = self.get_weak_battery_users_set_elastic([yesterday])
                    elif key == 'slaby sygnal':
                        and_set = self.get_weak_signal_users_indices([yesterday])
                    elif key == 'slaba pamiec':
                        and_set = self.get_weak_memory_users_indices([yesterday], threshold=0.1)
                    elif key == 'sluchawkowcy':
                        and_set = self.get_headphone_users_indices([yesterday])
                    elif key == 'duzo danych pobiera przez GSM':
                        and_set = self.get_high_mobile_download_indices([yesterday], threshold=1*10**9)
                    elif key == 'NFC':
                        and_set = self.get_NFC_users_indices([yesterday])
                    else:
                        and_set = self.get_store_users([yesterday], keyword=key)
                    log = log +  '"get_'+key+'_users": "Successfully finished getting '+key+'_users.",'
                    logfile.write('"get_'+key+'_users": "Successfully finished getting '+key+'_users.",')
                    log = log +  '"no_'+key+'_users_found": "'+ str(len(and_set))  +'",'
                    logfile.write('"no_'+key+'_users_found": "'+ str(len(and_set))  +'",')            
                except:
                    logfile.write('"get_'+key+'_users": "ERROR: Something went wrong with get_'+key+'_users method:'+str(sys.exc_info()[0])+'.",')
                    log = log +  '"get_'+key+'_users": "ERROR: Something went wrong with get_'+key+'_users method:'+str(sys.exc_info()[0])+'.",'
                logfile.flush()
                p = 'CSVs/'+ yesterday + '/'+key + yesterday + '.csv'
                pd.DataFrame(list(and_set)).to_csv(p, index=False)
                rows.append([key, yesterday, len(list(and_set))])
                print('Adding '+key+' users.....')
                try:
                    new_count = self.add_keyword(and_set, key, db=db, dates=[yesterday])
                    logfile.write('"add_'+key+'_users_to_db": "Successfully finished adding '+key+'_users to db.",')
                    logfile.write('"no_new_users_assigned_'+key+'_keyword": '+str(new_count) +',')

                    log = log + '"add_'+key+'_users_to_db": "Successfully finished adding '+key+'_users to db.",'
                except:
                    logfile.write('"add_'+key+'_users_to_db": "ERROR: Something went wrong with add_'+key+'_users_to_db method:'+str(sys.exc_info()[0])+'.",')
                    log = log + '"add_'+key+'_users_to_db": "ERROR: Something went wrong with add_'+key+'_users_to_db method:'+str(sys.exc_info()[0])+'.",'
                #cos sie killuje i trzeba sie dowiedziec gdzie
                logfile.flush()

        if not geo_only:
                for key in self.apps_dict.keys():
                    apps_set = self.apps_dict[key]
                    #adv_set = self.get_users_set_mongo(apps_set,[yesterday])
                    #and_set = self.get_rand_set(self.db, adv_set)
                    try:
                        if CommonConfigElastic.aprh:
                            and_set = self.get_users_set_aprh(apps_set, [yesterday])
                        else:
                            and_set = self.get_users_set_elastic(apps_set, [yesterday])
                            and_set |= self.get_users_set_elastic(apps_set,
                                        [yesterday], field='cpuProcess.processRunning.name')
                        logfile.write('"get_' + key + '_users": "Successfully finished getting '+ key +'_users.",')
                        log = log + '"get_' + key + '_users": "Successfully finished getting '+ key +'_users.",'
                    except:
                        logfile.write('"get_'+ key +'_users": "ERROR: Something went wrong with get_'+ key +'_users method:'+str(sys.exc_info()[0])+'.",')
                        log = log + '"get_'+ key +'_users": "ERROR: Something went wrong with get_'+ key +'_users method:'+str(sys.exc_info()[0])+'.",'
                    p = 'CSVs/'+ yesterday + '/'+key + yesterday + '.csv'
                    pd.DataFrame(list(and_set)).to_csv(p, index=False)
                    rows.append([key, yesterday, len(list(and_set))])
                    try:
                        new_count = self.add_keyword(and_set, key, db=db, dates=[yesterday])
                        logfile.write('"add_'+key+'_users_to_db": "Successfully finished adding '+key+'_users to db.",')
                        logfile.write('"no_new_users_assigned_'+key+'_keyword": '+str(new_count) +',')
                        log = log + '"add_'+key+'_users_to_db": "Successfully finished adding '+key+'_users to db.",'
                        log = log + '"no_new_users_assigned_'+key+'_keyword": '+str(new_count) +','
                    except ValueError:
                        logfile.write('"add_'+key+'_users_to_db": "ERROR: Something went wrong with add_'+key+'_users_to_db method.",')
                        log = log + '"add_'+key+'_users_to_db": "ERROR: Something went wrong with add_'+key+'_users_to_db method.",'
                    logfile.flush()
        log = log[:-1]
        df = pd.DataFrame(rows, columns=['kwd_name','date','no_kwd_name'])
        df.to_csv('CSVs/'+ yesterday + '/'+'kwds_'+yesterday+'.csv',index=False)
        logfile.write('}')
        log = log + '}'
        logfile.close()
        return log


def main(limit_to=None, yesterday=None, geo_only=False, db='mid', db_df=None,
         endpoint=None):
    '''  '''
    pipeline = Pipeline({})
    log = pipeline.run(yesterday=yesterday, geo_only=geo_only, db=db, db_df=db_df,
                       endpoint=endpoint)
    return log
    

def print_context(ds, **kwargs):
    today = datetime.today()
    year, month, day = [getattr(today, a) for a in ['year','month','day']]
    main()

'''
run_this = PythonOperator(
    task_id='get_keywords',
    provide_context=True,
    python_callable=print_context,
    dag=dag)
'''
import CommonConfigElastic
if __name__ == "__main__":
    yesterday = None
    if len(sys.argv)>=2: yesterday=sys.argv[1]
    if CommonConfigElastic.test:
        db = 'mid'
    else:
        db = 'mid2'
    main(yesterday=yesterday, geo_only=False, db=db, db_df=CommonConfigElastic.db_df,
         endpoint=None)
    '''

    main(yesterday=yesterday, geo_only=True, db=db, db_df=CommonConfigElastic.db_df,
         endpoint=None)
    '''

# usage: python get_keywords_dag.py 2018-06-11 