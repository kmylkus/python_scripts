# -*- coding: utf-8 -*-
#!/usr/bin/python3.6

from session import MySQLSession
from datetime import datetime
import pandas as pd
import json, ujson
from tqdm import tqdm
from sqlalchemy import or_


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def add_keyword(and_set, kwd_name, test=True, db_df=None):
    ''' adds keyword to database, na razie do spicy_test,
        docelowo do spicy2 na spicymobile1'''
    s = MySQLSession()
    s.get(host='spicymobile1_m', db_df=db_df)
    s.DVE = s.Base.classes.device
    s.KWD = s.Base.classes.keywords
        
    if test:
        s_spicy_test = MySQLSession()
        s_spicy_test.get(host='devuser2', db_df=db_df)
    else:
        s_spicy_test = s
    s_spicy_test.UKWD = s_spicy_test.Base.classes.user_keywords
    s_spicy_test.U = s_spicy_test.Base.classes.user
    s_spicy_test.KWD = s_spicy_test.Base.classes.keywords

    kwd_name = kwd_name.replace('_',' ')# np. duze_miasto -> 'duze miasto'
    q = s.session.query(s.DVE.dve_usr_id).filter(s.DVE.dve_devicekey.in_(list(and_set)))
    usr_ids = [str(i[0]) for i in q.all()]
    res = s_spicy_test.session.query(s_spicy_test.UKWD,
                                     s_spicy_test.UKWD.ukwd_usr_id).filter(
            s_spicy_test.UKWD.ukwd_usr_id.in_(usr_ids)).all()
    df = pd.DataFrame(res, columns=['ukwd','ukwd_usr_id'])
    u = 'ukwd'
    usr_ids_in_ukwd, usrs_in_ukwd = [list(df[i]) for i in [u+'_usr_id', u]]
    KWD_I = s_spicy_test.session.query(s_spicy_test.KWD).filter(
        s_spicy_test.KWD.kwd_name==kwd_name).one()         
    usrs_not_in_ukwd = set(usr_ids) - set(usr_ids_in_ukwd) - set([None])
          
    for usr in usrs_in_ukwd:
        zestaw = set(ujson.loads(usr.ukwd_keywords_ids))
        if KWD_I.kwd_id not in zestaw:
            zestaw.add(KWD_I.kwd_id)
            usr.ukwd_keywords_ids = json.dumps(list(zestaw))
            s_spicy_test.session.add(usr)
    
    q = s_spicy_test.session.query(s_spicy_test.U.usr_id).filter(
        s_spicy_test.U.usr_id.in_(usrs_not_in_ukwd))
    res = [str(i[0]) for i in q.all()]
    #for usr_id in usrs_not_in_ukwd:
    for usr_id in res:
        if usr_id is not None and usr_id!='None':
            # print (usr_id)
            # dopisac warunek, albo cos co bedzie insertowac brakujacych userow
            new_UKWD = s_spicy_test.UKWD(ukwd_usr_id=usr_id,ukwd_keywords_ids=json.dumps([KWD_I.kwd_id]))    
            s_spicy_test.session.add(new_UKWD)
            # s.session.flush()
    s_spicy_test.session.flush()
    s_spicy_test.session.commit()
    return


def add_keyword_refactored(and_set, kwd_name, test=True, db_df=None):
    ''' adds keyword to database, na razie do spicy_test,
        docelowo do spicy2 na spicymobile1 '''
    s = MySQLSession()
    s.get(host='spicymobile1_m', db_df=db_df)
    s.DVE = s.Base.classes.device
    s.KWD = s.Base.classes.keywords
        
    if test:
        s_spicy_test = MySQLSession()
        s_spicy_test.get(host='devuser2', db_df=db_df)
    else:
        s_spicy_test = s
    s_spicy_test.UKWD = s_spicy_test.Base.classes.user_keywords
    s_spicy_test.U = s_spicy_test.Base.classes.user
    s_spicy_test.KWD = s_spicy_test.Base.classes.keywords

    kwd_name = kwd_name.replace('_',' ')# np. duze_miasto -> 'duze miasto'
    q = s.session.query(s.DVE.dve_usr_id).filter(s.DVE.dve_devicekey.in_(list(and_set)))
    usr_ids = [str(i[0]) for i in q.all()]
    res = s_spicy_test.session.query(s_spicy_test.UKWD,
                                     s_spicy_test.UKWD.ukwd_usr_id).filter(
            s_spicy_test.UKWD.ukwd_usr_id.in_(usr_ids)).all()
    df = pd.DataFrame(res, columns=['ukwd','ukwd_usr_id'])
    u = 'ukwd'
    usr_ids_in_ukwd, usrs_in_ukwd = [list(df[i]) for i in [u+'_usr_id', u]]
    KWD_I = s_spicy_test.session.query(s_spicy_test.KWD).filter(
        s_spicy_test.KWD.kwd_name==kwd_name).one()         
    usrs_not_in_ukwd = set(usr_ids) - set(usr_ids_in_ukwd) - set([None])
    i = KWD_I.kwd_id
    UKWD = s_spicy_test.UKWD
    res = s_spicy_test.session.query(UKWD.ukwd_usr_id).filter(
        or_(
            UKWD.ukwd_keywords_ids.like('['+str(i)+', %'),
            UKWD.ukwd_keywords_ids.like('% '+str(i)+', %'),
            UKWD.ukwd_keywords_ids.like('%, '+str(i)+']'),
            UKWD.ukwd_keywords_ids.like('['+str(i)+']'),
            )).filter(UKWD.ukwd_usr_id.in_(usr_ids_in_ukwd)).all()
    res = [i[0] for i in res]
    usr_ids_to_add = list(set(usr_ids_in_ukwd) - set(res))
    usrs_to_add = s_spicy_test.session.query(UKWD).filter(UKWD.ukwd_usr_id.in_(
        usr_ids_to_add)).all()
    usrs_to_add = [i[0] for i in usrs_to_add]
    for usr in tqdm(usrs_to_add):
        zestaw = set(ujson.loads(usr.ukwd_keywords_ids))
        if KWD_I.kwd_id not in zestaw:
            zestaw.add(KWD_I.kwd_id)
            usr.ukwd_keywords_ids = json.dumps(list(zestaw))
            s_spicy_test.session.add(usr)
    
    q = s_spicy_test.session.query(s_spicy_test.U.usr_id).filter(
        s_spicy_test.U.usr_id.in_(usrs_not_in_ukwd))
    res = [str(i[0]) for i in q.all()]
    #for usr_id in usrs_not_in_ukwd:
    for usr_id in tqdm(res):
        if usr_id is not None and usr_id!='None':
            # print (usr_id)
            # dopisac warunek, albo cos co bedzie insertowac brakujacych userow
            new_UKWD = s_spicy_test.UKWD(ukwd_usr_id=usr_id,ukwd_keywords_ids=json.dumps([KWD_I.kwd_id]))    
            s_spicy_test.session.add(new_UKWD)
            # s.session.flush()
    s_spicy_test.session.flush()
    s_spicy_test.session.commit()
    return



def main(test=False, kwd_name = 'kierowca', profile_name='Kierowca', db_df=None):

    '''
    to jest wersja testowa:
        z zasiegatora, a konkretnie z jego bazy 'mid' 
        przerzuca keywordy do 'spicy2_test'
    '''

    s = MySQLSession()
    if test:
        s.get(host='devuser', db_df=db_df)
    else: 
        s.get(host='mid2', db_df=db_df)

    s_spicy_test = MySQLSession()
    if test:
        s_spicy_test.get(host='devuser2', db_df=db_df)
    else:
        s_spicy_test.get(host='spicymobile1_m', db_df=db_df)

    s2 = MySQLSession()
    s2.get(host='spicymobile1_m', db_df=db_df)
    
    U = s.Base.classes.user
    UP = s.Base.classes.user_profile
    P = s.Base.classes.profiles
    UKWD_S = s_spicy_test.Base.classes.user_keywords

    '''
    ukwds = []
    for ukwd in tqdm(s2.session.query(UKWD).yield_per(10000)):
        ukwds.append(ukwd)
    '''
    
    #q = s.session.query(P.prof_id).filter(P.prof_name==profile_name)
    prof_id = s.session.query(P.prof_id).filter(P.prof_name==profile_name).one()[0]
    #kierowcy w bazie mid
    
    q = s.session.query(U.usr_android_id).join(UP, U.usr_id==UP.up_usr_id).filter(
        getattr(UP, 'up_prof_id_%d' %prof_id)==1, U.usr_android_id!=None)
    
    res = [i[0] for i in q.all()]
    
    #trzeba dodac skeywordowanych uzytkownikow do bazy
    # to moze byc add_keyword z get_keywords_dag_elastic
    #add_keyword_refactored(res, kwd_name, test=test, db_df=db_df)
    add_keyword(res, kwd_name, test=test, db_df=db_df)
    return


def main_main(test=False, db_df=None):
    '''
    to jest wersja testowa:
        z zasiegatora, a konkretnie z jego bazy 'mid' 
        przerzuca keywordy do 'spicy2_test'
    '''
    primitive_mapping_pk = {'Kierowca':'kierowca', 'Rodzic':'rodzic',
 'Aktywny finansowo':'bankowiec', 'Smakosz':'smakosz', 'Turysta':'podroznik',
 'Aktywny fizycznie':'sportowiec aktywny', 'Milenialsi':'generacja Z',
 'Zakupoholik':'zakupoholik', 'Biedronka':'Biedronka','Rossmann':'Rossmann'}
    #'duże miasto':'duze miasto'} #, 'NetflixHBO':'kinomaniak'}
    # w  dev jest duze miasto a w prod jest duże miasto
    if test: primitive_mapping_pk['duze miasto'] = 'duze miasto'
    else: primitive_mapping_pk['duże miasto'] = 'duze miasto'
    for i  in ['smartfon premium',
                    'duzo aplikacji', 'uzytkownicy tabletow','roamingowcy',
                    'slaba bateria', 'slaby sygnal', 'slaba pamiec',
                    'duzo danych pobiera przez GSM', 'sluchawkowcy', 'NFC',
                    'video', 'poszukujacy pracy', 'gracze', 'male miasto',
                    'wies']:
        primitive_mapping_pk[i] = i
    logfile = open('info_update_sp1_ukwds_from_zasiegator.log', 'w')
    logtext = ''
    d = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logtext = logtext + '{"date": "'+ str(d) +'",'
    logfile.write('{"date": "'+ str(d) +'",')
    print('****************************************')
    print('')
    
    for profile_name in primitive_mapping_pk.keys():
        try:
            kwd_name = primitive_mapping_pk[profile_name]
            print('Copying %s profile users to %s keyword...' %(profile_name, kwd_name) )
            main(test=test, kwd_name=kwd_name, profile_name=profile_name, db_df=db_df)
            logfile.write('"copy_'+kwd_name+'_users_to_db": "Successfully finished adding '+kwd_name+'_users to db.",')
            logtext = logtext + '"copy_'+kwd_name+'_users_to_db": "Successfully finished adding '+kwd_name+'_users to db.",'
        except ValueError:
            logfile.write('"copy_'+kwd_name+'_users_to_db": "There was something wrong with adding '+kwd_name+'_users to db.",')
            logtext = logtext + '"copy_'+kwd_name+'_users_to_db": "There was something wrong with adding '+kwd_name+'_users to db.",'
        logfile.flush()
    logfile.write('}')
    logtext = logtext[:-1]
    logtext = logtext + '}'
    return logtext

#dopiescic te logi

import CommonConfigElastic

if __name__ == '__main__':
    main_main(test=CommonConfigElastic.test, db_df=CommonConfigElastic.db_df)
