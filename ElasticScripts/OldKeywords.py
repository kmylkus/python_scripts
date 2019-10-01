# -*- coding: utf-8 -*-
#!/usr/bin/python3.6

from session import MySQLSession
from datetime import datetime
import pandas as pd
import json, ujson
from tqdm import tqdm
from sqlalchemy import or_

def remove_old_keyword_assignment(kwd_name, date_i, profile_name):
    s = MySQLSession()
    if CommonConfigElastic.test:
        s.get(host='devuser', db_df=CommonConfigElastic.db_df)
    else:
        s.get(host='mid2', db_df=CommonConfigElastic.db_df)
    # no to teraz czy szukamy starych czy nowych? moze nowych keywordow
    UP = s.Base.classes.user_profile
    U = s.Base.classes.user
    D  = s.Base.classes.date
    
    s_spicy1 = MySQLSession()
    if CommonConfigElastic.test:
        s_spicy1.get(host='devuser2', db_df=CommonConfigElastic.db_df)
    else:
        s_spicy1.get(host='spicymobile1_m', db_df=CommonConfigElastic.db_df)
    UKWD = s_spicy1.Base.classes.user_keywords
    KWD = s_spicy1.Base.classes.keywords
    # KWD_I
    KWD_I = s_spicy1.session.query(KWD).filter(
        KWD.kwd_name==kwd_name).one()
    i = KWD_I.kwd_id

    all_ids = s_spicy1.session.query(DVE.dve_devicekey).join(
        UKWD, UKWD.ukwd_usr_id==DVE.dve_usr_id).filter(
        or_(
            UKWD.ukwd_keywords_ids.like('['+str(i)+', %'),
            UKWD.ukwd_keywords_ids.like('% '+str(i)+', %'),
            UKWD.ukwd_keywords_ids.like('%, '+str(i)+']'),
            UKWD.ukwd_keywords_ids.like('['+str(i)+']')
            )        
    ).all()
    # UKWD = s_spicy_test.UKWD
    # spicy_test nie ma device'a bedzie trzeba to uzupelnic
    '''
    res = s_spicy_test.session.query(UKWD.ukwd_usr_id).filter(
        or_(
            UKWD.ukwd_keywords_ids.like('['+str(i)+', %'),
            UKWD.ukwd_keywords_ids.like('% '+str(i)+', %'),
            UKWD.ukwd_keywords_ids.like('%, '+str(i)+']'),
            UKWD.ukwd_keywords_ids.like('['+str(i)+']')
            )
        ).filter(UKWD.ukwd_usr_id.in_(usr_ids_in_ukwd)).all()
    '''
    date_id = s.session.query(D.date_id).filter(D.date==date_i).first()
    date_id = date_id[0]

    new_ids = s.session.query(U.usr_android_id).join(UP,U.usr_id==UP.up_usr_id
                ).filter(UP.up_prof_id_1>=date_id).all()
    new_ids = set([i[0] for i in new_ids])
    old_ids = list(all_ids - new_ids)
    # teraz usuwamy z old_ids
    # tylko ze lacznikiem jest dve_devicekey
    # to moze utworzyc tabele device na spicy_test
    # to bedzie zbyt skomplikowane wiec
    # najpierw rozpisze to tak jak ma dzialac na spicy2 na spicymobile1.gem.pl
    # wiec mamy liste devicekeyow, gdzie mamy zapdejtowac rekordy w user_keywords
    # albo
    usr_ids = s_spicy1.session.query(DVE.dve_usr_id).filter(DVE.dve_devicekey.in_(old_ids))
    ukwds_to_delete = s_spicy1.session.query(UKWD).filter(
        and_(UKWD.ukwd_usr_id.in_(usr_ids),
             UKWD.ukwd_keywords_ids.like('['+str(i)+']'))
    )
    ukwds_to_delete.delete()
    ukwds_to_update = s_spicy1.session.query(UKWD).filter(
        and_(UKWD.ukwd_usr_id.in_(usr_ids),
             or_(
                UKWD.ukwd_keywords_ids.like('['+str(i)+', %'),
                UKWD.ukwd_keywords_ids.like('% '+str(i)+', %'),
                UKWD.ukwd_keywords_ids.like('%, '+str(i)+']'))
    )).all()
    for ukwd_i in ukwds_to_update:
        keywords_ids = set(json.loads(ukwd_i.ukwd_keywords_ids)) - set(i)
        ukwd_i.ukwd_keywords_ids = json.dumps(keywords_ids)
        s_spicy1.session.add(ukwd_i)
    s_spicy1.session.flush()
    s_spicy1.session.commit()
    return



if __name__ == '__main__':
    date_i = datetime(2018,12,26).date
    profile_name = 'Kierowca'
    remove_old_keyword_assignment('kierowca', date_i, profile_name)