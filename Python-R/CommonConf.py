
# coding: utf-8
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb as my
import csv
import datetime
import pandas as pd
from pandas.io import sql
from sqlalchemy import create_engine
import io

selectURL = 'mysql://'
insertURL = 'mysql://'

def executeAndSaveIdList( location, query, headers):
    engine = create_engine(selectURL,pool_size=10, max_overflow=20)
    connection = engine.connect()
    #print(engine)
    result = connection.execute(query)
    connection.close
    with open(location, 'w', newline="") as f_handle:
        writer = csv.writer(f_handle)
        header = headers
        writer.writerow(header)
        for row in result: 
            row_as_dict= dict(row)
            dve_id = row_as_dict['dve_id']
            dve_usr_id = row_as_dict['dve_usr_id']
            dve_id = str(dve_id).replace('"','')
            dve_usr_id = str(dve_usr_id).replace('"','')
            s = str(dve_id) +';'+str(dve_usr_id)
            writer.writerow([s])
			
def executeAndSaveDemografy( location, query, headers):
    engine = create_engine(selectURL,pool_size=10, max_overflow=20)
    connection = engine.connect()
    #print(engine)
    result = connection.execute(query)
    connection.close
    with open(location, 'w', newline='') as f_handle:
        writer = csv.writer(f_handle)
        header = headers
        writer.writerow(header)
        for row in result:
             writer.writerow(row)
    df = pd.read_csv(location, sep='delimiter', names=headers,engine='python')
    df = pd.DataFrame(df.dve_id.str.split(',').tolist(),
                                         columns =headers)

    df['Wiek']=df['Wiek'].str.replace('.0','')
    df['Wiek']=df['Wiek'].str.replace('"','')
    df['dve_id']=df['dve_id'].str.replace('"','')
    df[1:].to_csv(location, index = False, sep=';', encoding='cp1250')
	
def executeAndSave( location, query, headers):
    engine = create_engine(selectURL, encoding = 'utf-8',pool_size=10, max_overflow=20)
    connection = engine.connect()
    #print(engine)
    result = connection.execute(query)
    connection.close
    with open(location, 'w', newline='') as f_handle:
        writer = csv.writer(f_handle, quoting=csv.QUOTE_NONE, delimiter="\t")
        header = headers
        writer.writerow(header)
        writer = csv.writer(f_handle, delimiter=';')
        for row in result:
             writer.writerow(row)
def readAndInsertAge(path):
    df = pd.read_csv(path + 'results/prognozy_wiek_final.csv', sep='delimiter', names=['dwa_usr_id', 'status', 'dwa_pred_age', 'dwa_age_range', 'dwa_age_low', 'dwa_age_high','dwa_dataset'],engine='python')

    df1 = pd.DataFrame(df.dwa_usr_id.str.split(';').tolist(),
                                        columns = ['dwa_usr_id', 'status', 'dwa_pred_age', 'dwa_age_range', 'dwa_age_low', 'dwa_age_high','dwa_dataset'])
    del df1['status'] 
    df1 = df1[1:]
    #inserting columns
    df1.insert(loc=0, column='dwa_dve_id', value=0)
    date = datetime.datetime.now().strftime ("%Y%m%d")
    df1.insert(loc=1, column='dwa_importdate', value=date)
    #converting to float
    df1['dwa_age_low'] = df1['dwa_age_low'].str.replace(',','.')
    df1['dwa_age_low'] =  pd.to_numeric(df1['dwa_age_low'])

    df1['dwa_age_high'] = df1['dwa_age_high'].str.replace(',','.')
    df1['dwa_age_high'] =  pd.to_numeric(df1['dwa_age_high'])

    df1['dwa_pred_age'] = df1['dwa_pred_age'].str.replace(',','.')
    df1['dwa_pred_age'] =  pd.to_numeric(df1['dwa_pred_age'])
    engine = create_engine(insertURL)
    df1.to_sql(con=engine, name='demography_wlog_age', if_exists='append', index = False)


def readAndInsertGender(path):
    df = pd.read_csv(path +'results/prognozy_plec_final.csv',sep='delimiter',names=['dwg_usr_id', 'status', 'dwg_gender', 'dwg_woman', 'dwg_men', 'dwg_dataset'],engine='python')
    df2 = pd.DataFrame(df.dwg_usr_id.str.split(';').tolist(), 
                                   columns = ['dwg_usr_id', 'status', 'dwg_gender', 'dwg_woman', 'dwg_men', 'dwg_dataset'])

    df2=df2[1:]
    del df2['status'] 
    #inserting columns
    df2.insert(loc=0, column='dwg_dve_id', value=0)
    date = datetime.datetime.now().strftime ("%Y%m%d")
    df2.insert(loc=1, column='dwg_importate', value=date)
    #converting to float
    df2['dwg_woman'] = df2['dwg_woman'].str.replace(',','.')
    df2['dwg_woman'] =  pd.to_numeric(df2['dwg_woman'])
    #converting to float
    df2['dwg_men'] = df2['dwg_men'].str.replace(',','.')
    df2['dwg_men'] =  pd.to_numeric(df2['dwg_men'])
    #insert
    engine = create_engine(insertURL)
    df2.to_sql(con=engine, name='demography_wlog_gender', if_exists='append', index = False)
