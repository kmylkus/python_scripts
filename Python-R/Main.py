
# coding: utf-8
from __future__ import division
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb as my
import csv
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import SqlQueries as sql
import CommonConf as cm
import sys
import rpy2.robjects as robjects
import os as o
import sys
import time

#poczatek = '20180101';
#koniec = '20180131';
#os = '/home/'

start_time = time.time()
if sys.version_info <= (3, 0):
    sys.stdout.write("---Sorry, requires Python 3.x, not Python 2.x\n")
    sys.exit(1)
	
if len(sys.argv)<6:
    sys.stdout.write("---Please enter correct arguments... Directory Path, StartDate as yyyymmdd, EndDate as yyyymmdd, StartDate2 as yyyymmdd, EndDate2 as yyyymmdd\n")
    sys.exit(1)
	
print("---Starting executing sql queries")
os = sys.argv[1]
poczatek = sys.argv[2]
koniec = sys.argv[3]
poczatek_2 = sys.argv[4]
koniec_2 = sys.argv[5]
#sqlquery
demografia_all = sql.demografia_all()
domeny = sql.domeny(poczatek, koniec)
smsy = sql.smsy(poczatek, koniec)
kategorie_app = sql.kategorie_app(poczatek, koniec)
polaczenia = sql.polaczenie(poczatek, koniec)
wybrane_app = sql.wybrane_app(poczatek, koniec)

domeny_2 = sql.domeny(poczatek_2, koniec_2)
smsy_2 = sql.smsy(poczatek_2, koniec_2)
kategorie_app_2 = sql.kategorie_app(poczatek_2, koniec_2)
polaczenia_2 = sql.polaczenie(poczatek_2, koniec_2)
wybrane_app_2 = sql.wybrane_app(poczatek_2, koniec_2)

#pobieranie danych
cm.executeAndSaveIdList(os + 'data_raw/id_list.csv',demografia_all,['dve_id;dve_usr_id'])
print('---Data saved to ..data_raw/id_list.csv')
cm.executeAndSaveDemografy(os + 'data_raw/demografia/demografia_all.csv',demografia_all,['dve_id','dve_usr_id','Zaznacz_swoją_płeć','Określ_swoje_wykszałcenie','Wskaż_wielkość_miejscowości','Wiek'])
print('---Data saved to ..data_raw/demografia/demografia_all.csv')
cm.executeAndSave(os + 'data_raw/domeny/domeny'+ '_'+ poczatek+ '.csv',domeny, ['dve_id;wbhh_hourperiod;wdmn_domain;wywolan;unikalni_uzytkownicy'])
cm.executeAndSave(os + 'data_raw/domeny/domeny'+ '_'+ poczatek_2+ '.csv',domeny_2, ['dve_id;wbhh_hourperiod;wdmn_domain;wywolan;unikalni_uzytkownicy'])
print('---Data saved to ...data_raw/domeny/domeny.csv')
cm.executeAndSave(os + 'data_raw/kategorie_app/kategorie_app'+ '_'+ poczatek+ '.csv', kategorie_app, ['aprh_hourperiod;dve_id;appc_catname;czas'])
cm.executeAndSave(os + 'data_raw/kategorie_app/kategorie_app'+ '_'+ poczatek_2+ '.csv', kategorie_app_2, ['aprh_hourperiod;dve_id;appc_catname;czas'])
print('---Data saved to ...data_raw/kategorie_app/kategorie_app.csv')
cm.executeAndSave(os + 'data_raw/polaczenia/polaczenia'+ '_'+ poczatek+ '.csv', polaczenia, ['dve_id;calh_hourperiod;call-przychodzace-czas;call-wychodzace-czas'])
cm.executeAndSave(os + 'data_raw/polaczenia/polaczenia'+ '_'+ poczatek_2+ '.csv', polaczenia_2, ['dve_id;calh_hourperiod;call-przychodzace-czas;call-wychodzace-czas'])
print('---Data saved to ...data_raw/polaczenia/polaczenia.csv')
cm.executeAndSave(os + 'data_raw/smsy/smsy'+ '_'+ poczatek+ '.csv',smsy,['dve_id;smsh_hourperiod;sms-dlugosc-odebrane;sms-dlugosc-wyslane'])
cm.executeAndSave(os + 'data_raw/smsy/smsy'+ '_'+ poczatek_2+ '.csv',smsy_2,['dve_id;smsh_hourperiod;sms-dlugosc-odebrane;sms-dlugosc-wyslane'])
print('---Data saved to ...data_raw/smsy/smsy.csv')
cm.executeAndSave(os + 'data_raw/wybrane_app/wybrane_app'+ '_'+ poczatek+ '.csv',wybrane_app,['aprh_hourperiod;dve_id;app_packagename;czas'])
cm.executeAndSave(os + 'data_raw/wybrane_app/wybrane_app'+ '_'+ poczatek_2+ '.csv',wybrane_app_2,['aprh_hourperiod;dve_id;app_packagename;czas'])
print('---Data saved to ...data_raw/wybrane_app/wybrane_app.csv')

#R skript
print('---Starting executing R skript')
r=robjects.r
r.source("01_executor.R")

#ReadAndInsert 
cm.readAndInsertAge(os)
print('---Data insert to the table demography_wlog_age')
cm.readAndInsertGender(os)
print('---Data insert to the table demography_wlog_gender')

#removing
print('---Removing data files............')
o.remove(os + 'data_raw/id_list.csv')
o.remove(os + 'data_raw/demografia/demografia_all.csv')
o.remove(os + 'data_raw/domeny/domeny'+ '_'+ poczatek+ '.csv')
o.remove(os + 'data_raw/domeny/domeny'+ '_'+ poczatek_2+ '.csv')
o.remove(os + 'data_raw/kategorie_app/kategorie_app'+ '_'+ poczatek+ '.csv')
o.remove(os + 'data_raw/kategorie_app/kategorie_app'+ '_'+ poczatek_2+ '.csv')
o.remove(os + 'data_raw/polaczenia/polaczenia'+ '_'+ poczatek+ '.csv')
o.remove(os + 'data_raw/polaczenia/polaczenia'+ '_'+ poczatek_2+ '.csv')
o.remove(os + 'data_raw/smsy/smsy'+ '_'+ poczatek+ '.csv')
o.remove(os + 'data_raw/smsy/smsy'+ '_'+ poczatek_2+ '.csv')
o.remove(os + 'data_raw/wybrane_app/wybrane_app'+ '_'+ poczatek+ '.csv')
o.remove(os + 'data_raw/wybrane_app/wybrane_app'+ '_'+ poczatek_2+ '.csv')

print('---Data files deleted')
print("---Program execution time " +str((time.time() - start_time)/60)+  " minutes")