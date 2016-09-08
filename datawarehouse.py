import pandas as pd

from pandas import DataFrame, Series
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import datetime

Session = sessionmaker()
engine = create_engine('mysql://root:root@127.0.0.1/hq?charset=utf8')
connection = engine.connect()
session = Session()

# create db 'hq' if not exist
engine.execute("CREATE DATABASE IF NOT EXISTS hq") 
engine.execute("USE hq") 

# create schema primary_date 

# create tables 'fx_rate', 'lst_currency', 'offer' if not exist
# cursor.execute(sql)
ratespd = pd.DataFrame(pd.read_csv('fx_rate.csv',header=0,sep=',',parse_dates=True))
currpd = pd.DataFrame(pd.read_csv('lst_currency.csv',header=0,sep=',',parse_dates=True))
offerpd = pd.DataFrame(pd.read_csv('offer.csv',header=0,sep=',',parse_dates=True))

# skip 1 row in lst_currency
currpd = currpd[1:]

# rename columns in offerpd
offerpd.columns = ['id','hotel_id','currency_id','source_system_code','available_cnt','sellings_price',
      'checkin_date','checkout_date','valid_offer_flag','offer_valid_from','offer_valid_to',
      'breakfast_included_flag','insert_datetime']

def csvToMySQL():
   try:
      currpd.to_sql('lst_currency', engine, if_exists='append')
      ratespd.to_sql('fx_rate', engine, if_exists='append',chunksize=1000)
      offerpd.to_sql('offer', engine, if_exists='append',chunksize=1000,index=False)
      session.commit()
      print datetime.now() - startTime
   finally:
      session.rollback()
      connection.close()

startTime = datetime.now()
csvToMySQL()


# create schema bi_date

engine.execute("CREATE DATABASE IF NOT EXISTS bi_date;") 
engine.execute("USE bi_date") 

# take out the invalid offers
offerpd = offerpd.loc[offerpd['valid_offer_flag'] == 1]
offerpd.drop(['source_system_code','valid_offer_flag','checkin_date','checkout_date','available_cnt','id'],inplace=True,axis=1)
offerpd['insert_date'] = pd.to_datetime(offerpd['insert_datetime']).dt.date
offerpd['insert_time'] = pd.to_datetime(offerpd['insert_datetime']).dt.time

# rename rates id column to hotel_id in ratespd
ncol = ratespd.columns.values
ncol[1] = 'currency_id'    # prim_currency_id
ncol[3] = 'insert_date'
ratespd.columns = ncol

# only to USD
ratespd = ratespd[ratespd['scnd_currency_id'] == 1]   
# force datatypes
ratespd.currency_id = ratespd.currency_id.astype(float)
ratespd.insert_date = pd.to_datetime(ratespd.insert_date)
offerpd.insert_date = pd.to_datetime(offerpd.insert_date)

mpd = pd.merge(offerpd,ratespd,how='left',on=['insert_date','currency_id'])

# show specific day
# mpd.insert_date = mpd.insert_date.astype(str)
# mpd = mpd.set_index(['insert_date'])
# mpd = mpd.loc['2015-10-26']

# rename id column to currency_id in currpd
ncol = currpd.columns.values
ncol[0] = 'currency_id'
currpd.columns = ncol
currpd.currency_id = currpd.currency_id.astype(float)
mpd = pd.merge(mpd,currpd,how='left',on=['currency_id'])

# price in USD
mpd['price_usd'] = mpd['sellings_price'] * mpd['currency_rate']
# drop unnecessary columns
mpd.drop(['currency_id','insert_datetime','insert_time','scnd_currency_id','currency_rate','name'],inplace=True,axis=1)
# reorder columns to specifications
mpd = DataFrame(mpd, columns=['id', 'hotel_id','price_usd', 'sellings_price', 'code', 'breakfast_included_flag','offer_valid_from','offer_valid_to'])
print mpd.head()

# create valid_offers

# create table 'hotel_offers'

connection.close()
