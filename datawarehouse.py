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
# csvToMySQL()


# create schema bi_date .. MySQL does not really offer this? Nope so I create a db

engine.execute("CREATE DATABASE IF NOT EXISTS bi_date") 
engine.execute("USE bi_date") 

# take out the invalid offers
offerpd = offerpd.loc[offerpd['valid_offer_flag'] == 1]

# rename rates id column to hotel_id in ratespd
ncol = ratespd.columns.values
ncol[0] = 'hotel_id'
ratespd.columns = ncol
mpd = pd.merge(ratespd,offerpd,on='hotel_id')

# rename id column to currency_id in currpd
ncol = currpd.columns.values
ncol[0] = 'currency_id'
currpd.columns = ncol
mpd = pd.merge(mpd,currpd,on='currency_id')

# price in USD
mpd['price_usd'] = mpd['sellings_price'] * mpd['currency_rate']

# create valid_offers
mpd['price_usd'] = mpd['sellings_price'] * mpd['currency_rate']
mpd = mpd.drop(['prim_currency_id', 'scnd_currency_id','date', 'available_cnt', 'currency_rate', 'source_system_code','checkin_date','checkout_date','valid_offer_flag','insert_datetime','name'], axis=1)
mpd = mpd[mpd['code'] != 'USD']
print mpd.head()
# mpd.to_sql('valid_offers', engine, if_exists='replace',index=False)

# create table 'hotel_offers'


# get hotel_id, date and hour
offerspd = offerpd[['hotel_id','offer_valid_from','offer_valid_to']]
offerspd['date'] = offerspd['offer_valid_from'].value.str.split(" ").stack().str.strip().reset_index(level=1, drop=True)
print offerspd.head()

connection.close()
