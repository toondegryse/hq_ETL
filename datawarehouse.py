import pandas as pd
from pandas import DataFrame, Series
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime

class HQ_etl:
	def __init__(self):
		# start a timer
		startTime = datetime.now()
		# create session
		Session = sessionmaker()
		engine = create_engine('mysql://root:root@127.0.0.1/hq?charset=utf8')
		connection = engine.connect()
		session = Session()
		# create db 'hq' if not exist
		sql = "CREATE DATABASE IF NOT EXISTS hq; USE hq;"
		session.query(sql) 

		# load csv files into dataframes
		ratespd = pd.DataFrame(pd.read_csv('fx_rate.csv',header=0,sep=',',parse_dates=True))
		currpd = pd.DataFrame(pd.read_csv('lst_currency.csv',header=0,sep=',',parse_dates=True))
		offerpd = pd.DataFrame(pd.read_csv('offer.csv',header=0,sep=',',parse_dates=True))

		# load data to MySQL database 1:1
		try:
			currpd.to_sql('lst_currency', engine, if_exists='append')
			ratespd.to_sql('fx_rate', engine, if_exists='append',chunksize=1000)
			offerpd.to_sql('offer', engine, if_exists='append',chunksize=1000,index=False)
			session.commit()
   		finally:
   			session.rollback()

		# rename columns in offerpd, remove invalid records, drop unnecessary columns
		offerpd.columns = ['id','hotel_id','currency_id','source_system_code','available_cnt','sellings_price','checkin_date','checkout_date','valid_offer_flag','offer_valid_from','offer_valid_to','breakfast_included_flag','insert_datetime']
		offerpd = offerpd.loc[offerpd['valid_offer_flag'] == 1]
		offerpd = offerpd[offerpd['sellings_price'] != 0]
		offerpd.drop(['source_system_code','valid_offer_flag','checkin_date','checkout_date','available_cnt'],inplace=True,axis=1)
		offerpd['insert_date'] = pd.to_datetime(offerpd['insert_datetime']).dt.date
		offerpd['insert_time'] = pd.to_datetime(offerpd['insert_datetime']).dt.time
		offerpd.currency_id = offerpd.currency_id.astype(float)
		offerpd.insert_date = pd.to_datetime(offerpd.insert_date)

		# skip 1 row in lst_currency, rename first column, fix datatypes
		currpd = currpd[1:]
		ncol = currpd.columns.values
		ncol[0] = 'currency_id'
		currpd.columns = ncol
		currpd.currency_id = currpd.currency_id.astype(float)

		# rename columns, remove records to keep only to USD entries, fix datatypes
		ncol = ratespd.columns.values
		ncol[1] = 'currency_id'
		ncol[3] = 'insert_date'
		ratespd.columns = ncol
		ratespd = ratespd[ratespd['scnd_currency_id'] == 1]
		ratespd.currency_id = ratespd.currency_id.astype(float)
		ratespd.insert_date = pd.to_datetime(ratespd.insert_date)

		# merge dataframes with inner join to assure only valid records
		mpd = pd.merge(offerpd,ratespd,how='inner',on=['insert_date','currency_id'])
		mpd = pd.merge(mpd,currpd,how='left',on=['currency_id'])

		# calculate price in USD
		mpd['price_usd'] = mpd['sellings_price'] * mpd['currency_rate']
		mpd.drop(['currency_id','insert_datetime','insert_time','scnd_currency_id','currency_rate','name'],inplace=True,axis=1)
		mpd = DataFrame(mpd, columns=['id_x', 'hotel_id','price_usd', 'sellings_price', 'code', 'breakfast_included_flag','offer_valid_from','offer_valid_to'])

		# match columns names
		ncol = mpd.columns.values
		ncol = ['offer_id','hotel_id','price_usd','original_price','original_currency_code','breakfast_included_flag','valid_from_date','valid_to_date']
		mpd.columns = ncol

		# fix datatypes
		mpd.offer_id = mpd.offer_id.astype(int)
		mpd.hotel_id = mpd.hotel_id.astype(int)
		# create schema bi_date
		sql = "CREATE DATABASE IF NOT EXISTS bi_date; USE bi_date;"
		session.query(sql) 																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																													
		try:
			mpd.to_sql('valid_offer', engine, if_exists='append')
			print mpd.head(4)
			session.commit()
   		finally:
   			session.rollback()
		print datetime.now() - startTime
		connection.close()


if __name__ == '__main__':
	a = HQ_etl()
