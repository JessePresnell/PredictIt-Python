from lxml import html
import requests
import json, re
import urllib2
import psycopg2
from datetime import datetime
import sys
import codecs
import pytz
import sqlalchemy as sa
import MySQLdb
from bs4 import BeautifulSoup


eastern = pytz.timezone('US/Eastern')

def connect_local():
  conn = MySQLdb.connect(host='', user='', passwd='',  db='')
  cur = conn.cursor()
  cur.execute("create table if not exists contracts (id integer primary key AUTO_INCREMENT, ticker_id varchar(255) NOT NULL, contract_ticker_symbol varchar(255), contract_name varchar(255), contract_date_start varchar(255), contract_date_end varchar(255))")
  cur.execute("create table if not exists tickers (ticker_id integer primary key, ticker_symbol varchar(255) NOT NULL unique, ticker_name varchar(255), ticker_short_name varchar(255), ticker_image varchar(255), ticker_timestamp varchar(255), ticker_status varchar(255));")
  cur.execute("create table if not exists contract_data (id INTEGER primary key AUTO_INCREMENT, contract_ticker_symbol varchar(255), ticker_timestamp varchar(255), contract_traded INTEGER,contract_today_volume INTEGER,contract_total_shares INTEGER,contract_todays_change varchar(20))")
  cur.execute("create table if not exists contract_offers (ticker_id integer, contract_ticker_symbol varchar(255), contract_short_name varchar(255), contract_last_trade_price REAL, contract_best_buy_yes REAL, contract_best_buy_no REAL, contract_best_sell_yes REAL, contract_best_sell_no REAL, contract_last_close_price REAL, ticker_timestamp varchar(255))")
  return cur, conn

def get_tickers():
	page = re=json.load(urllib2.urlopen('https://www.predictit.org/api/marketdata/all/'))
	market_data=page['Markets']
	return market_data

def get_all_data(l):
	for x in l:
		ticker_name=x['Name'].replace(u"\u2018", "'").replace(u"\u2019", "'")
		ticker_short_name=x['ShortName'].replace(u"\u2018", "'").replace(u"\u2019", "'")
		ticker_symbol=x['TickerSymbol']
		ticker_image=x['Image']
		ticker_timestamp=x['TimeStamp']
		ticker_status=x['Status']
		ticker_id=x['ID']
		cur.execute("INSERT IGNORE INTO tickers (ticker_name, ticker_short_name, ticker_symbol, ticker_image, ticker_timestamp, ticker_status, ticker_id) values (%s, %s, %s, %s, %s, %s, %s)",(ticker_name, ticker_short_name, ticker_symbol, ticker_image, ticker_timestamp, ticker_status, ticker_id,))
		for c in x['Contracts']:
			contract_id=c['ID']
			contract_status=c['Status']
			contract_name=c['Name'].replace(u"\u2018", "'").replace(u"\u2019", "'")
			contract_url=c['URL']
			contract_last_trade_price=c['LastTradePrice']
			contract_best_buy_yes=c['BestBuyYesCost']
			contract_best_buy_no=c['BestBuyNoCost']
			contract_best_sell_yes=c['BestSellYesCost']
			contract_best_sell_no=c['BestSellNoCost']
			contract_short_name=c['ShortName'].replace(u"\u2018", "'").replace(u"\u2019", "'")
			contract_ticker_symbol=c['TickerSymbol']
			contract_last_close_price=c['LastClosePrice']
			try:
				response=urllib2.urlopen('https://www.predictit.org/Ticker/'+c['TickerSymbol'])
				html=response.read()
				soup=BeautifulSoup(html,'lxml')
				contract_date_start=soup.find('td',text=re.compile("Start Date:")).next_sibling.next_sibling.string
				contract_traded=soup.find('td',text=re.compile("Shares Traded:")).next_sibling.next_sibling.string.replace(",", "")
				contract_today_volume=soup.find('td',text=re.compile("Today's Volume:")).next_sibling.next_sibling.string.replace(",", "")
				contract_total_shares=soup.find('td',text=re.compile("Total Shares:")).next_sibling.next_sibling.string.replace(",", "")
				contract_todays_change=soup.find('td',text=re.compile("Today's Change:")).next_sibling.next_sibling.string.replace("+", "")
				cur.execute("INSERT INTO contract_data (contract_ticker_symbol, ticker_timestamp, contract_traded, contract_today_volume, contract_total_shares, contract_todays_change) values (%s, %s, %s, %s, %s, %s)",(contract_ticker_symbol, ticker_timestamp, contract_traded, contract_today_volume, contract_total_shares, contract_todays_change,))
				if contract_todays_change=='NC':
				  contract_todays_change=0.0
				if c['DateEnd']!="N/A":
					cur.execute("insert into contracts(ticker_id, contract_ticker_symbol, contract_name, contract_date_start,contract_date_end) values (%s, %s, %s, %s, %s))",(ticker_id, contract_ticker_symbol, contract_name, contract_date_start,contract_date_end,))
				else:
					cur.execute("insert into contracts(ticker_id, contract_ticker_symbol, contract_name, contract_date_start) values (%s, %s, %s, %s)",(ticker_id, contract_ticker_symbol, contract_name, contract_date_start,))
			except:
				try:
					cur.execute("insert into contracts(ticker_id, contract_ticker_symbol, contract_name, contract_date_end) values (%s, %s, %s, %s))",(ticker_id, contract_ticker_symbol, contract_name,contract_date_end,))
				except:
					cur.execute("insert into contracts(ticker_id, contract_ticker_symbol, contract_name) values (%s, %s, %s)",(ticker_id, contract_ticker_symbol, contract_name,))
				pass
			cur.execute("INSERT INTO contract_offers (ticker_id, contract_ticker_symbol, contract_short_name, contract_last_trade_price, contract_best_buy_yes, contract_best_buy_no, contract_best_sell_yes, contract_best_sell_no, contract_last_close_price, ticker_timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (ticker_id, contract_ticker_symbol, contract_short_name, contract_last_trade_price, contract_best_buy_yes, contract_best_buy_no, contract_best_sell_yes, contract_best_sell_no, contract_last_close_price, ticker_timestamp, ))
			conn.commit()
cur, conn=connect_local()
x=get_tickers()
get_all_data(x)
print "Data Uploaded"
