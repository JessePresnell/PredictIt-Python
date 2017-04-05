from lxml import html
import requests
import json
import urllib2
import psycopg2
from datetime import datetime
import sys
import codecs
import pytz
eastern = pytz.timezone('US/Eastern')
def connect_local():
	conn = psycopg2.connect(database="political", user="postgres", host="localhost", password="INSERT PASSWORD HERE")
	print "Connected to Database"
	cur = conn.cursor()
	return cur, conn
def get_tickers():
	page = requests.get('https://www.predictit.org/PublicData/_Ticker')
	tree = html.fromstring(page.content)
	x= tree.xpath('//*[@id="newsticker_1"]/li/a/div/b/text()')
	page2 = requests.get('https://www.predictit.org/PublicData/_Ticker/1')
	tree2 = html.fromstring(page2.content)
	y= tree2.xpath('//*[@id="newsticker_1"]/li/a/div/b/text()')
	for i in y:
		x.append(i)
	return x
def get_all_data(x):
	for b in x:
		re=json.load(urllib2.urlopen('https://www.predictit.org/api/marketdata/ticker/'+b))
		status=re['Status']
		name=re['Name']
		timestamp=re['TimeStamp']
		for c in re['Contracts']:
			if c['DateEnd']!="N/A":
				contract_id=c['ID']
				contract_status=c['Status']
				contract_name=c['Name'].replace(u"\u2018", "'").replace(u"\u2019", "'")
				contract_url=c['URL']
				contract_last_trade_price=c['LastTradePrice']
				contract_best_buy_yes_cost=c['BestBuyYesCost']
				contract_best_buy_no_cost=c['BestBuyNoCost']
				contract_best_sell_yes_cost=c['BestSellYesCost']
				contract_best_sell_no_cost=c['BestSellNoCost']
				contract_date_end=c['DateEnd'].decode("utf-8").replace(u"\u2018", "'").replace(u"\u2019", "'").replace(u"p.m.","").replace(u"a.m.","")
				contract_short_name=c['ShortName'].replace(u"\u2018", "'").replace(u"\u2019", "'")
				contract_ticker_symbol=c['TickerSymbol']
				contract_last_close_price=c['LastClosePrice']
				cur.execute("INSERT INTO predictit (status, name, url, last_trade_price, best_buy_yes, best_buy_no, best_sell_yes, best_sell_no, date_end, id, short_name, ticker_symbol, last_close_price, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now());", (contract_status, contract_name, contract_url, contract_last_trade_price, contract_best_buy_yes_cost, contract_best_buy_no_cost, contract_best_sell_yes_cost, contract_best_sell_no_cost, contract_date_end, contract_id, contract_short_name, contract_ticker_symbol, contract_last_close_price, ))
			else:
				contract_id=c['ID']
				contract_status=c['Status']
				contract_name=c['Name'].replace(u"\u2018", "'").replace(u"\u2019", "'")
				contract_url=c['URL']
				contract_last_trade_price=c['LastTradePrice']
				contract_best_buy_yes_cost=c['BestBuyYesCost']
				contract_best_buy_no_cost=c['BestBuyNoCost']
				contract_best_sell_yes_cost=c['BestSellYesCost']
				contract_best_sell_no_cost=c['BestSellNoCost']
				contract_short_name=c['ShortName'].replace(u"\u2018", "'").replace(u"\u2019", "'")
				contract_ticker_symbol=c['TickerSymbol']
				contract_last_close_price=c['LastClosePrice']
				cur.execute("INSERT INTO predictit (status, name, url, last_trade_price, best_buy_yes, best_buy_no, best_sell_yes, best_sell_no, id, short_name, ticker_symbol, last_close_price, date_time) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now() );", (contract_status, contract_name, contract_url, contract_last_trade_price, contract_best_buy_yes_cost, contract_best_buy_no_cost, contract_best_sell_yes_cost, contract_best_sell_no_cost, contract_id, contract_short_name, contract_ticker_symbol, contract_last_close_price, ))
			conn.commit()
cur, conn=connect_local()
x=get_tickers()
get_all_data(x)
print "Data Uploaded"
