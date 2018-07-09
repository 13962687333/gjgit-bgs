# encoding: UTF-8

"""
导入MC导出的CSV历史数据到MongoDB中
"""

"""
本模块中主要包含：
1. 将MultiCharts导出的历史数据载入到MongoDB中用的函数
2. 将通达信导出的历史数据载入到MongoDB中的函数
3. 将交易开拓者导出的历史数据载入到MongoDB中的函数
4. 将OKEX下载的历史数据载入到MongoDB中的函数
"""

import csv
from datetime import datetime, timedelta
#from time import time
import time

import pymongo
import pandas as pd
import tushare as ts
from vnpy.trader.vtGlobal import globalSetting
from vnpy.trader.vtConstant import *
from vnpy.trader.vtObject import VtBarData
#from .ctaBase import SETTING_DB_NAME, TICK_DB_NAME, MINUTE_DB_NAME, DAILY_DB_NAME

#from vnpy.trader.app.ctaStrategy.ctaBase import MINUTE_DB_NAME

#----------------------------------------------------------------------
def downloadEquityDailyBarts(symbol,date,itype):
    """
    下载股票的1分钟行情，symbol是股票代码
    """
    #print u'开始下载%s日行情' %symbol
    
    connd = pymongo.MongoClient('localhost',27017)
    if itype=='D':
        db= connd['VnTrader_Daily_Db']
        cl_gj=db[symbol]
        print (u'开始下载%s日行情') %symbol
    elif itype=='5min':
        db=connd['VnTrader_5Min_Db']
        cl_gj=db[symbol]
        print u'开始下载%s,5分钟行情' %symbol
    elif itype=='1min':
        db=connd['VnTrader_1Min_Db']
        cl_gj=db[symbol]
        date=date[:4]+'-'+date[4:6]+'-'+date[6:]
        print u'开始下载%s,1分钟行情' %symbol
        
       
    # 查询数据库中已有数据的最后日期
    #cl = self.dbClient[DAILY_DB_NAME][symbol]
    cx = cl_gj.find(sort=[('datetime', pymongo.DESCENDING)])
    if cx.count():
        last = cx[0]
    else:
        last = ''
    # 开始下载数据
    #import tushare as ts
    
    if last:
        start = last['date'][:4]+'-'+last['date'][4:6]+'-'+last['date'][6:]
    else:
        start=date
        
    conns = ts.get_apis()
    strat_time=time.strftime("%Y-%m-%d %H:%M:%S")
    data = ts.bar(symbol,conn = conns, start_date = start,end_date='',freq = itype,adj='qfq',retry_count=1000)
  
    
    if isinstance(data,pd.core.frame.DataFrame):
        if not data.empty:
        # 创建datetime索引
        #self.dbClient[DAILY_DB_NAME][symbol].ensure_index([('datetime', pymongo.ASCENDING)], 
        #                                                    unique=True)                
            db[symbol].ensure_index([('datetime', pymongo.ASCENDING)],unique=True)    
            for index, d in data.iterrows():
                bar = VtBarData()
                bar.vtSymbol = symbol
                bar.symbol = symbol
                try:
                    bar.open = d.get('open')
                    bar.high = d.get('high')
                    bar.low = d.get('low')
                    bar.close = d.get('close')
                    bar.date = str(index).replace('-', '')
                    bar.time = bar.date[9:]+':00'  #为5分钟行情增加
                    bar.datetime = datetime.strptime(bar.date, '%Y%m%d %H:%M:%S')
                    #不加' %H:%M'时提示：ValueError: unconverted data remains: 14:55。' %H:%M'为5分钟行情增加
                    bar.date=bar.date[:8]  #为5分钟行情增加
                    bar.volume = d.get('vol')
                except KeyError:
                    print d
            
                    flt = {'datetime': bar.datetime}
                    #self.dbClient[DAILY_DB_NAME][symbol].update_one(flt, {'$set':bar.__dict__}, upsert=True)            
                    db[symbol].update_one(flt, {'$set':bar.__dict__}, upsert=True) 
            print u'%s下载完成' %symbol
            with open ('.\gj1mingetbar_ok', mode='a') as f:
                f.write('%s： %s 下载完成, 已下载%d\n' %(strat_time,symbol,i))
        else:
            print u'找不到合约%s' %symbol
    else:
        print u'下载%s失败' %symbol
        with open ('.\gj1mingetbar_err', mode='a') as f1:
            f1.write('%s: %s 下载失败, 已下载%d\n' %(strat_time,symbol,i ))

#df=pd.read_csv('e:/gupiao/get_stock_basics.csv',encoding='GBK') 
df=ts.get_stock_basics()
'''
df=pd.read_csv('f:/gupiao/get_stock_basics.csv',encoding='GBK') 
codes=df['code'].tolist()#将代码列转换成列表，成为迭代
'''
codes=df.index        #等效于上面三行

dates=df['timeToMarket'].tolist()


for itype in ['1min','5min','D']:
    for i in range(len(codes)):
        code=str(codes[i])
        date=str(dates[i])
        if len(str(codes[i]))<6:
            code='0'*(6-len(str(codes[i])))+str(codes[i])
     
        downloadEquityDailyBarts(code,date,itype)
        print u'已下载%d户' %i
