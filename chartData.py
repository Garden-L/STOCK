from multiprocessing.sharedctypes import synchronized
from pykrx import stock
from pykrx import bond
import requests
import datetime
import json
import sqlalchemy
from sqlalchemy import create_engine
import pymysql 
from datetime import datetime, timedelta
import time
from pykrx import stock
import pandas as pd
import numpy as np
import sqlalchemy as db


class datesave:
    def __init__(self):
        engine = db.create_engine('mysql+pymysql://root:root@localhost/a')
        pass
    
    def get_market_ohlcv(self, now=datetime.now()):
        df = stock.get_market_ohlcv(now.replace('-',''), market='ALL')
        df.columns = ['code', 'open', 'high', 'low', 'close', 'volume', 'value', 'rate', 'marketcap', 'shares']
        df['date'] = now
        return df
    
    def df_save(self, df, user,password, server, db, table):
        engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(user, password, server, db))
        df.to_sql(name=table, con=engine, if_exists='append', index=False)
        self.engine.dispose()
    
    def date_range(self, start, end):
        start = datetime.strptime(start, "%Y-%m-%d")
        end = datetime.strptime(end, "%Y-%m-%d")
        dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end-start).days+1)]
        return dates
        
    def df_replace_save(self, df : pd.DataFrame, user, password, server, db, table):
        for idx in range(len(df)):
            columns = ','.join(df.columns)
            values = ["'"+str(x)+"'" for x in df.iloc[idx]]
            sql = 'replace into {2}({0}) values({1})'.format(columns, ','.join(values), table)            
            self.engine.execute(sql)

OPEN = 'OPEN_PRICE'
CLOSE = 'CLOSE_PRICE'
HIGH = 'HIGH_PRICE'
LOW = 'LOW_PRICE'
TRDVOL = 'TRDVOL'
MKCAP = 'MARKETCAP'
SHARES = 'SHARES'
TRDVAL = 'TRDVAL'
FLUCRT = 'FLUCRT'
DATE = 'DATE'
STKCODE ='STKCODE'

def get_chartdata_days(start:str, end : str, stkCode):
    '''
    일봉 차트 데이터
    parame
    index = [시가, 고가, 저가, 종가, 거래량, 거래대금, 시가총액]
    '''
    
    df_naver = stock.get_market_ohlcv(start, end, stkCode, )
    df_krx = stock.get_market_ohlcv(start, end, stkCode, adjusted=False)
    
    if(not df_naver.empty and not df_krx.empty):        
        df = pd.merge(left=df_naver, right=df_krx, how='inner', on='날짜', suffixes=['_naver', '_krx'])
        
        df['주식수'] = np.int64((df['시가총액'] / df['종가_naver']))
        df['거래량'] = np.int64((df['종가_krx']/df['종가_naver']) * df['거래량_naver'])
        df = df[['시가_naver','고가_naver', '저가_naver', '종가_naver','거래대금', '등락률', '시가총액', '거래량', '주식수']]
        df['종목코드'] = stkCode
        df['날짜'] = df.index
        
        df.reset_index(drop=True, inplace=True)
        
        df.rename(columns={'시가_naver' : OPEN,
                            '고가_naver' : HIGH,
                            '저가_naver' : LOW,
                            '종가_naver' : CLOSE,
                            '거래량' : TRDVOL,
                            '시가총액' : MKCAP,
                            '주식수' : SHARES,
                            '거래대금' : TRDVAL,
                            '등락률' : FLUCRT,
                            '날짜' : DATE,
                            '종목코드' : STKCODE,
                        }, inplace=True)
        
        return df
    
    return df.DataFrame()
        
        
if __name__ =='__main__':
    code = stock.get_market_ticker_list(datetime.now().strftime('%Y-%m-%d'), 'KOSPI')
    time.sleep(4)
    code += stock.get_market_ticker_list(datetime.now().strftime('%Y-%m-%d'), 'KOSDAQ')
    # saved = code.copy()
    # a = datesave()
    # cnt= 0
    # # for d in code:
    # #     cnt +=1
    # #     print(cnt)
    # #     df = get_chartdata_days('20170101','20220511', d)
    # #     print(df)
    # #     try:
    # #         a.df_save(df, 'root', 'root', 'localhost', 'stock', 'stock')
    # #         saved.pop(saved.index(d))
    # #     except :
    # #         print('에러')
    # #         print(saved)
    # #         pass
    # #     time.sleep(15)

    # from FnGuide import fnguide
    
    # Fn = fnguide()
    # for d in code:
    #     print(d)
    #     cnt +=1
    #     print(cnt)

    #     try:
    #         df = Fn.get_Finance(d, 'D')           
    #         print('에러확인')
    #         a.df_save(df[0], 'root', 'root', 'localhost', 'stock', 'sonik')
    #         a.df_save(df[1], 'root', 'root', 'localhost', 'stock', 'deacha')
    #         a.df_save(df[2], 'root', 'root', 'localhost', 'stock', 'cash')
    #     except :
    #         print('에러')
    #         pass
    #     time.sleep(10)
        
    t = datesave()
    t.df_replace_save(df, 'root', 'root', 'localhost', 'a', 'b')
    
