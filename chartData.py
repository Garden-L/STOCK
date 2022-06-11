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
from db import data 

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
    save = data('root','root', 'localhost', 'my_stock')
    code = stock.get_market_ticker_list(datetime.now().strftime('%Y-%m-%d'), 'KOSPI')
    time.sleep(4)
    code += stock.get_market_ticker_list(datetime.now().strftime('%Y-%m-%d'), 'KOSDAQ')

    for d in code:
        df = get_chartdata_days('20170101','20220511', d)
        try:
            save.df_replace_save(df, 'ohlcv')
        except :
            print("에러")
            pass
        time.sleep(15)
