import pandas as pd
import numpy as np
import sqlalchemy as mysql
from datetime import datetime, timedelta
from pykrx import stock
import time

class datasave:
    def __init__(self, user, password, server, db):
        self.engine = mysql.create_engine(f'mysql+pymysql://{user}:{password}@{server}/{db}')
    
    def get_market_ohlcv(self, now=datetime.now()):
        df = stock.get_market_ohlcv(now.replace('-',''), market='ALL')
        df.columns = ['code', 'open', 'high', 'low', 'close', 'volume', 'value', 'rate', 'marketcap', 'shares']
        df['date'] = now
        return df
    
    def df_save(self, df, user,password, server, db, table):
        engine = db.create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8'.format(user, password, server, db))
        df.to_sql(name=table, con=engine, if_exists='append', index=False)
        self.engine.dispose()
    
    def date_range(self, start, end):
        start = datetime.strptime(start, "%Y-%m-%d")
        end = datetime.strptime(end, "%Y-%m-%d")
        dates = [(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end-start).days+1)]
        return dates
        
    def df_replace_save(self, df : pd.DataFrame,table):
        for idx in range(len(df)):
            columns = ','.join(df.columns)
            values = ["'"+str(x)+"'" for x in df.iloc[idx]]
            sql = 'replace into {2}({0}) values({1})'.format(columns, ','.join(values), table)
            try:
                self.engine.execute(sql)
            except:
                print('db에러')
            
