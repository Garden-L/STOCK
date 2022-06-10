from datetime import datetime
import requests
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
import pandas as pd
import ticker
import numpy as np
import calendar
import pandas as pd
import numpy as np
import sqlalchemy as db
from datetime import datetime, timedelta
from pykrx import stock
import time
from db import data
class url:
    def __init__(self, host='', path='', query='') -> str:
        self.setURL(host, path, query)
    
    def setURL(self, host, path, query):
        self.host = host
        self.path = path
        self.query = query
        
        self.url = host+path+query
        
        return self.url
    
    def setHost(self, host):
        return self.setURL(host, '', '')
    
    def setPath(self, path):
        return self.setURL(self.host, path, '')
    
    def setQuery(self, qeury):
        return self.setURL(self.host, self.path, '')
    
    def request(self):
        return Request(self.url)
    


class fnguide(url):
    host = 'https://asp01.fnguide.com/'
    def __init__(self):
        super().__init__(self.host)
    
    def request(self, spreadButton=True):
        req = super().request()
        html = urlopen(req).read()
        soup = bs(html, 'html.parser')

        return soup

    def tableTodf(self, html, stkCode):
        div = html.find_all('div', {'class':'um_table'})
        dict_df = {}
        for _div in div:
            tablename = _div['id'].replace('div', '') #테이블이름
            list_date = [i.get_text().replace('/','-') for i in _div.find('thead').find_all('th')]
            body = {}
            column = 0
            body['STKCODE'] = [stkCode for i in range(len(list_date)-1)]
            body['DATE'] =[]
            for i in list_date[1:]:
                try:
                    date = datetime(year=int(i[0:4]), month=int(i[5:]), day = 1).date()
                    date = date.replace(day= calendar.monthrange(date.year, date.month)[1])
                    body['DATE'].append(date)
                except:
                    body['DATE'].append(i)
            
            body['REPORT'] = [tablename[-1] for i in range(len(list_date)-1)]
            for trtag in _div.find('tbody').find_all('tr'):
                values=[]
                for tdtag in trtag.find_all('td'):
                    value = 0
                    if tdtag.has_attr('title') :
                        val = tdtag['title'].replace(',', '')
                        if val:
                            val = float(val)
                            value = np.int64(val * 100000000)
                    
                    values.append(value)
                body[column] = values
                column += 1

                df = pd.DataFrame(data=body)
                dict_df[tablename] = df

        return dict_df

    def get_Finance(self, stkCode, reportDB):
        '''
        FnGuide
        재무제표 - 포괄손익계산서가져오기
        
        return datafame
        '''
        path = 'SVO2/ASP/SVD_Finance.asp'
        query = '?pGB=1&gicode=A{}&cID=&MenuYn=Y&ReportGB={}&NewMenuID=103&stkGb=701'.format(stkCode, reportDB)
        self.setURL(self.host, path, query)
        
        
        html = self.request(False)
        
        dict_df = self.tableTodf(html, stkCode)
        dict_df['SonikY'].drop([4,5], axis=0, inplace=True)
        
        dict_df['CashY'].columns = ticker.CASH.values()
        dict_df['DaechaY'].columns = ticker.DEACHA.values()
        dict_df['SonikY'].columns = ticker.SONIK.values()
        
        dict_df['CashQ'].columns = ticker.CASH.values()
        dict_df['DaechaQ'].columns = ticker.DEACHA.values()
        dict_df['SonikQ'].columns = ticker.SONIK.values()
        dict_df['SonikQ'].drop([4,5], axis=0, inplace=True)
        
        if dict_df['SonikY'].iloc[0,2] == 'Y':
            dict_df['SonikY'].drop(3, axis=0, inplace=True)  
            dict_df['DaechaY'].drop(3, axis=0, inplace=True)
            dict_df['CashY'].drop(3, axis=0, inplace=True)
        
        return {
                'SonikY' :dict_df['SonikY'], 
                'DaechaY':dict_df['DaechaY'], 
                'CashY':dict_df['CashY'],
                'SonikQ':dict_df['SonikQ'], 
                'DaechaQ':dict_df['DaechaQ'], 
                'CashQ':dict_df['CashQ']
                }

if __name__ =='__main__':
    fn = fnguide()

    save = data('root','root', 'localhost', 'my_stock')
    code = stock.get_market_ticker_list(datetime.now().strftime('%Y-%m-%d'), 'KOSPI')
    time.sleep(4)
    code += stock.get_market_ticker_list(datetime.now().strftime('%Y-%m-%d'), 'KOSDAQ')
    
    for i in code:
        print(i)
        try:
            df = fn.get_Finance(i, 'D')

            save.df_replace_save(df['SonikY'],'sonik')
            save.df_replace_save(df['SonikQ'],'sonik')
            save.df_replace_save(df['DaechaY'], 'daecha')
            save.df_replace_save(df['DaechaQ'], 'daecha')
            save.df_replace_save(df['CashY'], 'cash')
            save.df_replace_save(df['CashQ'], 'cash')
        except:
            print("에러")
        
        time.sleep(5)
