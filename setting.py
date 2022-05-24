import requests
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen, Request
import pandas as pd
import ticker
import numpy as np
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
            body['DATE'] = list_date[1:]
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
        
        return [dict_df['SonikY'], dict_df['DaechaY'], dict_df['CashY']]
    
    

if __name__ =='__main__':
    print(fnguide().get_Finance('005930','D'))