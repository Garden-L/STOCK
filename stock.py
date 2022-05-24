import requests
import json
import pandas as pd
import numpy as np
import ast

def post(url, headers, body):
    return requests.post(url = url, headers=headers, data=body)

def naver_get_ohlc_days(stkCode, requestType, startDate, endDate, timeFrame):
    url='https://api.finance.naver.com/siseJson.naver?'
    headers={
        'user_agent' : 'Mozilla/5.0'
    }
    body ={
        'symbol': stkCode,
        'requestType': requestType,
        'startTime': startDate,
        'endTime': endDate,
        'timeframe': timeFrame,
    }
    
    data = post(url, headers, body).text.strip()
    data = ast.literal_eval(data) 
    
    return pd.DataFrame(data=data[1:], columns=data[0])
    
def krx_get_companyInfo():
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd' 
    
    headers={
        'user_agent' : 'Mozilla/5.0'
    }
    
    body={
        'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901',
        'locale': 'ko_KR',
        'mktId': 'ALL',
        'share': '1',
        'csvxls_isNo': 'false',
    }
    
    data = post(url, headers, body).content
    
    data = json.loads(data)
    df = pd.DataFrame(data['OutBlock_1'])
    
    return df
    
def krx_get_ohlc_days(stkCode, requestType, startDate, endDate):
    url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
#     body ={
#         'bld': 'dbms/MDC/STAT/standard/MDCSTAT01701',
#         'locale': 'ko_KR',
#         'tboxisuCd_finder_stkisu0_0': 095570/AJ네트웍스
# isuCd: KR7095570008
# isuCd2: KR7005930003
# codeNmisuCd_finder_stkisu0_0: AJ네트웍스
# param1isuCd_finder_stkisu0_0: ALL
# strtDd: 20220504
# endDd: 20220513
# share: 1
# money: 1
# csvxls_isNo: false
#     }

print(naver_get_ohlc_days('005930', '1', '20210101', '20220101', 'day'))