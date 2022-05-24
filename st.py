from inspect import getcomments
import sqlalchemy as db
import pandas as pd
engine = db.create_engine('mysql+pymysql://root:root@localhost/stock')
def getCompanyInfo(date):
    sql = '''
            select O.STKCODE, O.DATE, O.CLOSE_PRICE, O.TRDVOL, O.TRDVAL, O.MARKETCAP, O.SHARES,
                    D.REPORT, D.Assets_Total, D.Equity_Total, 
                    S.REV, S.OPR, S.NETINC
                FROM OHLCV O 
                INNER JOIN deacha D 
                        ON O.stkcode= D.stkcode AND date(concat(year(date_add(O.date,interval -1 year)), '-12-31')) = D.date
                INNER JOIN CASH C
                        ON O.stkcode= C.stkcode AND date(concat(year(date_add(O.date,interval -1 year)), '-12-31')) = C.date
                INNER JOIN SONIK S
                        ON O.stkcode= S.stkcode AND date(concat(year(date_add(O.date,interval -1 year)), '-12-31')) = S.date
                WHERE O.DATE = DATE('{}');
        '''\
        .format(date)

    ret = pd.read_sql(sql, engine.connect())
 
    return ret

def getEPS(df:pd.DataFrame) -> pd.DataFrame:
    df['EPS'] = df['NETINC'] / df['SHARES']
    
    return df

def getPER(df:pd.DataFrame) -> pd.DataFrame:
    df['PER'] = df['MARKETCAP'] / df['NETINC'] 
    
    return df

def getPCR(df:pd.DataFrame) -> pd.DataFrame:
    df['EPS'] = df['NETINC'] / df['SHARES']
    
    return df

def getPBR(df:pd.DataFrame) -> pd.DataFrame:
    df['PBR'] = df['MARKETCAP'] / df['Equity_Total']
    
    return df

#구현 미숙
def getPSR(df:pd.DataFrame) -> pd.DataFrame:
    #실질 매출액
    df['PSR'] = df['MARKETCAP'] / df['REV']
    
    return df

def getROA(df:pd.DataFrame) -> pd.DataFrame:
    #%
    df['ROA'] = df['NETINC'] / df['Assets_Total'] *100
    
    return df



df = getCompanyInfo('2022-05-10')
df = getROA(df)


print (df)