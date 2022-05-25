from inspect import getcomments
import sqlalchemy as db
import pandas as pd
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import relativedelta
engine = db.create_engine('mysql+pymysql://root:root@localhost/stock')
def getCompanyInfo(date):
    sql = '''
            select O.STKCODE, O.DATE, O.CLOSE_PRICE, O.TRDVOL, O.TRDVAL, O.MARKETCAP, O.SHARES,
                    D.DATE AS REPORT_DATE, D.REPORT, D.Assets_Total, D.Current_Fin_Assets, D.Cash_and_Cash_Equivalents, D.Current_Liab_Total, D.Current_Liab_Total, D.Current_Payables, D.Current_Emp_Benefits, D.LT_Liab_Total, D.LT_Payables, D.LT_Emp_Benefits,  D.Equity_Total, D.Controlling_Equity_Total,
                    S.REV, S.GROSS, S.OPR, S.SGA_2, S.NETINC, S.NETINC_1,
                    C.Add_Exp_WO_CF_Out_5, C.Add_Exp_WO_CF_Out_6
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

#완료
def getEPS(df:pd.DataFrame) -> pd.DataFrame:
    df['EPS'] = df['NETINC'] / df['SHARES']
    
    return df

#완료
def getPER(df:pd.DataFrame) -> pd.DataFrame:
    df['PER'] = df['MARKETCAP'] / df['NETINC'] 
    
    return df

#현금흐름
def getCF(df:pd.DataFrame) -> pd.DataFrame:
    df['CF'] = df['NETINC_1'] + df['SGA_2'] +df['Add_Exp_WO_CF_Out_5']
    
    return df

#주당현금흐름 
def getCFPS(df:pd.DataFrame) -> pd.DataFrame:
    df = getCF(df)
    df['CFPS'] = df['CF'] / df['SHARES']
    
    return df

#완성
def getPCR(df:pd.DataFrame) -> pd.DataFrame:
    df = getCFPS(df)
    df['PCR'] = df['MARKETCAP'] / df['CF']
    
    return df

#완성
def getEBITDA(df:pd.DataFrame) -> pd.DataFrame:
    df['EBITDA'] = df['OPR'] + df['SGA_2'] + df['Add_Exp_WO_CF_Out_5']
    
    return df

#순차입부채
def getNetDebt(df:pd.DataFrame) -> pd.DataFrame:
    df['NetDebt'] = ( df['Current_Liab_Total'] - (df['Current_Payables'] + df['Current_Emp_Benefits']) + df['LT_Liab_Total'] - (df['LT_Payables'] + df['LT_Emp_Benefits'])) - (df['Current_Fin_Assets'] + df['Cash_and_Cash_Equivalents'] )
    
    return df

def getEV(df:pd.DataFrame) -> pd.DataFrame:
    df = getNetDebt(df)
    df['EV'] = df['MARKETCAP'] + df['NetDebt']
    
    return df

#완료
def getPBR(df:pd.DataFrame) -> pd.DataFrame:
    df['PBR'] = df['MARKETCAP'] / df['Equity_Total']
    
    return df

#완료
def getPSR(df:pd.DataFrame) -> pd.DataFrame:
    #실질 매출액
    df['PSR'] = df['MARKETCAP'] / df['REV']
    
    return df

#완료
def getROA(df:pd.DataFrame) -> pd.DataFrame:
    #%
    df['ROA'] = df['NETINC'] / df['Assets_Total'] *100
    
    return df

def getEBITDA(df:pd.DataFrame) -> pd.DataFrame:
    df['EBITDA'] = df['OPR'] + df['Add_Exp_WO_CF_Out_5'] + df['Add_Exp_WO_CF_Out_6']
    
    return df

def getEV_EBITDA(df:pd.DataFrame) -> pd.DataFrame:
    df = getEV(df)
    df = getEBITDA(df)
    df['EV/EBITDA'] = df['EV'] / df['EBITDA']
    
    return df

def getEV_SALES(df:pd.DataFrame) -> pd.DataFrame:
    df = getEV(df)
    df['EV/SALES'] = df['EV'] / df['rev']
    
    return df

def getBPS(df:pd.DataFrame) -> pd.DataFrame:
    df['BPS'] = df['Controlling_Equity_Total'] / df['SHARES']
    
    return df

def getROE(df:pd.DataFrame) -> pd.DataFrame:
    df['ROE'] = df['NETINC_1'] / df['Controlling_Equity_Total'] * 100
    
    return df

def getGPA(df:pd.DataFrame) -> pd.DataFrame:
    df['GPA'] = df['GROSS'] / df['Assets_Total']
    
    return df
def getPEG(df:pd.DataFrame) -> pd.DataFrame:
    pass

#부채비율
def getLiabRatio(df:pd.DataFrame) -> pd.DataFrame:
    df['LiabRatio'] = df['Current_Liab_Total'] / df['Equity_Total'] *100
    
    return df

#순차입금비율
def getNetDebtRatio(df:pd.DataFrame) -> pd.DataFrame:
    df = getNetDebt(df)
    df['NetDebtRatio'] = df['NetDebt'] / df['Equity_Total'] *100
    
    return df

def getGrossMargin(df:pd.DataFrame) -> pd.DataFrame:
    df['GrossMargin'] = df['Gross'] / df['REV'] 
    
    return df

def getOperatingMargin(df:pd.DataFrame) -> pd.DataFrame:
    df['OperatingMargin'] = df['OPR'] / df['REV'] 
    
    return df

def getProfitMargin(df:pd.DataFrame) -> pd.DataFrame:
    df['ProfitMargin'] = df['NETINC'] / df['REV'] 
    
    return df

def getGrawth(df, prevYear:int):
    
    date = df['DATE'][0]
    print(dt.now() - relativedelta(years=1))
    sql = '''
            select O.STKCODE, O.DATE, O.CLOSE_PRICE, O.TRDVOL, O.TRDVAL, O.MARKETCAP, O.SHARES,
                    D.REPORT, D.Assets_Total, D.Current_Fin_Assets, D.Cash_and_Cash_Equivalents, D.Current_Liab_Total, D.Current_Liab_Total, D.Current_Payables, D.Current_Emp_Benefits, D.LT_Liab_Total, D.LT_Payables, D.LT_Emp_Benefits,  D.Equity_Total, D.Controlling_Equity_Total,
                    S.REV, S.GROSS, S.OPR, S.SGA_2, S.NETINC, S.NETINC_1,
                    C.Add_Exp_WO_CF_Out_5, C.Add_Exp_WO_CF_Out_6
                FROM OHLCV O 
                INNER JOIN deacha D 
                        ON O.stkcode= D.stkcode AND {} = D.date
                INNER JOIN CASH C
                        ON O.stkcode= C.stkcode AND {} = C.date
                INNER JOIN SONIK S
                        ON O.stkcode= S.stkcode AND {} = S.date
                WHERE O.DATE = DATE('{}');
        '''\
        .format({'a' : dt.strftime, 'prevYear': prevYear})
    pass
    
df = getCompanyInfo('2022-05-10')
#df = getROE(df)

print (getGrawth(df, 4))