from inspect import getcomments
import numpy
import sqlalchemy as db
import pandas as pd
from datetime import datetime as dt, timedelta
from dateutil.relativedelta import relativedelta
from db import data as db

data = db('root','root', 'localhost', 'my_stock')

def getOHLCV(date):
    return data.getOHLCV(data)

def getCompanyInfo(date):
    return data.getCompanyInfo(date)

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

#주가현금흐름
def getPCR(df:pd.DataFrame) -> pd.DataFrame:
    df = getCFPS(df)
    df['PCR'] = df['MARKETCAP'] / df['CF']
    
    return df

#완성
def getEBIT(df:pd.DataFrame) -> pd.DataFrame:
    df['EBIT'] = df['NETINC'] + df['TAX'] - df['FININC'] +df['FINCOST']
    
    return df

#순차입부채
def getNetDebt(df:pd.DataFrame) -> pd.DataFrame:
    df['NetDebt'] = ( df['Current_Liab_Total'] - (df['Current_Payables'] + df['Current_Emp_Benefits']) + df['LT_Liab_Total'] - (df['LT_Payables'] + df['LT_Emp_Benefits'])) - (df['Current_Fin_Assets'] + df['Cash_and_Cash_Equivalents'] )
    
    return df

def getEV(df:pd.DataFrame) -> pd.DataFrame:
    df = getNetDebt(df)
    df['EV'] = df['MARKETCAP'] + df['NetDebt']
    
    return df

#주당순자산 (PBR이 1보다 낮으면 주당 장부가치가)
def getPBR(df:pd.DataFrame) -> pd.DataFrame:
    df['PBR'] = df['MARKETCAP'] / df['Equity_Total']
    
    return df

#주당매출액 ( 매출액보다 주가가 낮다면 1.0 미만)
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
    df['ROE'] = df['NETINC_1'] / df['Controlling_Equity_Total'] 
    
    return df

def getGPA(df:pd.DataFrame) -> pd.DataFrame:
    df['GPA'] = df['GROSS'] / df['Assets_Total']
    
    return df
def getPEG(df:pd.DataFrame) -> pd.DataFrame:
    pass

#부채비율
def getLiabRatio(df:pd.DataFrame) -> pd.DataFrame:
    df['LiabRatio'] = df['Current_Liab_Total'] / df['Equity_Total']
    
    return df

#순차입금비율
def getNetDebtRatio(df:pd.DataFrame) -> pd.DataFrame:
    df = getNetDebt(df)
    df['NetDebtRatio'] = df['NetDebt'] / df['Equity_Total'] *100
    
    return df

def getGrossMargin(df:pd.DataFrame) -> pd.DataFrame:
    df['GrossMargin'] = df['GROSS'] / df['REV'] 
    
    return df

def getOperatingMargin(df:pd.DataFrame) -> pd.DataFrame:
    df['OperatingMargin'] = df['OPR'] / df['REV'] 
    
    return df

def getProfitMargin(df:pd.DataFrame) -> pd.DataFrame:
    df['ProfitMargin'] = df['NETINC'] / df['REV'] 
    
    return df

    
#PEG
def getPEG(df, prevYear:int):
    df2 = data.getPrevCompanyInfo(df['DATE'][0], prevYear)
    getEPS(df)
    getEPS(df2)
    
    df2 = df2[['STKCODE','EPS']]
    
    df = df.merge(df2, how='inner', on="STKCODE", suffixes=("", "_prev"))
    df['EPS Growth'] = (df['EPS'] - df['EPS_prev']) / abs(df['EPS_prev']) *100
    df['PEG'] = (df['CLOSE_PRICE'] / df['EPS']) / df['EPS Growth']

    return df

#자산 성장률
def geTAG(df, prevYear:int):
    df2 = data.getPrevCompanyInfo(df['DATE'][0], prevYear)
    
    df2 = df2[['STKCODE','Assets_Total']]
    
    df = df.merge(df2, how='inner', on="STKCODE", suffixes=("", "_prev"))
    df['TAG'] = (df['Assets_Total'] - df['Assets_Total_prev']) / abs(df['Assets_Total_prev'])

    return df

#순이익 증가율
def geNIG(df, prevYear:int):
    df2 = data.getPrevCompanyInfo(df['DATE'][0], prevYear)
    
    df2 = df2[['STKCODE','Assets_Total']]
    
    df = df.merge(df2, how='inner', on="STKCODE", suffixes=("", "_prev"))
    df['NIG'] = (df['NETINC'] - df['NETINC_prev']) / abs(df['NETINC_prev'])

    return df

#매출 증가율
def getRG(df, prevYear:int):
    df2 = data.getPrevCompanyInfo(df['DATE'][0], prevYear)
    
    df2 = df2[['STKCODE','REV']]
    
    df = df.merge(df2, how='inner', on="STKCODE", suffixes=("", "_prev"))
    df['RG'] = (df['REV'] - df['REV_prev']) / abs(df['REV_prev'])

    return df

#영업 이익 증가율
def getOIG(df, prevYear:int):
    df2 = data.getPrevCompanyInfo(df['DATE'][0], prevYear)
    
    df2 = df2[['STKCODE','OPR']]
    
    df = df.merge(df2, how='inner', on="STKCODE", suffixes=("", "_prev"))
    df['OIG'] = (df['OPR'] - df['OPR_prev']) / abs(df['OPR_prev'])

    return df

#총자산 회전율
def getAssetTurnover(df):
    df['AssetTurnover'] = df['REV'] / df['Assets_Total']
    
    return df

#매출채권 회전율
def getReceivableTrunover(df):
    df['ReceivableTrunover'] = df['REV'] / df['Current_Receviables']
    
    return df

#재고자산 회전율
def getInventoryTurnover(df):
    df['InventoryTurnover'] = df['CGS'] / df['Current_Inventory']
    
    return df

def dfMerge(df1, df2, columns:list):
    columns.append('STKCODE')
    df2 = df2[columns]

    df = df1.merge(df2, how='inner', on="STKCODE", suffixes=("", "_prev"))

    return df
#유동비율
def getCurrentRatio(df):
    df['CurrentRatio'] = df['Current_Assets_Total'] / df['Current_Liab_Total']
    
    return df

#F_score
def getF_score(df, prevYear):
    getROA(df)
    getCurrentRatio(df)
    getLiabRatio(df)
    getGrossMargin(df)
    getAssetTurnover(df)
    
    df2 = data.getPrevCompanyInfo(df['DATE'][0], prevYear)
    getROA(df2)
    getLiabRatio(df2)
    getCurrentRatio(df2)
    getGrossMargin(df2)
    getAssetTurnover(df2)
    df = dfMerge(df, df2, ['ROA','GrossMargin','AssetTurnover', 'LiabRatio', 'CurrentRatio', 'SHARES'])
    
    df['F_ROA'] = 0
    df['F_ROA_dt']= 0
    df['F_CFO']= 0
    df['F_Accrual']= 0
    df['F_Liquid']= 0
    df['F_Lever']= 0
    df['F_Shares']= 0
    df['F_Margin']= 0
    df['F_Turn']= 0
    
    df['F_Score']= 0

    df.to_csv('./merge1.csv')
    df.loc[df['ROA'] >0, 'F_ROA'] = 1
    df.loc[df['ROA'] > df['ROA_prev'], 'F_ROA_dt'] = 1
    df.loc[df['CFO_Total']>0, 'F_CFO'] = 1
    df.loc[df['CFO_Total'] - df['NETINC'] > 0, 'F_Accrual'] = 1
    df.loc[df['CurrentRatio'] -df['CurrentRatio_prev']>0, 'F_Liquid'] = 1
    df.loc[df['LiabRatio'] - df['LiabRatio_prev']<0, 'F_Lever'] = 1
    df.loc[df['SHARES'] - df['SHARES_prev'] <=0, 'F_Shares'] = 1
    df.loc[df['GrossMargin'] - df['GrossMargin_prev'] >0, 'F_Margin'] = 1
    df.loc[df['AssetTurnover'] - df['AssetTurnover_prev']>0, 'F_Turn'] = 1
    df['F_Score'] = (df['F_ROA'] + df['F_ROA_dt'] + df['F_CFO']+df['F_Accrual']
                     +df['F_Liquid'] + df['F_Lever']+df['F_Shares']+ df['F_Margin']+df['F_Turn'])
    
    return df

#그린블라트
def getGreenBlatt(df):
    getPER(df)
    getEBIT(df)
    
    df['GreenBlatt'] = df['EBIT'] / (df['Assets_Total']-df['Current_Liab_Total'])

    return df


def df_filter(df, column, min=-numpy.inf, max=numpy.inf, n=None, asc=True):
    df = df.replace([numpy.inf, -numpy.inf], numpy.nan)
    df = df.query(('{b} >= {f} and {b} <={c}').format(b=column, f = min, c = max))
    df = df.sort_values(by=column, ascending=asc)
    df = df.head(n)
    
    return df[['STKCODE', column]]

def df_combine(*args, how='and'):
    how_dict = {'and':'inner', 'or':'outer'}
    df = args[0]
    for other in args[1:]:
        df = df.join(other.set_index('STKCODE'), on='STKCODE')
    df.dropna(inplace=True)
    return df

def getRIM(df, rate):
    getBPS(df)
    getROE(df)
    
    df['RIM'] = df['BPS'] * df['ROE'] / rate
    
    return df


if __name__ == '__main__':
    df = getCompanyInfo('2022-05-10')
    df= getPER(df)
    
    df = filter(df, 'PER', 0, 12, 30, True)
    print(df)
# df.to_csv('./rim.csv')

# df =getPER(df)
# df=getF_score(df,2)
# df= getGreenBlatt(df)
# print(df_filter(df, ['PER', 'F_Score', 'GreenBlatt'], [[6, 12],[3, 6],['asc', True]]).to_csv("./종목추출.csv"))
