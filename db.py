import pandas as pd
import numpy as np
import sqlalchemy as mysql
from datetime import datetime, timedelta
from pykrx import stock
import time

class data:
    def __init__(self, user, password, server, db):
        self.engine = mysql.create_engine(f'mysql+pymysql://{user}:{password}@{server}/{db}')
    
        
    def df_replace_save(self, df : pd.DataFrame,table):
        for idx in range(len(df)):
            columns = ','.join(df.columns)
            values = ["'"+str(x)+"'" for x in df.iloc[idx]]
            sql = 'replace into {2}({0}) values({1})'.format(columns, ','.join(values), table)
            try:
                self.engine.execute(sql)
            except:
                print('db에러')
            
    def getOHLCV(self, date):
        sql = '''
            select * from ohlcv
            where date = date('{}')
        '''.format(date)
        
        df = pd.DataFrame = pd.read_sql(sql, self.engine.connect())
        
        return df
    
    def getCompanyInfo(self, date):
        sql = '''
                SELECT STKCODE, Assets_Total, Current_Inventory, Current_Fin_Assets, Cash_and_Cash_Equivalents, Current_Receviables,  Current_Liab_Total, Current_Payables, Current_Emp_Benefits, LT_Liab_Total, LT_Payables, LT_Emp_Benefits,  Equity_Total, Controlling_Equity_Total, Current_Assets_Total,
                        sum(REV) REV, sum(GROSS) GROSS, SUM(CGS) CGS,  SUM(OPR) OPR, SUM(SGA_2) SGA_2, SUM(NETINC) NETINC, SUM(NETINC_1) NETINC_1, SUM(FINCOST) FINCOST, SUM(FININC) FININC, sum(TAX) TAX,
                    sum(Add_Exp_WO_CF_Out_5) Add_Exp_WO_CF_Out_5, sum(Add_Exp_WO_CF_Out_6) Add_Exp_WO_CF_Out_6, sum(CFO_Total) CFO_Total, sum(Liabilities_Total) as Liabilities_Total
            FROM (
                    SELECT S.STKCODE,
                        D.DATE AS REPORT_DATE, D.REPORT, D.Assets_Total, D.Current_Inventory, D.Current_Fin_Assets, D.Cash_and_Cash_Equivalents, D.Current_Receviables, D.Liabilities_Total,  D.Current_Liab_Total, D.Current_Payables, D.Current_Emp_Benefits, D.LT_Liab_Total, D.LT_Payables, D.LT_Emp_Benefits,  D.Equity_Total, D.Controlling_Equity_Total, D.Current_Assets_Total,
                        S.REV, S.GROSS, S.CGS,  S.OPR, S.SGA_2, S.NETINC, S.NETINC_1, S.FINCOST, S.FININC, S.TAX,
                        C.Add_Exp_WO_CF_Out_5, C.Add_Exp_WO_CF_Out_6, C.CFO_Total,
                        RANK() OVER (PARTITION BY stkcode ORDER BY S.date desc) AS a
                    FROM sonik S 
                                inner join cash C on S.stkcode = C.stkcode and S.date = C.date and S.report = C.report
                                inner join Daecha D on S.stkcode = D.stkcode and S.date = D.date and S.report = D.report
                    where S.report = 'Q' and S.date <= date('{}')
                    ) AS D
            WHERE D.a between 1 and 4
            group by stkcode
            '''\
            .format(date)

        df1 =  self.getOHLCV(date)
        df2 =  pd.DataFrame = pd.read_sql(sql, self.engine.connect())
        
        df = df1.merge(df2, how='inner', on='STKCODE')
        print(df.STKCODE)
        return df

    
    def getPrevCompanyInfo(self, date, prevYear: int):
        sql = '''
            select O.STKCODE, O.DATE, O.CLOSE_PRICE, O.TRDVOL, O.TRDVAL, O.MARKETCAP, O.SHARES,
                    D.DATE AS REPORT_DATE, D.REPORT, D.Assets_Total, D.Current_Fin_Assets, D.Cash_and_Cash_Equivalents, D.Current_Liab_Total, D.Current_Payables, D.Current_Emp_Benefits, D.LT_Liab_Total, D.LT_Payables, D.LT_Emp_Benefits,  D.Equity_Total, D.Controlling_Equity_Total, Current_Assets_Total,
                    S.REV, S.GROSS, S.OPR, S.SGA_2, S.NETINC, S.NETINC_1,
                    C.Add_Exp_WO_CF_Out_5, C.Add_Exp_WO_CF_Out_6, C.CFO_Total
                FROM OHLCV O 
                INNER JOIN daecha D 
                        ON O.stkcode= D.stkcode AND date(concat(year(date_add(O.date,interval -{1} year)), '-12-31')) = D.date
                INNER JOIN CASH C
                        ON O.stkcode= C.stkcode AND date(concat(year(date_add(O.date,interval -{1} year)), '-12-31')) = C.date
                INNER JOIN SONIK S
                        ON O.stkcode= S.stkcode AND date(concat(year(date_add(O.date,interval -{1} year)), '-12-31')) = S.date
                WHERE O.DATE = DATE('{0}');
        '''\
        .format(date, prevYear)

        ret = pd.read_sql(sql, self.engine.connect())
    
        return ret