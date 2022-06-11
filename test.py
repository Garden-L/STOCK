from JIpyo import *

#22년 5월 10일자에 투자한다고 가정했을 시 
#5월 10일날 주가, 재무제표를 가져옴
df = getCompanyInfo('2022-05-10') 

#4 대장 콤보
df=getPER(df)
df=getPBR(df)
df=getPSR(df)
df=getPCR(df)
s1 = df_filter(df, 'PER', min= 1, n=400, asc = True) #하위 400종목
s2 = df_filter(df, 'PBR', min=0.1,  n=400, asc= True) # 하위 400종목
s3 = df_filter(df, 'PSR', min=0.1,  n=400, asc= True) # 하위 400종목
s4 = df_filter(df, 'PCR', min=0.1, n=400, asc= True) # 하위 400종목
#df_combine(s1,s2,s3,s4 , how='and').to_csv("./4대장콤보.csv",index = False)

#EV/EBITDA
df=getEV_EBITDA(df)
s1 = df_filter(df, 'EV_EBITDA', min=0, n = 100, asc=True)
# s1.to_csv("./EV_EBITDA.csv", index = False)

#안전마진이 있는 그레이엄의 NCAV 투자법
df=Safety_Margin(df)
s1 = df_filter(df, 'Safety_Margin', min = 0, n = 30, asc=False )
# s1.to_csv("./NCAV투자법.csv", index = False)

#수가수익성장비율 (성장주 투자)
df=getPEG(df, 2)
s1 = df_filter(df, 'PEG', min=0.1, max=1, n=30, asc=True)
# s1.to_csv("./PEG_성장주투자.csv", index = False)

#ROE값이 높은 상위 종목
df = getROE(df)
s1 = df_filter(df, 'ROE', min = 0, n = 30, asc=False)
s1.to_csv("./ROE.csv", index=False)

#PRIM잔여이익 모델 저평가 상태
df = getRIM(df, 0.1)
df['P_RIM'] = df['CLOSE_PRICE'] / df['RIM']
s1 = df_filter(df, 'P_RIM', min = 0, max=1, n =30, asc=True)
s1.to_csv('./P_RIM.csv', index = False)

#GP/A 값이 높은 상위종목에 투자
df = getGPA(df)
s1 = df_filter(df, 'GPA', min = 0, n = 30, asc=False)
s1.to_csv("./GPA.csv", index=False)

#부채비율이 낮은 종목에 투자
df = getLiabRatio(df)
s1 = df_filter(df, 'LiabRatio', min = 0, n = 30, asc=True)
s1.to_csv('./부채비율.csv', index=False)

#매출액 증가율이 높은 회사
df = getRG(df, 2)
s1 = df_filter(df, 'RG', min = 0, n = 30, asc=False)
s1.to_csv('./매출액 증가율높은 회사 투자.csv', index = False)

#영업이익 증가율이 높은 회사
df = getOIG(df, 2)
s1 = df_filter(df, 'OIG', min = 0, n = 30, asc=False)
s1.to_csv('영업이익 증가율이 높은회사 투자.csv', index = False)

#그린블라트 마법공식
df = getPER(df)
df = getROC(df)
s1 = df_filter(df, 'PER', min=1, max=10, asc=True)
s2 = df_filter(df, 'ROC', min=0, asc=False)
df_combine(s1,s2, how='and').to_csv("./그린블라트 마법공식.csv", index=False)

#F Score
df = getF_Score(df, 2)
s1 = df_filter(df, 'F_Score', min = 3, max= 6,asc=True)

