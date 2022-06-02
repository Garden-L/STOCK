from JIpyo import *

df = getCompanyInfo('2022-05-10')

#4 대장 콤보
getPER(df)
getPBR(df)
getPSR(df)
getPCR(df)
s1 = df_filter(df, 'PER', min= 1, n=50, asc = True) #하위 30종목
s2 = df_filter(df, 'PBR', min=0.1,  n=50, asc= True) # 하위 30종목
s3 = df_filter(df, 'PSR', min=0.1,  n=50, asc= True) # 하위 30종목
s4 = df_filter(df, 'PCR', min=0.1, n=50, asc= True) # 하위 30종목
print(df_combine(s1,s2,s3,s4 , how='and'))
