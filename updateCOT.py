import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
import sqlite3

path = 'data'
files = [os.path.join(path, x) for x in os.listdir(path) if 'xls' in x]

products=[       
       'CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE',
       'SWISS FRANC - CHICAGO MERCANTILE EXCHANGE',
       'BRITISH POUND - CHICAGO MERCANTILE EXCHANGE',
       'BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE',
       'JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE',
       'EURO FX - CHICAGO MERCANTILE EXCHANGE',
       'NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE',
       'AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE',
       'GOLD - COMMODITY EXCHANGE INC.',
       'SILVER - COMMODITY EXCHANGE INC.',
       'CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE',
       'E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE',
       'E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE',
       'BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE',
       'BRENT CRUDE OIL LAST DAY - NEW YORK MERCANTILE EXCHANGE',
       'NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE',
       'NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE'
]
product_dict={'CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE':'CAD',
       'SWISS FRANC - CHICAGO MERCANTILE EXCHANGE':'CHF',
       'BRITISH POUND - CHICAGO MERCANTILE EXCHANGE':'GBP',
       'BRITISH POUND STERLING - CHICAGO MERCANTILE EXCHANGE':'GBP',
       'JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE':'JPY',
       'EURO FX - CHICAGO MERCANTILE EXCHANGE':'EUR',
       'NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE':'NZD',
       'AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE':'AUD',
       'GOLD - COMMODITY EXCHANGE INC.':'XAU',
       'SILVER - COMMODITY EXCHANGE INC.':'XAG',
       'CRUDE OIL, LIGHT SWEET-WTI - ICE FUTURES EUROPE':'WTI',
       'E-MINI S&P 500 - CHICAGO MERCANTILE EXCHANGE':'SPX',
       'E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE':'SPX',
       'BRENT LAST DAY - NEW YORK MERCANTILE EXCHANGE':'BCO',
       'BRENT CRUDE OIL LAST DAY - NEW YORK MERCANTILE EXCHANGE':'BCO',
       'NASDAQ MINI - CHICAGO MERCANTILE EXCHANGE':'NSX',
       'NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE':'NSX'}
cot=pd.DataFrame()
for p in products:
    symb_name = product_dict.get(p)
    print(symb_name)
    for f in files:
        df=pd.read_excel(f)

        df=df[df['Market_and_Exchange_Names']==p][['Report_Date_as_MM_DD_YYYY', 'Market_and_Exchange_Names',
                                                   'NonComm_Positions_Long_All','NonComm_Positions_Short_All','Comm_Positions_Long_All','Comm_Positions_Short_All', 'Open_Interest_All']]
        df['Market_and_Exchange_Names'] = symb_name
        df['Report_Date_as_MM_DD_YYYY']=pd.to_datetime(df['Report_Date_as_MM_DD_YYYY'])
        df.set_index('Report_Date_as_MM_DD_YYYY', inplace=True)

        df['Net NonComm'] = df['NonComm_Positions_Long_All']-df['NonComm_Positions_Short_All']
        df['Net Comm'] = df['Comm_Positions_Long_All']-df['Comm_Positions_Short_All']
        df['IDX']=df['Net NonComm']-df['Net Comm']
        df['Spec']=df['NonComm_Positions_Long_All']/(df['NonComm_Positions_Long_All']+df['NonComm_Positions_Short_All'])
        df['Comm']=df['Comm_Positions_Long_All']/(df['Comm_Positions_Long_All']+df['Comm_Positions_Short_All'])

        cot = pd.concat([cot,df[['Market_and_Exchange_Names', 'IDX','Spec','Comm', 'Net Comm', 'Net NonComm', 'Open_Interest_All']]])
        
cot.sort_index(inplace=True)
cot.columns=['PRODUCT', 'IDX', 'SPEC', 'COMM', 'Net Comm', 'Net NonComm', 'OI']
cot['DATE']=cot.index
cot = cot[['DATE', 'PRODUCT', 'IDX', 'SPEC', 'COMM', 'Net Comm', 'Net NonComm', 'OI']]
conn = sqlite3.connect("D:/News/news.db")
cot.to_sql('COT', conn, if_exists='replace', index=False) 