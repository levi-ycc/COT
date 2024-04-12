import pandas as pd
import os
import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
import sqlite3
from PIL import Image
import talib

conn = sqlite3.connect("D:/News/news.db")

conn2 = sqlite3.connect("D:/News/price.db")

cot=pd.read_sql('SELECT * FROM COT', conn)

sym_list=['EUR', 'GBP', 'NZD' ,'AUD', 'CAD', 'JPY', 'CHF', 'XAU', 'XAG', 'WTI', 'BCO', 'SPX', 'NSX']
pair_dict={'EUR':'EURUSD', 'GBP':'GBPUSD', 'NZD':'NZDUSD', 'AUD':'AUDUSD', 'CAD': 'USDCAD', 'JPY': 'USDJPY', 'CHF': 'USDCHF', 'XAU':'XAUUSD', 'XAG':'XAGUSD', 'WTI':'WTIUSD', 'BCO':'BCOUSD', 'SPX':'SPXUSD', 'NSX':'NSXUSD'}
gold = pd.read_sql("Select * from PRICE where Sym='XAUUSD' and date(Date)>='2018'",conn2)
gold.drop(['Sym'], axis=1, inplace=True)
gold['Date Time'] = gold.Date + ' ' + gold.Time
gold['Date Time']=pd.to_datetime(gold['Date Time'])
gold.set_index('Date Time', inplace=True)
daily_gold = gold['Close'].resample('1D').agg({'Close':'last'})

udx = pd.read_sql("Select * from PRICE where Sym='UDXUSD' and date(Date)>='2018'",conn2)
udx.drop(['Sym'], axis=1, inplace=True)
udx['Date Time'] = udx.Date + ' ' + udx.Time
udx['Date Time']=pd.to_datetime(udx['Date Time'])
udx.set_index('Date Time', inplace=True)
daily_udx = udx['Close'].resample('1D').agg({'Close':'last'})

for s in sym_list:
    print(s)
    product=cot[cot['PRODUCT']==s]

    price = pd.read_sql("Select * from PRICE where Sym='{}' and date(Date)>='2018'".format(pair_dict[s]),conn2)

    price.drop(['Sym'], axis=1, inplace=True)

    price['Date Time'] = price.Date + ' ' + price.Time

    price['Date Time']=pd.to_datetime(price['Date Time'])

    price.set_index('Date Time', inplace=True)

    daily_price = price[['Open', 'High', 'Low', 'Close']].resample('1D').agg({'Open':'first', 'High': max, 'Low': min, 'Close':'last'}) #price['Close'].resample('1D').agg({'Close':'last'})

    product.set_index(pd.to_datetime(product.DATE), inplace=True)

    product['max_comm']=product['Net Comm'].rolling(26).max()
    product['min_comm']=product['Net Comm'].rolling(26).min()

    product['CX']=(product['Net Comm']-product.min_comm)/(product.max_comm-product.min_comm)*100

    product['max_comm2']=product['Net Comm'].rolling(13).max()
    product['min_comm2']=product['Net Comm'].rolling(13).min()
    product['CX2']=(product['Net Comm']-product.min_comm2)/(product.max_comm2-product.min_comm2)*100
    # product['LTX']=(product['Net Comm']-product['Net Comm'].rolling(15).min())/(product['Net Comm'].rolling(15).max()-product['Net Comm'].rolling(15).min())*100
    product.dropna(inplace=True)
    merged=product.join(daily_price).dropna()

    merged['Close_1'] = merged.Close.shift()
    merged['trh']=merged[['Close_1','High']].max(axis=1)
    merged['trl']=merged[['Close_1','Low']].min(axis=1)
    merged['pov']=(merged.OI*np.sign(merged.Close-merged.Close_1)).cumsum()+(merged.OI*(merged.Close-merged.Close_1)/(merged.trh-merged.trl)).cumsum()

    fig, ax = plt.subplots(4,1, figsize=(30,15), gridspec_kw={'height_ratios': [2, 1, 1, 1]})
    ax[0].plot(merged.Close.iloc[-104:], color='black')
    ax[1].plot(merged.CX.iloc[-104:], color='blue')
    ax[1].plot(merged.CX2.iloc[-104:], color='green')
    # ax[1].plot(merged.LTX.iloc[-104:], color='red')
    ax[1].axhline(75, color='red')
    ax[1].axhline(25, color='red')
    ax[1].set_title('CX', fontdict={'fontsize':16}, loc='right')
    ax[2].plot(merged['Net Comm'].iloc[-104:], color='blue')
    ax[2].plot(merged['Net NonComm'].iloc[-104:], color='green')
    ax[2].axhline(0, color='red')
    ax[2].set_title('Net Position', fontdict={'fontsize':16}, loc='right')
    ax[3].plot(merged.pov.iloc[-104:], color='blue')
    ax[3].set_title('POIV AD', fontdict={'fontsize':16}, loc='right')
    fig.suptitle('Commercial Trader', fontsize=24)


    plt.savefig('img/{}.jpg'.format(pair_dict[s]))
    plt.close()
    
    results = seasonal_decompose(daily_price.Close.dropna(), model='additive', period=252)
    merged=merged.join(results.seasonal).dropna()
    if s == 'XAU':
        udx_spread=(daily_gold.Close/daily_udx.Close*100).dropna()
        pal=udx_spread.ewm(span=2*5).mean()-udx_spread.ewm(span=22*5).mean()
        spread=((pal-pal.rolling(52*3).min())/(pal.rolling(52*3).max()-pal.rolling(52*3).min())).dropna()
        merged['Dspread']=spread*100
        merged.dropna(inplace=True)
        fig, ax = plt.subplots(3,1, figsize=(30,15), gridspec_kw={'height_ratios': [2, 1, 1]})
        ax[0].plot(merged.Close.iloc[-104:], color='black')
        ax[1].plot(merged.seasonal.iloc[-104:], color='blue')
        ax[1].set_title('Seasonality', fontdict={'fontsize':16}, loc='right')
        ax[2].plot(merged.Dspread.iloc[-104:], color='green')
        ax[2].set_title('Valuation', fontdict={'fontsize':16}, loc='right')
        ax[2].axhline(75, color='red')
        ax[2].axhline(25, color='red')
        fig.suptitle('Valuation Model', fontsize=24)
    else:
        udx_spread=(daily_price.Close/daily_udx.Close*100).dropna()
        pal=udx_spread.ewm(span=2*5).mean()-udx_spread.ewm(span=22*5).mean()
        spread=((pal-pal.rolling(52*3).min())/(pal.rolling(52*3).max()-pal.rolling(52*3).min())).dropna()
        merged['Dspread']=spread*100
        merged.dropna(inplace=True)
        gx_spread=(daily_price.Close/daily_gold.Close*100).dropna()
        pal=gx_spread.ewm(span=2*5).mean()-gx_spread.ewm(span=22*5).mean()
        spread=((pal-pal.rolling(52*3).min())/(pal.rolling(52*3).max()-pal.rolling(52*3).min())).dropna()
        merged['spread']=spread*100
        merged.dropna(inplace=True)
        fig, ax = plt.subplots(3,1, figsize=(30,15), gridspec_kw={'height_ratios': [2, 1, 1]})
        ax[0].plot(merged.Close.iloc[-104:], color='black')
        ax[1].plot(merged.seasonal.iloc[-104:], color='blue')
        ax[1].set_title('Seasonality', fontdict={'fontsize':16}, loc='right')
        ax[2].plot(merged.spread.iloc[-104:], color='blue')
        ax[2].plot(merged.Dspread.iloc[-104:], color='green')
        ax[2].set_title('Valuation', fontdict={'fontsize':16}, loc='right')
        ax[2].axhline(75, color='red')
        ax[2].axhline(25, color='red')
        fig.suptitle('Valuation Model', fontsize=24)
    
    plt.savefig('img/{}_seasonal.jpg'.format(pair_dict[s]))
    plt.close()

    weekly=price[['Open', 'High', 'Low', 'Close']].resample('5D').agg({'Open':'first', 'High': max, 'Low': min, 'Close':'last'}) 
    adx=talib.ADX(weekly.High, weekly.Low, weekly.Close, 7)
    k, _ = talib.STOCH(weekly.High, weekly.Low, weekly.Close, fastk_period=14, slowk_period=3)
    weekly['ADX']=adx
    weekly['K']=k
    weekly.dropna(inplace=True)
    fig, ax = plt.subplots(3,1, figsize=(30,15), gridspec_kw={'height_ratios': [2, 1, 1]})
    ax[0].plot(weekly.Close.iloc[-104:], color='black')
    ax[1].plot(weekly.ADX.iloc[-104:], color='blue')
    ax[1].set_title('ADX', fontdict={'fontsize':16}, loc='right')
    ax[1].axhline(40, color='red')
    ax[1].axhline(60, color='red')
    ax[2].plot(weekly.K.iloc[-104:], color='blue')
    ax[2].set_title('Stoch K', fontdict={'fontsize':16}, loc='right')
    ax[2].axhline(75, color='red')
    ax[2].axhline(25, color='red')
    fig.suptitle('Technical', fontsize=24)
    plt.savefig('img/{}_tech.jpg'.format(pair_dict[s]))
    plt.close()

    image_1 = Image.open(r'img/{}.jpg'.format(pair_dict[s]))
    image_2 = Image.open(r'img/{}_seasonal.jpg'.format(pair_dict[s]))
    image_3 = Image.open(r'img/{}_tech.jpg'.format(pair_dict[s]))
    im_1 = image_1.convert('RGB')
    im_2 = image_2.convert('RGB')
    im_3 = image_3.convert('RGB')

    image_list = [im_2, im_3]
    im_1.save(r'report/{}.pdf'.format(pair_dict[s]), save_all=True, append_images=image_list)




