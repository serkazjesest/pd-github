import pandas as pd
#import sys
import requests
import datetime
from datetime import datetime
#import json
import numpy as np
import math
import time
#from imblearn.ensemble import BalancedRandomForestClassifier as brfc
#from sklearn.ensemble import RandomForestClassifier as brfc
import joblib

# создаём датафрейм, в который положим нужные модели параметры
temporary_dataframe = pd.DataFrame(columns=['sym','last_price','last_volumefrom','last_volumeto','last_return',
'return3h','return12h','return24h','return36h','return48h','return60h','return72h','returnvola3h',
'returnvola12h','returnvola24h','returnvola36h','returnvola48h','returnvola60h','returnvola72h',
'volumefrom3h','volumefrom12h','volumefrom24h','volumefrom36h','volumefrom48h','volumefrom60h',
'volumefrom72h','volumefromvola3h','volumefromvola12h','volumefromvola24h','volumefromvola36h',
'volumefromvola48h','volumefromvola60h','volumefromvola72h','volumeto3h','volumeto12h','volumeto24h',
'volumeto36h','volumeto48h','volumeto60h','volumeto72h','volumetovola3h','volumetovola12h','volumetovola24h',
'volumetovola36h','volumetovola48h','volumetovola60h','volumetovola72h'])

# переменные связанные с временем для функции заполняющей таблицу
unixtime = int(time.mktime(datetime.now().timetuple()))
a = unixtime - 104800
val = datetime.fromtimestamp(a)
years_n_days = datetime.strftime(val, '%Y-%m-%d')
hours_n_minutes = datetime.strftime(val, '%H.%M')

def list_of_coins_paired_to_btc():

    # берём с binance данные о текущих ценах всех существующих на бирже пар
    request_to_binance = requests.get('https://api.binance.com/api/v1/ticker/allPrices')
    json_data_temp = request_to_binance.json()
    binance_prices = pd.DataFrame(json_data_temp)

    # отбираем из данных с бинанс только список всех возможных пар без цен
    pare = binance_prices['symbol']
    list_of_coin = []

    # циклом проходимся по всем парам и отбираем лишь те, которые торгуются в паре к btc, здесь же можно вручную отфильтровать монеты
    # листинг на биржу которых произошёл менее, чем 72 часа назад
    for k in range(len(pare.index)):
        coin = pare[k]
        #if 'POLYX' in pare[k]:
        #    continue
        #if 'HOOK' in pare[k]:
        #    continue
        if coin[-3:]=='BTC':
            list_of_coin.append(coin[:-3])

    return list_of_coin


def fill_in_the_worksheet(temporary_dataframe):

    list_of_coin = list_of_coins_paired_to_btc()
    # циклом подставляем в API-запрос cryptocompare список монет, чтобы получить OHLC (open, high, low, close)
    for i in range(len(list_of_coin)):
        print(list_of_coin[i])
        try:       
            url = ('https://min-api.cryptocompare.com/data/histohour?fsym=' + list_of_coin[i] + '&e=binance&tsym=BTC&limit=72&toTs=' + str(unixtime))
            request = requests.get(url)
            json_data_temp = request.json()
            ohlc = pd.DataFrame(json_data_temp['Data'])
            lprice = ohlc.loc[len(ohlc.index)-1,'open']
            lvolumefrom = ohlc.loc[len(ohlc.index)-1,'volumefrom']
            lvolumeto = ohlc.loc[len(ohlc.index)-1,'volumeto']
            lreturn = np.log(ohlc.loc[len(ohlc.index)-1,'close'])-np.log(ohlc.loc[len(ohlc.index)-1,'open'])
            z = [list_of_coin[i], lprice, lvolumefrom, lvolumeto, lreturn]
            rtx, rtvx, vlfx, vlfvx, vltx, vltvx, unixtim = [],[],[],[],[],[],[]

            for j in range(7):        
                 period = [3, 12, 24, 36, 48, 60, 72]
                 row = len(ohlc.index)
                 returnx = math.log(ohlc.loc[row-1,'close'])-math.log(ohlc.loc[row-period[j],'open'])
                 rtx.append(returnx)
                 returnvolax = np.std(np.log(ohlc.loc[row-period[j]:row, 'close']) - np.log(ohlc.loc[row-period[j]:row, 'open']), ddof=1)
                 rtvx.append(returnvolax)
                 volumefromx = sum(ohlc.loc[row-period[j]:row, 'volumefrom'])
                 vlfx.append(volumefromx)
                 volumefromvolax = np.std(ohlc.loc[row-period[j]:row, 'volumefrom'], ddof=1)
                 vlfvx.append(volumefromvolax)
                 volumetox = sum(ohlc.loc[row-period[j]:row, 'volumeto'])
                 vltx.append(volumetox)
                 volumetovolax = np.std(ohlc.loc[row-period[j]:row, 'volumeto'], ddof=1)
                 vltvx.append(volumetovolax)
            unixtim.append(unixtime)
            z.extend(rtx + rtvx + vlfx + vlfvx + vltx + vltvx)
            temporary_dataframe = temporary_dataframe.append(pd.DataFrame([z], columns=temporary_dataframe.columns), ignore_index=False)

        except KeyError:
            print('KEY ERROR!')
            z = ['X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X',
            'X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X',
            'X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X',
            'X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X','X_X']
            temporary_dataframe = temporary_dataframe.append(pd.DataFrame([z], columns=temporary_dataframe.columns), ignore_index=False)

    temporary_dataframe.to_csv('data_without_cap.csv', header=True, sep=';', index=False)

    return temporary_dataframe


def add_capitalization():    
    
    tempq = pd.read_csv('data_without_cap.csv', sep=';')
    url = ('https://coincodex.com/api/coincodex/get_historical_snapshot/' + years_n_days + '%20' + hours_n_minutes + '/0/10000')
    headers = {'authority': 'coincodex.com', 'method': 'GET', 'accept': 'text/html, application/xhtml+xml, application/xml;q=0.9, image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7', 'cache-control': 'max-age=0', 'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"', 'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"', 'sec-fetch-dest': 'document', 'sec-fetch-mode': 'navigate', 'sec-fetch-site': 'none', 'sec-fetch-user': '?1', 'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36'}
    request = requests.get(url, headers=headers)
    json_data_temp = request.json()
    cap = pd.DataFrame(json_data_temp['coins'])
    tempo = pd.DataFrame(columns=['caps'])
    for i in range(len(tempq.index)):
        try:      
            sym = tempq.at[i, 'sym']
            inde = cap.index[cap['display_symbol'] == sym].tolist()
            capvalue = cap.at[inde[0], 'market_cap_usd']
            z=[capvalue]
            tempo = tempo.append(pd.DataFrame([z], columns=tempo.columns), ignore_index=True)
            print(sym)
        except IndexError:
            z=['X-X']
            tempo = tempo.append(pd.DataFrame([z], columns=tempo.columns), ignore_index=True)
            print('Index Error')
    temporary_dataframe = tempq.join(tempo)

    return temporary_dataframe


def remove_null_lines():

    temporary_dataframe = add_capitalization()
    for i in range(len(temporary_dataframe.index)):
        if temporary_dataframe.at[i, 'sym']=='X_X' or temporary_dataframe.at[i, 'caps']=='X-X' or temporary_dataframe.at[i, 'volumetovola72h']=='0.0' or temporary_dataframe.at[i, 'caps']=='0.0':
            temporary_dataframe =  temporary_dataframe.drop(labels=i, axis = 0)
    temporary_dataframe.to_csv('final_table.csv', header=True, sep=';', index=False)

    return temporary_dataframe

def collect_up_to_date_data():

    fill_in_the_worksheet(temporary_dataframe)
    return remove_null_lines()


if __name__ == '__main__':
    print(collect_up_to_date_data())

