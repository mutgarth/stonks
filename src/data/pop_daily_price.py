import yfinance as yf
from db_connect import db_connect
from db_retrieving import ret_tickers, check_if_exists
import datetime
import pandas_datareader.data as pdr
import numpy 
import math

def update_tup_to_datetime(tup):
   lst = list(tup)
   lst[1] = lst[1].to_pydatetime().date()
   lst[2] = lst[2].to_pydatetime().date()
   lst[3] = lst[3].to_pydatetime().date()
   return tuple(lst)

def get_hist_data(ticker):
    yf.pdr_override()
    trade_data = pdr.get_data_yahoo(ticker, period='max', interval='1d', group_by = 'column')
    return trade_data

def insert_price_db(con, daily_data):
    cursor = con.cursor()
    columns_name = """symbol_id, price_date, created_date, last_updated_date, open_price,
                    high_price, low_price, close_price, adj_close_price, volume"""
    insert_str = ("%s, "*10)[:-2]
    insert_query = "INSERT INTO daily_price (%s) VALUES (%s)" % (columns_name, insert_str)
    cursor.executemany(insert_query, daily_data)
    con.commit()

def handling_nan(dataframe):
    """
    The missing values were handled as proposed by Prof. Mazin A. M. Al Janabi.
    It was previosly verified that the prices were availabe for some tickers,
    but not for others, therefore the missing price was calculate from the geometric
    mean of the previous and the following price.

    P_t = sqrt(P_{t-1} * P_{t+1})
    """
    for c in dataframe.columns[:-1]:
        for i in range(0,len(dataframe[c])):
            if numpy.isnan(dataframe[c][i]):
                dataframe[c][i] = math.sqrt(dataframe[c][i-1] * dataframe[c][i+1])
    return dataframe.replace({numpy.nan: None})

def print_db_stats(count_new, new_tickers):
    print('--------------------------')
    print('%s new tickers were added:' % count_new)
    for ticker in new_tickers:
        print(ticker)
    print('--------------------------')

if __name__ == "__main__":
    
    con = db_connect()
    tickers = ret_tickers()
    tickers = check_if_exists(tickers)
    failed_tickers = []
    success_tickers = []
    count = 0

    for t in tickers:
        print('Trying to download %s data' % t[1])
        try:
            daily_data = get_hist_data(t[1])
            if 'Empty DataFrame' in str(daily_data):
                raise Exception()
            else:
                count+=1
                success_tickers.append(t[1])

            daily_data = handling_nan(daily_data)

            now = datetime.datetime.utcnow()
            daily_data = daily_data.rename_axis('date').reset_index()
            daily_data.insert(0,'symbol_id',t[0])
            daily_data.insert(2,'created_date', now)
            daily_data.insert(3,'last_updated_date', now)

            daily_data = list(daily_data.itertuples(index=False, name=None))

            for i in range(0,len(daily_data)):
                daily_data[i] = update_tup_to_datetime(daily_data[i])  
        
            insert_price_db(con, daily_data)

        except Exception:
            failed_tickers.append(t[1])
            pass
            
    con.close()
    print(print_db_stats(count, success_tickers))
    print(failed_tickers)




 

             
        
